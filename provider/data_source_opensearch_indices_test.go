package provider

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccOpensearchDataSourceIndices_basic(t *testing.T) {
	var providers []*schema.Provider
	resource.ParallelTest(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
		},
		ProviderFactories: testAccProviderFactories(&providers),
		Steps: []resource.TestStep{
			{
				Config: testAccOpensearchDataSourceIndices,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "id", "terraform-test-indices-*"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "names.#", "2"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "names.0", "terraform-test-indices-a"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "names.1", "terraform-test-indices-b"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.#", "2"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.name", "terraform-test-indices-a"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.leader_alias", ""),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.leader_index", ""),
					resource.TestCheckResourceAttr("data.opensearch_indices.none", "names.#", "0"),
					resource.TestCheckResourceAttr("data.opensearch_indices.missing", "names.#", "0"),
				),
			},
		},
	})
}

var testAccOpensearchDataSourceIndices = `
# for_each is not supported by the SDK test framework.
resource "opensearch_index" "test" {
  count = 2

  name               = "terraform-test-indices-${["a", "b"][count.index]}"
  number_of_shards   = "1"
  number_of_replicas = "0"
}

data "opensearch_indices" "test" {
  pattern = "terraform-test-indices-*"

  depends_on = [opensearch_index.test]
}

data "opensearch_indices" "none" {
  pattern = "terraform-test-indices-none-*"
}

data "opensearch_indices" "missing" {
  pattern = "terraform-test-indices-missing"
}
`

// Stops the replications started by an auto-follow rule the way the
// documentation describes it: list the follower indexes, import them as
// `opensearch_cross_cluster_replication` resources, then destroy those.
func TestAccOpensearchDataSourceIndices_crossClusterReplication(t *testing.T) {
	leaderURL, leaderSeed := testAccCrossClusterReplicationPreCheck(t)
	followerURL := os.Getenv("OPENSEARCH_URL")

	leaderIndexes := []string{"terraform-test-ccr-auto-a", "terraform-test-ccr-auto-b"}

	for _, index := range leaderIndexes {
		// The follower indexes keep the name of the leader ones.
		if err := testAccClusterRequest("DELETE", followerURL, "/"+index, ""); err != nil {
			t.Fatal(err)
		}
		if err := testAccClusterRequest("PUT", leaderURL, "/"+index, `{"settings":{"index.number_of_shards":1,"index.number_of_replicas":0}}`); err != nil {
			t.Fatal(err)
		}
	}

	t.Cleanup(func() {
		for _, index := range leaderIndexes {
			if err := testAccClusterRequest("DELETE", leaderURL, "/"+index, ""); err != nil {
				t.Logf("failed to delete the leader index: %s", err)
			}
			if err := testAccClusterRequest("DELETE", followerURL, "/"+index, ""); err != nil {
				t.Logf("failed to delete the follower index: %s", err)
			}
		}
	})

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testCheckOpensearchCrossClusterReplicationDestroy,
		Steps: []resource.TestStep{
			{
				// The rule replicates the leader indexes that already exist.
				Config: testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed, true, false),
			},
			{
				PreConfig: func() {
					// The rule starts the replications in the background, the
					// follower indexes do not exist right away.
					for _, index := range leaderIndexes {
						err := retry.RetryContext(context.Background(), 2*time.Minute, func() *retry.RetryError {
							status, err := getCrossClusterReplicationStatus(context.Background(), testAccProvider.Meta().(*ProviderConf), index)
							if err != nil {
								return retry.RetryableError(err)
							}
							if state, _ := status["status"].(string); !strings.EqualFold(state, replicationStatusSyncing) {
								return retry.RetryableError(fmt.Errorf("replication of index %q is in status %q", index, state))
							}
							return nil
						})
						if err != nil {
							t.Fatal(err)
						}
					}
				},
				Config: testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed, true, false),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.#", "2"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.name", "terraform-test-ccr-auto-a"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.leader_alias", "terraform-test-ccr"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.leader_index", "terraform-test-ccr-auto-a"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.1.leader_index", "terraform-test-ccr-auto-b"),
				),
			},
			{
				// A paused replication still has a leader. The follower stats
				// would not report it — they only list the syncing indexes —
				// which is why the leader is read from the replication status.
				PreConfig: func() {
					conf := testAccProvider.Meta().(*ProviderConf)
					if err := pauseCrossClusterReplication(context.Background(), conf, leaderIndexes[0]); err != nil {
						t.Fatal(err)
					}
				},
				Config: testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed, true, false),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.leader_alias", "terraform-test-ccr"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.leader_index", "terraform-test-ccr-auto-a"),
				),
			},
			{
				// Resumed, so that the import below finds it syncing.
				PreConfig: func() {
					conf := testAccProvider.Meta().(*ProviderConf)
					if err := resumeCrossClusterReplication(context.Background(), conf, leaderIndexes[0], false); err != nil {
						t.Fatal(err)
					}
				},
				Config: testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed, true, false),
			},
			{
				// Importing the follower indexes, without any change.
				Config: testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed, true, true),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("opensearch_cross_cluster_replication.test.0", "status", replicationStatusSyncing),
					resource.TestCheckResourceAttr("opensearch_cross_cluster_replication.test.1", "status", replicationStatusSyncing),
				),
			},
			{
				// Destroying the imported replications stops them.
				Config: testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed, false, false),
				Check: resource.ComposeTestCheckFunc(
					testCheckOpensearchCrossClusterReplicationStopped(leaderIndexes),
				),
			},
			{
				// The stopped indexes are no longer reported as follower indexes.
				Config: testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed, false, false),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.#", "2"),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.0.leader_alias", ""),
					resource.TestCheckResourceAttr("data.opensearch_indices.test", "indices.1.leader_alias", ""),
				),
			},
		},
	})
}

func testCheckOpensearchCrossClusterReplicationStopped(followerIndexes []string) resource.TestCheckFunc {
	return func(_ *terraform.State) error {
		for _, index := range followerIndexes {
			status, err := getCrossClusterReplicationStatus(context.Background(), testAccProvider.Meta().(*ProviderConf), index)
			if err != nil {
				return err
			}
			if state, _ := status["status"].(string); !strings.EqualFold(state, replicationStatusNotInProgress) {
				return fmt.Errorf("replication of index %q is still in status %q", index, state)
			}
		}

		return nil
	}
}

func testAccOpensearchDataSourceIndicesCrossClusterReplicationConfig(leaderSeed string, withRule bool, withReplications bool) string {
	config := fmt.Sprintf(`
resource "opensearch_cross_cluster_connection" "test" {
  name  = "terraform-test-ccr"
  seeds = ["%s"]
}

data "opensearch_indices" "test" {
  pattern = "terraform-test-ccr-auto-*"
}
`, leaderSeed)

	if withRule {
		config += `
resource "opensearch_cross_cluster_replication_rule" "test" {
  name         = "terraform-test-auto"
  leader_alias = opensearch_cross_cluster_connection.test.name
  pattern      = "terraform-test-ccr-auto-*"

  use_roles {
    leader_cluster_role   = "all_access"
    follower_cluster_role = "all_access"
  }
}
`
	}

	if withReplications {
		config += `
# A list and count rather than a map and for_each, which the SDK test framework
# does not support: each.key is the position in the list.
locals {
  followers = [for index in data.opensearch_indices.test.indices : index if index.leader_alias != ""]
}

import {
  for_each = local.followers

  to = opensearch_cross_cluster_replication.test[each.key]
  id = each.value.name
}

resource "opensearch_cross_cluster_replication" "test" {
  count = length(local.followers)

  follower_index = local.followers[count.index].name
  leader_alias   = local.followers[count.index].leader_alias
  leader_index   = local.followers[count.index].leader_index
}
`
	}

	return config
}

func TestIsSystemIndex(t *testing.T) {
	cases := map[string]bool{
		".kibana_1":            true,
		".opendistro_security": true,
		".plugins-ml-config":   true,
		"movies":               false,
		"terraform-test-ccr":   false,
		"logs.2026-09-28":      false,
	}

	for name, expected := range cases {
		if isSystemIndex(name) != expected {
			t.Errorf("isSystemIndex(%q) = %t, expected %t", name, isSystemIndex(name), expected)
		}
	}
}
