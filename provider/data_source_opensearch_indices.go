package provider

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// The replication plugin does record the leader of a follower index in
// `index.plugins.replication.follower.leader_index`, but OpenSearch Service
// does not return that setting: it filters the plugin's internal index
// settings out of the settings API. Reading it works on a self-managed cluster
// and silently reports no follower at all on a managed domain, so the leader is
// read from the replication API instead, which is the documented surface.

func dataSourceOpensearchIndices() *schema.Resource {
	return &schema.Resource{
		Description: "`opensearch_indices` can be used to list the indexes of the provider's current cluster matching a pattern. Each index also reports whether it is the follower index of a running (syncing or paused) cross-cluster replication, e.g. to iterate over the indexes created by an `opensearch_cross_cluster_replication_rule` and import them as `opensearch_cross_cluster_replication` resources in order to stop their replication.",
		ReadContext: dataSourceOpensearchIndicesRead,

		Schema: map[string]*schema.Schema{
			"pattern": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "*",
				Description: "The index names or wildcard patterns to match, comma-separated, e.g. `books*`. Hidden indexes are not matched by wildcards. Defaults to `*`, i.e. all the indexes. Finding which indexes are followers costs one request each, so narrowing the pattern is worth it on a cluster holding many of them — the pattern of the `opensearch_cross_cluster_replication_rule` being looked after is usually the right one.",
			},
			"names": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "The names of the matching indexes, sorted",
				Elem:        &schema.Schema{Type: schema.TypeString},
			},
			"indices": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "The matching indexes, sorted by name",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "The name of the index",
						},
						"leader_alias": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "The name of the cross-cluster connection to the leader cluster when the index is the follower index of a running replication, empty otherwise",
						},
						"leader_index": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "The name of the replicated index on the leader cluster when the index is the follower index of a running replication, empty otherwise",
						},
					},
				},
			},
		},
	}
}

func dataSourceOpensearchIndicesRead(ctx context.Context, d *schema.ResourceData, m any) diag.Diagnostics {
	conf := m.(*ProviderConf)
	pattern := d.Get("pattern").(string)

	names, err := listIndices(ctx, conf, pattern)
	if err != nil {
		return diag.FromErr(err)
	}

	// One replication status per index. The follower stats would list them all
	// in a single call, but only while they are syncing: a paused replication
	// is counted there and left out of `index_stats`, and pausing is what comes
	// right before stopping.
	//
	// System indexes are reported without asking: the replication plugin
	// rejects any leader or follower index name starting with a dot, so none
	// of them can be a follower. The call would cost a round trip per index to
	// learn nothing, and fail outright on a cluster where the identity may read
	// its own indexes but not `.opendistro_security`.
	indices := make([]map[string]any, 0, len(names))
	for _, name := range names {
		leaderAlias, leaderIndex := "", ""

		if !isSystemIndex(name) {
			leaderAlias, leaderIndex, err = getReplicationLeader(ctx, conf, name)
			if err != nil {
				return diag.FromErr(err)
			}
		}

		indices = append(indices, map[string]any{
			"name":         name,
			"leader_alias": leaderAlias,
			"leader_index": leaderIndex,
		})
	}

	d.SetId(pattern)

	ds := &resourceDataSetter{d: d}
	ds.set("names", names)
	ds.set("indices", indices)

	return diag.FromErr(ds.err)
}

// The names of the indexes matching the pattern, sorted. `index.uuid` is always
// set: asking for that one setting keeps every matching index in the response,
// which only lists the indexes having one of the settings asked for.
func listIndices(ctx context.Context, conf *ProviderConf, pattern string) ([]string, error) {
	url := conf.rawUrl + fmt.Sprintf("/%s/_settings/index.uuid?flat_settings=true&expand_wildcards=open,closed", pattern)
	result, err := performRequestAndParse(ctx, conf.osClient, "GET", url, nil, "get indices")
	if err != nil {
		var httpErr *HTTPError
		// An index listed by name, without wildcard, does not exist.
		if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
			return nil, err
		}
		result = map[string]any{}
	}

	names := make([]string, 0, len(result))
	for name := range result {
		names = append(names, name)
	}
	sort.Strings(names)

	return names, nil
}

// The connection and the index an index is replicated from, empty when it is
// not a follower: the replication API answers `REPLICATION NOT IN PROGRESS` for
// a regular index, and a paused replication reports its leader like a syncing
// one.
func getReplicationLeader(ctx context.Context, conf *ProviderConf, followerIndex string) (leaderAlias string, leaderIndex string, err error) {
	status, err := getCrossClusterReplicationStatus(ctx, conf, followerIndex)
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			return "", "", nil
		}

		return "", "", err
	}

	if state, _ := status["status"].(string); strings.EqualFold(state, replicationStatusNotInProgress) {
		return "", "", nil
	}

	leaderAlias, _ = status["leader_alias"].(string)
	leaderIndex, _ = status["leader_index"].(string)

	return leaderAlias, leaderIndex, nil
}

// OpenSearch keeps its own indexes under a leading dot, `.kibana` and
// `.opendistro_security` among them.
func isSystemIndex(name string) bool {
	return strings.HasPrefix(name, ".")
}
