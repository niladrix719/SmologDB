package integration

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/netstorage"
	"github.com/niladrix719/smologdb/storage"
)

func newClusterWith(t *testing.T, nodes int, policy netinsert.RoutingPolicy, ds *gen.Dataset) (*nodeSet, *netstorage.Storage) {
	t.Helper()
	ns := newNodeSet(t, nodes)
	c := ns.cluster(netstorage.Config{
		Insert: netinsert.Config{Routing: policy},
	})
	ingest(t, c, ds, 10_000)
	ns.flushAll()
	return ns, c
}

func TestClusterQueryMatchesBruteForce(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 100
	ds := gen.Generate(cfg)

	queries := randomQueries(ds, 1000, 20250826)

	for _, nodes := range []int{1, 2, 4, 8} {
		for _, policy := range []netinsert.RoutingPolicy{
			netinsert.RoutingSticky, netinsert.RoutingHybrid, netinsert.RoutingSpray,
		} {
			name := fmt.Sprintf("nodes=%d/%s", nodes, policy)
			t.Run(name, func(t *testing.T) {
				_, c := newClusterWith(t, nodes, policy, ds)

				for i, q := range queries {
					got, ss := c.Search(q)
					want := bruteForce(ds.Rows, q)
					if ok, msg := rowsEqual(got, want); !ok {
						t.Fatalf("%s: query %d disagrees with brute force: %s\n  query: %+v\n  stats: %s", name, i, msg, q, ss)
					}
					if ss.IsPartial() {
						t.Fatalf("%s: query %d reported PARTIAL with every node healthy: %s", name, i, ss)
					}
					if ss.NodesQueried != nodes {
						t.Fatalf("%s: query %d reports NodesQueried=%d, want %d.", name, i, ss.NodesQueried, nodes)
					}
				}
			})
		}
	}
	t.Logf("%d random queries x {1,2,4,8} nodes x 3 policies == brute force", len(queries))
}

func randomQueries(ds *gen.Dataset, n int, seed int64) []*storage.Query {
	rng := rand.New(rand.NewSource(seed))
	lo, hi := ds.Rows[0].Timestamp, ds.Rows[len(ds.Rows)-1].Timestamp
	span := hi - lo
	absent := gen.AbsentTokens(16, seed)
	tokens := []string{gen.RareToken, gen.CommonToken, "error", "info", "GET", "POST", "200", "500"}
	columns := []string{"_msg", "level", "method", "status", "service"}

	out := make([]*storage.Query, n)
	for i := range out {
		q := &storage.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}

		if rng.Intn(2) == 0 {
			a := lo + rng.Int63n(span+1)
			b := lo + rng.Int63n(span+1)
			if a > b {
				a, b = b, a
			}
			q.MinTimestamp, q.MaxTimestamp = a, b
		}
		if rng.Intn(4) == 0 {
			sid := ds.Streams[rng.Intn(len(ds.Streams))].ID
			q.StreamID = &sid
		}
		for f := rng.Intn(3); f > 0; f-- {
			tok := tokens[rng.Intn(len(tokens))]
			if rng.Intn(8) == 0 {
				tok = absent[rng.Intn(len(absent))]
			}
			q.Filters = append(q.Filters, storage.Filter{
				Column: columns[rng.Intn(len(columns))],
				Token:  tok,
			})
		}
		out[i] = q
	}
	return out
}

func TestQueryStatsAreMerged(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	_, c1 := newClusterWith(t, 1, netinsert.RoutingSpray, ds)
	_, c8 := newClusterWith(t, 8, netinsert.RoutingSpray, ds)

	q := &storage.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []storage.Filter{{Column: "_msg", Token: gen.RareToken}},
	}
	_, ss1 := c1.Search(q)
	_, ss8 := c8.Search(q)

	t.Logf("1 node:  %s", ss1)
	t.Logf("8 nodes: %s", ss8)

	if ss8.BlocksTotal == 0 {
		t.Fatalf("BlocksTotal is 0 across 8 nodes -- per-node SearchStats are being")
	}
	if ss8.BytesReceivedFromNodes == 0 {
		t.Errorf("BytesReceivedFromNodes is 0. Count the response body length in")
	}
	if ss8.NodesQueried != 8 {
		t.Errorf("NodesQueried=%d, want 8", ss8.NodesQueried)
	}
	if ss8.BlocksTotal <= ss1.BlocksTotal/2 {
		t.Errorf("8 nodes considered %d blocks, 1 node considered %d.", ss8.BlocksTotal, ss1.BlocksTotal)
	}
}
