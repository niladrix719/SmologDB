package integration

import (
	"fmt"
	"math"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/storage"
)

func TestLimitMatchesBruteForce(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 100
	ds := gen.Generate(cfg)

	limits := []int{1, 10, 100, 1000, 50_000}

	for _, nodes := range []int{1, 4, 8} {
		for _, policy := range []netinsert.RoutingPolicy{
			netinsert.RoutingSticky, netinsert.RoutingSpray,
		} {
			name := fmt.Sprintf("nodes=%d/%s", nodes, policy)
			t.Run(name, func(t *testing.T) {
				_, c := newClusterWith(t, nodes, policy, ds)

				for _, limit := range limits {
					for qi, base := range randomQueries(ds, 40, int64(limit)) {
						q := *base
						q.Limit = limit

						got, ss := c.Search(&q)

						unlimited := *base
						unlimited.Limit = 0
						want := (&q).ApplyLimit(bruteForce(ds.Rows, &unlimited))

						if ok, msg := rowsEqual(got, want); !ok {
							t.Fatalf("%s limit=%d query %d: %s\n  query: %+v\n  stats: %s", name, limit, qi, msg, &q, ss)
						}
						if limit > 0 && len(got) > limit {
							t.Fatalf("%s limit=%d returned %d rows", name, limit, len(got))
						}
					}
				}
			})
		}
	}
	t.Logf("limits %v x {1,4,8} nodes x 2 policies == first-N of brute force", limits)
}

func TestLimitPushdownReducesTransfer(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 400_000
	cfg.Streams = 200
	ds := gen.Generate(cfg)

	const nodes = 8
	_, c := newClusterWith(t, nodes, netinsert.RoutingSpray, ds)

	broad := &storage.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []storage.Filter{{Column: "level", Token: "info"}},
	}

	unlimited := *broad
	gotAll, ssAll := c.Search(&unlimited)

	t.Logf("================== LIMIT PUSHDOWN ==================")
	t.Logf("%10s %12s %16s %14s", "limit", "rows", "wire(MiB)", "vs unlimited")
	t.Logf("%10s %12d %16.2f %14s", "none", len(gotAll), float64(ssAll.BytesReceivedFromNodes)/(1<<20), "1.00x")

	for _, limit := range []int{100, 1000, 10000} {
		q := *broad
		q.Limit = limit
		got, ss := c.Search(&q)

		ratio := 0.0
		if ss.BytesReceivedFromNodes > 0 {
			ratio = float64(ssAll.BytesReceivedFromNodes) / float64(ss.BytesReceivedFromNodes)
		}
		t.Logf("%10d %12d %16.4f %13.1fx", limit, len(got), float64(ss.BytesReceivedFromNodes)/(1<<20), ratio)

		if ss.BytesReceivedFromNodes >= ssAll.BytesReceivedFromNodes {
			t.Errorf("limit=%d moved %d bytes over the network; the unlimited query moved\n%d. The limit is not reaching the storage nodes -- it is being applied", limit, ss.BytesReceivedFromNodes, ssAll.BytesReceivedFromNodes)
		}

		if limit*nodes*2 < len(gotAll) {
			maxExpected := float64(limit*nodes) / float64(len(gotAll))
			if float64(ss.BytesReceivedFromNodes) > float64(ssAll.BytesReceivedFromNodes)*maxExpected*3 {
				t.Errorf("limit=%d over %d nodes should move roughly %d rows' worth of bytes,\nbut moved %.1f%% of the unlimited traffic.", limit, nodes, limit*nodes,
					100*float64(ss.BytesReceivedFromNodes)/float64(ssAll.BytesReceivedFromNodes))
			}
		}
	}
	t.Logf("====================================================================")

}

func TestLimitWithPartialResponse(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	q := &storage.Query{
		MinTimestamp:         math.MinInt64,
		MaxTimestamp:         math.MaxInt64,
		Limit:                500,
		AllowPartialResponse: true,
	}

	healthy, ssHealthy := c.Search(q)
	if ssHealthy.IsPartial() {
		t.Fatalf("healthy cluster reported partial: %s", ssHealthy)
	}
	if len(healthy) != 500 {
		t.Fatalf("limit=500 on a healthy cluster returned %d rows", len(healthy))
	}

	ns.kill(1)
	degraded, ssDegraded := c.Search(q)
	ns.revive(1)

	t.Logf("healthy:  %d rows, %s", len(healthy), ssHealthy)
	t.Logf("1 down:   %d rows, %s", len(degraded), ssDegraded)

	if !ssDegraded.IsPartial() {
		t.Fatalf("a node is down and a limited query reported COMPLETE: %s\n%d, so nothing downstream looks suspicious -- but the rows are wrong,", ssDegraded, len(degraded))
	}
	if len(degraded) > 500 {
		t.Errorf("limit=500 returned %d rows", len(degraded))
	}

}
