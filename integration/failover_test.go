package integration

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/netstorage"
	"github.com/niladrix719/smologdb/storage"
)

func TestIngestSurvivesNodeLoss(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 120_000
	ds := gen.Generate(cfg)

	ns := newNodeSet(t, 4)
	ins := ns.insertClient(netinsert.Config{
		Routing:             netinsert.RoutingHybrid,
		NodeDisableDuration: 200 * time.Millisecond,
	})

	third := len(ds.Rows) / 3
	for off := 0; off < third; off += 5000 {
		ins.MustAddRows(ds.Rows[off:minInt(off+5000, third)])
	}
	ins.MustForceFlush()

	ns.kill(1)
	for off := third; off < 2*third; off += 5000 {
		ins.MustAddRows(ds.Rows[off:minInt(off+5000, 2*third)])
	}
	ins.MustForceFlush()

	ns.revive(1)
	for off := 2 * third; off < len(ds.Rows); off += 5000 {
		ins.MustAddRows(ds.Rows[off:minInt(off+5000, len(ds.Rows))])
	}
	ins.MustForceFlush()
	ns.flushAll()

	var all []storage.Row
	for i := range ns.storages {
		rows, _ := ns.storages[i].Search(&storage.Query{
			MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
		})
		all = append(all, rows...)
	}
	storage.SortRows(all)

	want := make([]storage.Row, len(ds.Rows))
	copy(want, ds.Rows)
	storage.SortRows(want)

	if ok, msg := rowsEqual(all, want); !ok {
		t.Fatalf("rows were lost or duplicated across a node outage: %s\ncounts per node: %v", msg, ns.rowCounts())
	}

	t.Logf("%d rows survived a node outage mid-ingest: %v", len(all), ns.rowCounts())
	t.Logf("rerouted rows by node: %v", ins.NodeReroutedRows())
}

func TestRerouteSpreadsLoad(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	ns := newNodeSet(t, 4)
	ins := ns.insertClient(netinsert.Config{
		Routing:             netinsert.RoutingHybrid,
		NodeDisableDuration: 50 * time.Millisecond,
		MaxInsertBlockSize:  256 << 10,
	})

	ns.kill(0)
	ingestVia(t, ins, ns, ds, 2_000)
	ns.revive(0)

	counts := ns.rowCounts()
	survivors := counts[1:]

	t.Logf("rows per node with node 0 down: %v", counts)
	t.Logf("reroutes by destination:        %v", ins.NodeReroutedRows())

	if counts[0] != 0 {
		t.Errorf("node 0 was down for the whole ingest but holds %d rows", counts[0])
	}

	ratio := balanceRatio(survivors)
	t.Logf("survivor balance: %.2fx", ratio)
	if ratio > 1.6 {
		t.Errorf("the three surviving nodes are unbalanced at %.2fx (%v).", ratio, survivors)
	}
}

func TestQueryFailsWhenNodeDown(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	q := &storage.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
	full, ssFull := c.Search(q)
	if ssFull.IsPartial() {
		t.Fatalf("healthy cluster reported a partial result: %s", ssFull)
	}

	ns.kill(2)
	defer ns.revive(2)

	got, ss := c.Search(q)

	if !ss.IsPartial() {
		t.Fatalf("a node is down and the query reported a COMPLETE result of %d rows\n(the healthy cluster returns %d).\nanswer. Stats: %s", len(got), len(full), ss)
	}
	if ss.NodesFailed != 1 {
		t.Errorf("NodesFailed=%d, want 1 (exactly one node was killed): %s", ss.NodesFailed, ss)
	}
	if len(got) >= len(full) {
		t.Errorf("a node holding %d of %d rows is down, yet the query returned %d rows.", ns.rowsOnNode(2), len(full), len(got))
	}

	t.Logf("healthy: %d rows, complete", len(full))
	t.Logf("1/4 nodes down, strict: %d rows, %s", len(got), ss)
}

func TestPartialResultIsMarked(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	strict := &storage.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
	partialQ := &storage.Query{
		MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
		AllowPartialResponse: true,
	}

	full, _ := c.Search(strict)
	fullSet := rowKeySet(full)

	for _, down := range [][]int{{0}, {1, 3}, {0, 1, 2}} {
		for _, i := range down {
			ns.kill(i)
		}

		got, ss := c.Search(partialQ)

		if !ss.IsPartial() {
			t.Fatalf("%d of 4 nodes are down and the result claims to be COMPLETE.\nStats: %s", len(down), ss)
		}
		if ss.NodesFailed != len(down) {
			t.Errorf("%d nodes down, NodesFailed=%d", len(down), ss.NodesFailed)
		}

		if len(got) > len(full) {
			t.Fatalf("partial result has MORE rows (%d) than the complete one (%d)", len(got), len(full))
		}
		for i := range got {
			if _, ok := fullSet[rowKey(&got[i])]; !ok {
				t.Fatalf("partial result contains a row that is not in the complete answer.\n  row: %+v", got[i])
			}
		}
		if !isSorted(got) {
			t.Fatalf("partial result is not sorted by (StreamID, Timestamp)")
		}

		t.Logf("%d/4 nodes down: %d of %d rows (%.1f%%), %s", len(down), len(got), len(full), 100*float64(len(got))/float64(len(full)), ss)

		for _, i := range down {
			ns.revive(i)
		}
	}
}

func TestAllNodesDownIsAnError(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 3, netinsert.RoutingSpray, ds)
	for i := range ns.storages {
		ns.kill(i)
	}
	defer func() {
		for i := range ns.storages {
			ns.revive(i)
		}
	}()

	q := &storage.Query{
		MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
		AllowPartialResponse: true,
	}
	got, ss := c.Search(q)

	if ss.NodesFailed != ss.NodesQueried {
		t.Fatalf("every node is down but NodesFailed=%d of NodesQueried=%d", ss.NodesFailed, ss.NodesQueried)
	}
	if len(got) != 0 {
		t.Fatalf("every node is down and the query returned %d rows", len(got))
	}
	if !ss.IsPartial() {
		t.Fatalf("every node is down and the result does not report as partial: %s", ss)
	}
	t.Logf("all nodes down: 0 rows, %s", ss)
}

func TestPartialDoesNotHideErrors(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	ds := gen.Generate(cfg)

	ns, c := newClusterWith(t, 4, netinsert.RoutingSpray, ds)

	q := &storage.Query{
		MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
		AllowPartialResponse: true,
	}

	ns.kill(1)
	gotDown, ssDown := c.Search(q)
	ns.revive(1)
	if !ssDown.IsPartial() || len(gotDown) == 0 {
		t.Fatalf("a downed node under AllowPartialResponse should degrade, not fail:\ngot %d rows, %s", len(gotDown), ssDown)
	}
	t.Logf("node down + partial:  %d rows, %s", len(gotDown), ssDown)

	ns.fault(1)
	gotFault, ssFault := c.Search(q)
	ns.heal(1)

	t.Logf("node erroring + partial: %d rows, %s", len(gotFault), ssFault)

	if !ssFault.IsPartial() {
		t.Fatalf("a node answered every request with 500 and the result claims to be\nCOMPLETE: %s", ssFault)
	}
	if len(gotFault) != 0 && len(gotFault) >= len(gotDown)+ns.rowsOnNode(1) {
		t.Errorf("the erroring node's rows appear in the result (%d rows) -- a 500 was", len(gotFault))
	}

}

func rowKey(r *storage.Row) string {
	s := fmt.Sprintf("%d|%d", r.StreamID, r.Timestamp)
	for i := range r.Fields {
		s += "|" + r.Fields[i].Name + "=" + r.Fields[i].Value
	}
	return s
}

func rowKeySet(rows []storage.Row) map[string]struct{} {
	out := make(map[string]struct{}, len(rows))
	for i := range rows {
		out[rowKey(&rows[i])] = struct{}{}
	}
	return out
}

func isSorted(rows []storage.Row) bool {
	for i := 1; i < len(rows); i++ {
		if rows[i-1].StreamID > rows[i].StreamID {
			return false
		}
		if rows[i-1].StreamID == rows[i].StreamID && rows[i-1].Timestamp > rows[i].Timestamp {
			return false
		}
	}
	return true
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var _ = netstorage.Config{}
