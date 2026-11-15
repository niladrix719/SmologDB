package integration

import (
	"math"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/storage"
)

func ingestVia(t *testing.T, ins *netinsert.Storage, ns *nodeSet, ds *gen.Dataset, batch int) {
	t.Helper()
	rows := ds.Rows
	for len(rows) > 0 {
		n := batch
		if n > len(rows) {
			n = len(rows)
		}
		ins.MustAddRows(rows[:n])
		rows = rows[n:]
	}
	ins.MustForceFlush()
	ns.flushAll()
}

func TestRoutingLosesNoRows(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	for _, policy := range []netinsert.RoutingPolicy{
		netinsert.RoutingHybrid, netinsert.RoutingSticky, netinsert.RoutingSpray,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			ns := newNodeSet(t, 4)
			ins := ns.insertClient(netinsert.Config{Routing: policy})
			ingestVia(t, ins, ns, ds, 5_000)

			total := 0
			for _, c := range ns.rowCounts() {
				total += c
			}
			if total != len(ds.Rows) {
				t.Fatalf("ingested %d rows, %d landed on nodes (%v).", len(ds.Rows), total, ns.rowCounts())
			}

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
				t.Fatalf("union of all nodes != ingested rows: %s", msg)
			}

			t.Logf("%-6s: %d rows -> %v", policy, total, ns.rowCounts())
		})
	}
}

func TestRouterConcurrency(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	ns := newNodeSet(t, 4)
	ins := ns.insertClient(netinsert.Config{Routing: netinsert.RoutingHybrid})

	const writers = 8
	chunk := len(ds.Rows) / writers
	done := make(chan struct{}, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			lo := w * chunk
			hi := lo + chunk
			if w == writers-1 {
				hi = len(ds.Rows)
			}
			for off := lo; off < hi; off += 1000 {
				end := off + 1000
				if end > hi {
					end = hi
				}

				batch := make([]storage.Row, end-off)
				copy(batch, ds.Rows[off:end])
				ins.MustAddRows(batch)
			}
		}(w)
	}
	for w := 0; w < writers; w++ {
		<-done
	}
	ins.MustForceFlush()
	ns.flushAll()

	total := 0
	for _, c := range ns.rowCounts() {
		total += c
	}
	if total != len(ds.Rows) {
		t.Fatalf("concurrent ingest: %d rows in, %d landed (%v).", len(ds.Rows), total, ns.rowCounts())
	}
	t.Logf("%d writers, %d rows, all accounted for: %v", writers, total, ns.rowCounts())
}
