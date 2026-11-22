package integration

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/netselect"
	"github.com/niladrix719/smologdb/netstorage"
	"github.com/niladrix719/smologdb/storage"
)

// hangingNode accepts requests and never answers them until the test ends.
func hangingNode(t *testing.T) string {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	return srv.Listener.Addr().String()
}

func searchWithin(t *testing.T, s *netselect.Storage, q *storage.Query, d time.Duration) ([]storage.Row, *storage.SearchStats, time.Duration) {
	t.Helper()
	type result struct {
		rows []storage.Row
		ss   *storage.SearchStats
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		rows, ss := s.Search(q)
		done <- result{rows, ss}
	}()
	select {
	case r := <-done:
		return r.rows, r.ss, time.Since(start)
	case <-time.After(d):
		t.Fatalf("Search was still waiting on the hung node after %v", d)
		return nil, nil, 0
	}
}

func TestQueryTimesOutOnHungNode(t *testing.T) {
	ns := newNodeSet(t, 2)
	for i := range 2 {
		ns.storages[i].MustAddRows([]storage.Row{testRow(uint64(i+1), 1e9, "_msg", fmt.Sprintf("node %d", i))})
	}
	ns.flushAll()

	addrs := append(append([]string{}, ns.addrs...), hangingNode(t))
	sel := netselect.NewStorage(&netselect.Config{Addrs: addrs, QueryTimeout: 300 * time.Millisecond})

	q := &storage.Query{MaxTimestamp: math.MaxInt64, AllowPartialResponse: true}
	rows, ss, took := searchWithin(t, sel, q, 5*time.Second)
	if len(rows) != 2 || ss.NodesFailed != 1 {
		t.Fatalf("partial query returned %d rows with %d failed nodes in %v, want 2 rows and 1 failed node", len(rows), ss.NodesFailed, took)
	}

	q.AllowPartialResponse = false
	rows, ss, took = searchWithin(t, sel, q, 5*time.Second)
	if rows != nil || ss.NodesFailed == 0 {
		t.Fatalf("strict query returned %d rows with %d failed nodes in %v, want an error result", len(rows), ss.NodesFailed, took)
	}
}

func TestRejectedBatchIsNotRerouted(t *testing.T) {
	ns := newNodeSet(t, 2)
	ns.rejectInserts(0, true)
	ins := ns.insertClient(netinsert.Config{})

	var rows []storage.Row
	for i := range 200 {
		rows = append(rows, testRow(uint64(i%20+1), int64(i+1)*1e9, "_msg", fmt.Sprintf("row %d", i)))
	}
	ins.MustAddRows(rows)
	ins.MustForceFlush()

	toNode0 := ins.NodeRowCounts()[0]
	if toNode0 == 0 {
		t.Fatalf("test setup: no rows were routed to node 0 (%v)", ins.NodeRowCounts())
	}
	for i, n := range ins.NodeReroutedRows() {
		if n != 0 {
			t.Fatalf("%d rows that node 0 refused as malformed were sent to node %d instead", n, i)
		}
	}
	if got := ins.RowsRejected(); got != toNode0 {
		t.Fatalf("RowsRejected = %d, want the %d rows routed to node 0", got, toNode0)
	}

	before := ns.servers[0].requests.Load()
	ins.MustAddRows(rows)
	ins.MustForceFlush()
	if ns.servers[0].requests.Load() == before {
		t.Fatalf("node 0 got no more requests: a 400 disabled it as if it were down")
	}
}

func TestNewFiltersOverTheWire(t *testing.T) {
	ns := newNodeSet(t, 2)
	c := ns.cluster(netstorage.Config{})
	c.MustAddRows([]storage.Row{
		testRow(1, 1e9, "_msg", "payment ok"),
		testRow(2, 2e9, "_msg", "payment failed"),
		testRow(3, 3e9, "_msg", "GET /health"),
	})
	c.MustForceFlush()

	cases := []struct {
		f    storage.Filter
		want int
	}{
		{storage.Filter{Column: "_msg", Token: "payment ok", Kind: storage.MatchExact}, 1},
		{storage.Filter{Column: "_msg", Token: "pay", Kind: storage.MatchPrefix}, 2},
		{storage.Filter{Column: "_msg", Token: "payment", Not: true}, 1},
	}
	for _, tc := range cases {
		got, _ := c.Search(&storage.Query{MaxTimestamp: math.MaxInt64, Filters: []storage.Filter{tc.f}})
		if len(got) != tc.want {
			t.Errorf("%+v through the router: %d rows, want %d", tc.f, len(got), tc.want)
		}
	}
}

func TestQueryCodecCarriesNewFields(t *testing.T) {
	in := storage.Query{
		MinTimestamp: 1, MaxTimestamp: 2, Limit: 10, Newest: true,
		Filters: []storage.Filter{{Column: "_msg", Token: "disk full", Kind: storage.MatchPhrase, Not: true}},
	}
	var out storage.Query
	if _, err := internalapi.UnmarshalQuery(&out, internalapi.MarshalQuery(nil, &in)); err != nil {
		t.Fatal(err)
	}
	if !out.Newest || len(out.Filters) != 1 || out.Filters[0] != in.Filters[0] {
		t.Fatalf("round trip lost fields: sent %+v, got %+v", in, out)
	}
}
