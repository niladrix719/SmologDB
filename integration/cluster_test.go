package integration

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/agent"
	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/netstorage"
	"github.com/niladrix719/smologdb/storage"
)

type gatedListener struct {
	net.Listener
	open atomic.Bool
}

func (l *gatedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.open.Load() {
			return c, nil
		}
		_ = c.Close()
	}
}

type nodeServer struct {
	h      http.Handler
	faulty atomic.Bool

	reject400 atomic.Bool

	storeThenFail atomic.Bool

	requests atomic.Int64
}

func (n *nodeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.requests.Add(1)
	if n.faulty.Load() {
		http.Error(w, "injected fault: this node is up but broken", http.StatusInternalServerError)
		return
	}
	if r.URL.Path == internalapi.InsertPath {
		if n.reject400.Load() {
			http.Error(w, "injected fault: this node rejects every insert", http.StatusBadRequest)
			return
		}
		if n.storeThenFail.Load() {
			rec := httptest.NewRecorder()
			n.h.ServeHTTP(rec, r)
			http.Error(w, "injected fault: stored, but the response was lost", http.StatusServiceUnavailable)
			return
		}
	}
	n.h.ServeHTTP(w, r)
}

type nodeSet struct {
	t         *testing.T
	storages  []*storage.Storage
	servers   []*nodeServer
	listeners []*gatedListener
	addrs     []string
}

func newNodeSet(t *testing.T, n int) *nodeSet {
	t.Helper()
	storage.ResetMetrics()

	ns := &nodeSet{t: t}
	for i := 0; i < n; i++ {
		st := storage.MustOpenStorage(t.TempDir(), &storage.Config{ShardsCount: 1})

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		gl := &gatedListener{Listener: ln}
		gl.open.Store(true)

		srv := &nodeServer{h: internalapi.NewStorageNodeHandler(st)}
		httpSrv := &http.Server{Handler: srv, ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = httpSrv.Serve(gl) }()

		ns.storages = append(ns.storages, st)
		ns.servers = append(ns.servers, srv)
		ns.listeners = append(ns.listeners, gl)
		ns.addrs = append(ns.addrs, ln.Addr().String())

		t.Cleanup(func() {
			_ = httpSrv.Close()
			st.MustClose()
		})
	}
	return ns
}

func (ns *nodeSet) kill(i int)   { ns.listeners[i].open.Store(false) }
func (ns *nodeSet) revive(i int) { ns.listeners[i].open.Store(true) }
func (ns *nodeSet) fault(i int)  { ns.servers[i].faulty.Store(true) }
func (ns *nodeSet) heal(i int)   { ns.servers[i].faulty.Store(false) }

func (ns *nodeSet) rejectInserts(i int, on bool) { ns.servers[i].reject400.Store(on) }
func (ns *nodeSet) storeThenFail(i int, on bool) { ns.servers[i].storeThenFail.Store(on) }

func (ns *nodeSet) flushAll() {
	for _, st := range ns.storages {
		st.MustForceFlush()
	}
}

func (ns *nodeSet) rowsOnNode(i int) int {
	rows, _ := ns.storages[i].Search(&storage.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
	})
	return len(rows)
}

func (ns *nodeSet) rowCounts() []int {
	out := make([]int, len(ns.storages))
	for i := range ns.storages {
		out[i] = ns.rowsOnNode(i)
	}
	return out
}

func (ns *nodeSet) insertClient(cfg netinsert.Config) *netinsert.Storage {
	cfg.Addrs = ns.addrs
	s := netinsert.NewStorage(&cfg)
	ns.t.Cleanup(s.MustClose)
	return s
}

func (ns *nodeSet) agentFor(dataPath string, cfg agent.Config, nodes ...int) *agent.Agent {
	for _, i := range nodes {
		cfg.Addrs = append(cfg.Addrs, ns.addrs[i])
	}
	cfg.DataPath = dataPath
	if cfg.RetryMinInterval == 0 {
		cfg.RetryMinInterval = 20 * time.Millisecond
	}
	if cfg.RetryMaxInterval == 0 {
		cfg.RetryMaxInterval = 200 * time.Millisecond
	}
	return agent.NewAgent(&cfg)
}

func (ns *nodeSet) cluster(cfg netstorage.Config) *netstorage.Storage {
	cfg.Addrs = ns.addrs
	s := netstorage.NewStorage(&cfg)
	ns.t.Cleanup(s.MustClose)
	return s
}

type directNode struct {
	addr string
	c    *http.Client
}

func newDirectNode(addr string) *directNode {
	return &directNode{addr: addr, c: &http.Client{Timeout: 30 * time.Second}}
}

func (d *directNode) urlFor(path string) string {
	return fmt.Sprintf("http://%s%s?%s=%s", d.addr, path,
		internalapi.VersionArg, url.QueryEscape(internalapi.ProtocolVersion))
}

func (d *directNode) MustAddRows(rows []storage.Row) {
	body := internalapi.MarshalRowBatch(nil, rows)
	resp, err := d.c.Post(d.urlFor(internalapi.InsertPath), "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		panic(fmt.Sprintf("directNode insert: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("directNode insert: status %d: %s", resp.StatusCode, b))
	}
}

func (d *directNode) Search(q *storage.Query) ([]storage.Row, *storage.SearchStats) {
	body := internalapi.MarshalQuery(nil, q)
	resp, err := d.c.Post(d.urlFor(internalapi.SelectPath), "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		panic(fmt.Sprintf("directNode select: %v", err))
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(fmt.Sprintf("directNode select: read body: %v", err))
	}
	if resp.StatusCode/100 != 2 {
		panic(fmt.Sprintf("directNode select: status %d: %s", resp.StatusCode, respBody))
	}
	rows, ss, err := internalapi.UnmarshalQueryResponse(respBody)
	if err != nil {
		panic(fmt.Sprintf("directNode select: decode response: %v", err))
	}
	return rows, ss
}

func (d *directNode) MustForceFlush() {
	resp, err := d.c.Get(d.urlFor(internalapi.ForceFlushPath))
	if err != nil {
		panic(fmt.Sprintf("directNode flush: %v", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("directNode flush: status %d: %s", resp.StatusCode, b))
	}
}

func (d *directNode) MustClose() {}

var _ storage.LogStorage = (*directNode)(nil)

func conformanceQueries(ds *gen.Dataset) []*storage.Query {
	full := func(fs ...storage.Filter) *storage.Query {
		return &storage.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64, Filters: fs}
	}
	lo, hi := ds.Rows[0].Timestamp, ds.Rows[len(ds.Rows)-1].Timestamp
	mid := lo + (hi-lo)/2
	sid := ds.Streams[3].ID

	return []*storage.Query{
		full(),
		{MinTimestamp: mid, MaxTimestamp: hi},
		{MinTimestamp: lo, MaxTimestamp: mid, StreamID: &sid},
		full(storage.Filter{Column: "_msg", Token: gen.RareToken}),
		full(storage.Filter{Column: "_msg", Token: gen.CommonToken}),
		full(storage.Filter{Column: "_msg", Token: gen.AbsentTokens(1, 7)[0]}),
		full(storage.Filter{Column: "level", Token: "error"}),
		full(storage.Filter{Column: "level", Token: "error"}, storage.Filter{Column: "method", Token: "GET"}),
	}
}

func runConformance(t *testing.T, s storage.LogStorage, ds *gen.Dataset, label string) {
	t.Helper()
	for i, q := range conformanceQueries(ds) {
		got, ss := s.Search(q)
		want := q.ApplyLimit(bruteForce(ds.Rows, q))
		if ok, msg := rowsEqual(got, want); !ok {
			t.Fatalf("%s: conformance query %d disagrees with brute force: %s\n  query: %+v\n  stats: %s", label, i, msg, q, ss)
		}
	}
}

func balanceRatio(counts []int) float64 {
	if len(counts) == 0 {
		return 0
	}
	maxN, sum := 0, 0
	for _, c := range counts {
		if c > maxN {
			maxN = c
		}
		sum += c
	}
	mean := float64(sum) / float64(len(counts))
	if mean == 0 {
		return 0
	}
	return float64(maxN) / mean
}

func httpPost(t *testing.T, url string, body []byte) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}
