package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/agent"
	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/netstorage"
	"github.com/niladrix719/smologdb/storage"
)

func newInsertNode(t *testing.T, ns *nodeSet) (addr string, c *netstorage.Storage) {
	t.Helper()
	c = ns.cluster(netstorage.Config{})
	srv := httptest.NewServer(internalapi.NewStorageNodeHandler(c))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), c
}

func agentToAddrs(dataPath string, addrs ...string) *agent.Agent {
	return agent.NewAgent(&agent.Config{
		Addrs:            addrs,
		DataPath:         dataPath,
		RetryMinInterval: 20 * time.Millisecond,
		RetryMaxInterval: 200 * time.Millisecond,
	})
}

func msgsOnNode(ns *nodeSet, i int) map[string]int {
	rows, _ := ns.storages[i].Search(&storage.Query{
		MinTimestamp: -1 << 63, MaxTimestamp: 1<<63 - 1,
	})
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		if v, ok := r.GetField("_msg"); ok {
			out[v]++
		}
	}
	return out
}

func totalMsgs(m map[string]int) int {
	n := 0
	for _, c := range m {
		n += c
	}
	return n
}

func TestAgentEndToEnd(t *testing.T) {
	ns := newNodeSet(t, 3)
	insAddr, c := newInsertNode(t, ns)

	ag := agentToAddrs(t.TempDir(), insAddr)
	defer ag.MustClose()
	rcv := httptest.NewServer(agent.NewReceiver(ag, &agent.ParseParams{StreamFields: []string{"app"}}))
	defer rcv.Close()

	const n = 3000
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"_msg":"e2e row","app":"app-%02d","seq":"%06d"}`+"\n", i%30, i)
	}
	resp, err := http.Post(rcv.URL+"/ingest", "application/x-ndjson", strings.NewReader(sb.String()))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("receiver answered %d", resp.StatusCode)
	}

	if !ag.WaitDrained(20 * time.Second) {
		t.Fatalf("agent did not drain to the insert node: %+v", ag.Stats())
	}

	c.MustForceFlush()
	ns.flushAll()

	distinct := map[string]int{}
	nonEmpty := 0
	for i := range ns.storages {
		seqs := ns.seqsOnNode(i)
		if len(seqs) > 0 {
			nonEmpty++
		}
		for s, c := range seqs {
			distinct[s] += c
		}
	}
	if len(distinct) != n {
		t.Fatalf("%d distinct rows reached the storage nodes, want %d (per node: %v)", len(distinct), n, ns.rowCounts())
	}
	for s, c := range distinct {
		if c != 1 {
			t.Fatalf("seq %s stored %d times; the agent must not copy blocks to storage nodes behind the insert node's back", s, c)
		}
	}
	if nonEmpty < 2 {
		t.Fatalf("all rows landed on one node (%v): the insert node did not route", ns.rowCounts())
	}
	t.Logf("%d rows, 30 streams, one agent address -> per-node row counts %v", n, ns.rowCounts())

}

func appendTo(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}

func startTailer(ag *agent.Agent, logf, cp string) *agent.Tailer {
	return agent.NewTailer(&agent.TailerConfig{
		Paths:          []string{logf},
		CheckpointPath: cp,
		PollInterval:   10 * time.Millisecond,
	}, ag)
}

func checkpointAt(cpPath, path string) int64 {
	b, err := os.ReadFile(cpPath)
	if err != nil {
		return 0
	}
	var m map[string]int64
	if json.Unmarshal(b, &m) != nil {
		return 0
	}
	return m[path]
}

func caughtUp(t *testing.T, ns *nodeSet, ag *agent.Agent, cpPath, path string, want int64) {
	t.Helper()
	if !waitFor(15*time.Second, func() bool { return checkpointAt(cpPath, path) == want }) {
		t.Fatalf("tailer checkpoint is %d, want %d", checkpointAt(cpPath, path), want)
	}
	if !ag.WaitDrained(15 * time.Second) {
		t.Fatalf("agent did not drain: %+v", ag.Stats())
	}
	ns.flushAll()
}

func quiesce(t *testing.T, ns *nodeSet, ag *agent.Agent) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	if !ag.WaitDrained(15 * time.Second) {
		t.Fatalf("agent did not drain: %+v", ag.Stats())
	}
	ns.flushAll()
}

func TestTailerDelivers(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()
	dir := t.TempDir()
	logf, cp := filepath.Join(dir, "app.log"), filepath.Join(dir, "cp.json")

	appendTo(t, logf, "first\nsecond\n")
	tl := startTailer(ag, logf, cp)
	defer tl.MustClose()

	appendTo(t, logf, "par")
	time.Sleep(100 * time.Millisecond)
	appendTo(t, logf, "tial\nthird\n")

	caughtUp(t, ns, ag, cp, logf, int64(len("first\nsecond\npartial\nthird\n")))
	quiesce(t, ns, ag)

	want := []string{"first", "second", "partial", "third"}
	got := msgsOnNode(ns, 0)
	if len(got) != len(want) || totalMsgs(got) != len(want) {
		t.Fatalf("got %v, want exactly %v once each. 'par' or 'tial' in there means a half-written", got, want)
	}
	for _, w := range want {
		if got[w] != 1 {
			t.Fatalf("%q appears %d times in %v", w, got[w], got)
		}
	}
	rows, _ := ns.storages[0].Search(&storage.Query{MinTimestamp: -1 << 63, MaxTimestamp: 1<<63 - 1})
	for _, r := range rows {
		if v, _ := r.GetField("file"); v != logf {
			t.Fatalf("row %v has file=%q, want the path %q", r.Fields, v, logf)
		}
	}
	t.Logf("4 lines, one written in two pieces, delivered exactly once each")
}

func TestTailerResumesFromCheckpoint(t *testing.T) {
	ns := newNodeSet(t, 1)
	dir := t.TempDir()
	logf, cp := filepath.Join(dir, "app.log"), filepath.Join(dir, "cp.json")

	lines := func(from, to int) string {
		var sb strings.Builder
		for i := from; i < to; i++ {
			fmt.Fprintf(&sb, "line-%04d\n", i)
		}
		return sb.String()
	}

	appendTo(t, logf, lines(0, 100))
	ag1 := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	tl1 := startTailer(ag1, logf, cp)
	caughtUp(t, ns, ag1, cp, logf, 1000)
	tl1.MustClose()
	ag1.MustClose()

	appendTo(t, logf, lines(100, 150))
	ag2 := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	tl2 := startTailer(ag2, logf, cp)
	caughtUp(t, ns, ag2, cp, logf, 1500)
	quiesce(t, ns, ag2)
	got := msgsOnNode(ns, 0)
	if len(got) != 150 || totalMsgs(got) != 150 {
		t.Fatalf("%d distinct, %d stored; want 150 and 150. More stored than distinct means the", len(got), totalMsgs(got))
	}
	tl2.MustClose()
	ag2.MustClose()

	ag3 := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	other := filepath.Join(dir, "other-cp.json")
	tl3 := startTailer(ag3, logf, other)
	defer ag3.MustClose()
	defer tl3.MustClose()
	caughtUp(t, ns, ag3, other, logf, 1500)
	if n := totalMsgs(msgsOnNode(ns, 0)); n != 300 {
		t.Fatalf("no-checkpoint run: %d rows stored, want 300 (150 again)", n)
	}
	t.Logf("150 lines, restart with a checkpoint -> 0 duplicates; without one -> 150 duplicates")
}

func TestTailerHandlesTruncation(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()
	dir := t.TempDir()
	logf, cp := filepath.Join(dir, "app.log"), filepath.Join(dir, "cp.json")

	var old strings.Builder
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&old, "old-line-%02d\n", i)
	}
	appendTo(t, logf, old.String())
	tl := startTailer(ag, logf, cp)
	defer tl.MustClose()
	caughtUp(t, ns, ag, cp, logf, 600)

	if err := os.Truncate(logf, 0); err != nil {
		t.Fatal(err)
	}
	var fresh strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&fresh, "new-%02d\n", i)
	}
	appendTo(t, logf, fresh.String())

	caughtUp(t, ns, ag, cp, logf, 140)
	quiesce(t, ns, ag)
	got := msgsOnNode(ns, 0)
	if totalMsgs(got) != 70 || got["new-19"] != 1 || got["old-line-00"] != 1 {
		t.Fatalf("messages %v: want 50 old + 20 new, each once", got)
	}
	t.Logf("truncate + 20 new lines -> all 20 delivered, none of the 50 old ones repeated")
}

type fakeSink struct {
	mu      sync.Mutex
	added   int
	flushed int
}

func (s *fakeSink) MustAddRows(rows []storage.Row) {
	s.mu.Lock()
	s.added += len(rows)
	s.mu.Unlock()
}

func (s *fakeSink) MustFlush() {
	time.Sleep(3 * time.Millisecond)
	s.mu.Lock()
	s.flushed = s.added
	s.mu.Unlock()
}

func (s *fakeSink) flushedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushed
}

func TestCheckpointNeverRunsAheadOfFlush(t *testing.T) {
	dir := t.TempDir()
	logf := filepath.Join(dir, "app.log")
	cpPath := filepath.Join(dir, "cp.json")
	const lineLen = 8

	sink := &fakeSink{}
	tl := agent.NewTailer(&agent.TailerConfig{
		Paths: []string{logf}, CheckpointPath: cpPath, PollInterval: 5 * time.Millisecond,
	}, sink)

	stop := make(chan struct{})
	var violation string
	var maxSeen atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			b, err := os.ReadFile(cpPath)
			if err != nil {
				continue
			}
			var cps map[string]int64
			if json.Unmarshal(b, &cps) != nil {
				violation = "checkpoint file was not valid JSON when read: " + string(b)
				return
			}
			off := cps[logf]
			if flushed := int64(sink.flushedCount()) * lineLen; off > flushed {
				violation = fmt.Sprintf("checkpoint says %d bytes, but only %d bytes' worth of rows were flushed", off, flushed)
				return
			}
			if off > maxSeen.Load() {
				maxSeen.Store(off)
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	total := 0
	for wave := 0; wave < 6; wave++ {
		var sb strings.Builder
		for i := 0; i < 300; i++ {
			fmt.Fprintf(&sb, "L%06d\n", total+i)
		}
		total += 300
		appendTo(t, logf, sb.String())
		time.Sleep(40 * time.Millisecond)
	}
	waitFor(10*time.Second, func() bool { return maxSeen.Load() == int64(total)*lineLen })
	close(stop)
	wg.Wait()
	tl.MustClose()

	if violation != "" {
		t.Fatalf("%s.", violation)
	}
	if maxSeen.Load() != int64(total)*lineLen {
		t.Fatalf("the checkpoint only reached %d of %d bytes: the tailer made no progress, so the", maxSeen.Load(), int64(total)*lineLen)
	}
	t.Logf("%d lines in 6 waves; the checkpoint never passed the flushed count", total)
}
