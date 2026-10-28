package harness

// ---------------------------------------------------------------------------
// STAGE 12 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage12 -v
//	go test ./harness -run TestStage12 -race
//
// Stage 11 made delivery durable. This stage gives the agent a front door and
// checks the whole path: the agent never routes (the insert node does), a
// receiver turns text into rows, and a tailer's checkpoint is the line between
// "read it again" and "lost it".
//
// The parsing rules and the tailer's pure pieces are tested in agent/ (go test
// ./agent -run TestStage12). These are the ones that need real nodes.
// ---------------------------------------------------------------------------

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

	"github.com/niladrix719/minilog/agent"
	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netstorage"
)

// newInsertNode builds the middle tier: an insert/select node in front of the
// storage nodes, serving the binary insert protocol over HTTP.
//
// No new code is involved. internalapi.NewStorageNodeHandler serves whatever
// minilog.LogStorage it is given, and a netstorage.Storage IS one -- so an
// insert node is a storage node whose "local storage" happens to be a
// cluster. cmd/minilogd's insert mode needs to do exactly this (see the
// STAGE 12 comment in runInsertSelectNode).
func newInsertNode(t *testing.T, ns *nodeSet) (addr string, c *netstorage.Storage) {
	t.Helper()
	c = ns.cluster(netstorage.Config{})
	srv := httptest.NewServer(internalapi.NewStorageNodeHandler(c))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), c
}

// agentToAddrs returns an agent forwarding to the given raw addresses.
func agentToAddrs(dataPath string, addrs ...string) *agent.Agent {
	return agent.NewAgent(&agent.Config{
		Addrs:            addrs,
		DataPath:         dataPath,
		RetryMinInterval: 20 * time.Millisecond,
		RetryMaxInterval: 200 * time.Millisecond,
	})
}

// msgsOnNode returns how many times each _msg value appears on node i.
func msgsOnNode(ns *nodeSet, i int) map[string]int {
	rows, _ := ns.storages[i].Search(&minilog.Query{
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

// TestStage12EndToEndThroughInsertNode is the topology test.
//
//	curl -> receiver -> agent -> insert node -> storage node 1..3
//
// The agent is given ONE address -- the insert node -- and the rows must come
// out SHARDED across the three storage nodes, each exactly once. If they come
// out three times each, the agent was pointed at the storage nodes and copied
// every block to all of them: replication by accident. Routing is the insert
// node's job; the agent only copies a block to every address it was given.
func TestStage12EndToEndThroughInsertNode(t *testing.T) {
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
	resp, err := http.Post(rcv.URL+"/insert/jsonline", "application/x-ndjson", strings.NewReader(sb.String()))
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
	// The agent's ack means the insert node BUFFERED the rows, not that the
	// storage nodes have them. Drain the insert node, then publish.
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
	t.Logf("MEASUREMENT %d rows, 30 streams, one agent address -> per-node row counts %v", n, ns.rowCounts())
	t.Logf("NOTE The agent's ack means 'the insert node buffered it'. Between that and a\n" +
		"     storage node there is still netinsert's FlushInterval of memory. The agent\n" +
		"     narrows where the loss window lives; it cannot remove it. The real\n" +
		"     VictoriaLogs has the same window at the same hop.")
}

// ---------------------------------------------------------------------------
// tailer
// ---------------------------------------------------------------------------

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

// checkpointAt returns the offset the tailer has durably checkpointed for path.
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

// caughtUp waits until the tailer's checkpoint is EXACTLY want bytes, then until
// the agent has delivered everything, and only then publishes node 0.
//
// The order is deliberate. The tailer's checkpoint advancing means its rows are
// in the agent's queue; WaitDrained means they have reached the node; only then
// is nothing inserting, so flushAll can run. (Calling flushAll on a node that is
// still receiving inserts is not safe in this engine: see the note in the stage
// 12 write-up about Storage.MustAddRows and shard.mu.)
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

// quiesce gives a buggy tailer time to re-send lines it should not, then
// publishes again, so the count that follows includes any such duplicates.
func quiesce(t *testing.T, ns *nodeSet, ag *agent.Agent) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	if !ag.WaitDrained(15 * time.Second) {
		t.Fatalf("agent did not drain: %+v", ag.Stats())
	}
	ns.flushAll()
}

// TestStage12TailerDelivers: lines that existed before the tailer started, and
// lines appended afterwards -- including one written in two pieces.
func TestStage12TailerDelivers(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()
	dir := t.TempDir()
	logf, cp := filepath.Join(dir, "app.log"), filepath.Join(dir, "cp.json")

	appendTo(t, logf, "first\nsecond\n")
	tl := startTailer(ag, logf, cp)
	defer tl.MustClose()

	appendTo(t, logf, "par")
	time.Sleep(100 * time.Millisecond) // several polls see a line with no newline
	appendTo(t, logf, "tial\nthird\n")

	caughtUp(t, ns, ag, cp, logf, int64(len("first\nsecond\npartial\nthird\n")))
	quiesce(t, ns, ag)

	want := []string{"first", "second", "partial", "third"}
	got := msgsOnNode(ns, 0)
	if len(got) != len(want) || totalMsgs(got) != len(want) {
		t.Fatalf("got %v, want exactly %v once each. 'par' or 'tial' in there means a half-written\n"+
			"line was consumed before its newline.", got, want)
	}
	for _, w := range want {
		if got[w] != 1 {
			t.Fatalf("%q appears %d times in %v", w, got[w], got)
		}
	}
	rows, _ := ns.storages[0].Search(&minilog.Query{MinTimestamp: -1 << 63, MaxTimestamp: 1<<63 - 1})
	for _, r := range rows {
		if v, _ := r.GetField("file"); v != logf {
			t.Fatalf("row %v has file=%q, want the path %q", r.Fields, v, logf)
		}
	}
	t.Logf("MEASUREMENT 4 lines, one written in two pieces, delivered exactly once each")
}

// TestStage12TailerResumesFromCheckpoint is what a checkpoint buys.
func TestStage12TailerResumesFromCheckpoint(t *testing.T) {
	ns := newNodeSet(t, 1)
	dir := t.TempDir()
	logf, cp := filepath.Join(dir, "app.log"), filepath.Join(dir, "cp.json")

	lines := func(from, to int) string {
		var sb strings.Builder
		for i := from; i < to; i++ {
			fmt.Fprintf(&sb, "line-%04d\n", i) // 10 bytes each
		}
		return sb.String()
	}

	// Run 1: 100 lines.
	appendTo(t, logf, lines(0, 100))
	ag1 := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	tl1 := startTailer(ag1, logf, cp)
	caughtUp(t, ns, ag1, cp, logf, 1000)
	tl1.MustClose()
	ag1.MustClose()

	// The app writes 50 more while the agent is down. Run 2 must send only those.
	appendTo(t, logf, lines(100, 150))
	ag2 := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	tl2 := startTailer(ag2, logf, cp)
	caughtUp(t, ns, ag2, cp, logf, 1500)
	quiesce(t, ns, ag2)
	got := msgsOnNode(ns, 0)
	if len(got) != 150 || totalMsgs(got) != 150 {
		t.Fatalf("%d distinct, %d stored; want 150 and 150. More stored than distinct means the\n"+
			"restarted tailer re-read lines the checkpoint said were delivered.", len(got), totalMsgs(got))
	}
	tl2.MustClose()
	ag2.MustClose()

	// A third tailer with NO checkpoint reads the whole file again.
	ag3 := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	other := filepath.Join(dir, "other-cp.json")
	tl3 := startTailer(ag3, logf, other)
	defer ag3.MustClose()
	defer tl3.MustClose()
	caughtUp(t, ns, ag3, other, logf, 1500)
	if n := totalMsgs(msgsOnNode(ns, 0)); n != 300 {
		t.Fatalf("no-checkpoint run: %d rows stored, want 300 (150 again)", n)
	}
	t.Logf("MEASUREMENT 150 lines, restart with a checkpoint -> 0 duplicates; without one -> 150 duplicates")
}

// TestStage12TailerHandlesTruncation: log rotation by copytruncate.
func TestStage12TailerHandlesTruncation(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()
	dir := t.TempDir()
	logf, cp := filepath.Join(dir, "app.log"), filepath.Join(dir, "cp.json")

	var old strings.Builder
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&old, "old-line-%02d\n", i) // 12 bytes each
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
		fmt.Fprintf(&fresh, "new-%02d\n", i) // 7 bytes each: shorter than the old content
	}
	appendTo(t, logf, fresh.String())

	caughtUp(t, ns, ag, cp, logf, 140) // the checkpoint went BACK to 140; stuck at 600 would time out here
	quiesce(t, ns, ag)
	got := msgsOnNode(ns, 0)
	if totalMsgs(got) != 70 || got["new-19"] != 1 || got["old-line-00"] != 1 {
		t.Fatalf("messages %v: want 50 old + 20 new, each once", got)
	}
	t.Logf("MEASUREMENT truncate + 20 new lines -> all 20 delivered, none of the 50 old ones repeated")
}

// fakeSink records how many rows were added and how many were FLUSHED, and
// makes MustFlush slow so that a wrong checkpoint order has a window to show in.
type fakeSink struct {
	mu      sync.Mutex
	added   int
	flushed int
}

func (s *fakeSink) MustAddRows(rows []minilog.Row) {
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

// TestStage12CheckpointNeverRunsAheadOfFlush is the invariant, checked from the
// outside: whenever the checkpoint file says "read up to byte N", the sink must
// already have flushed at least N bytes' worth of lines.
//
// A tailer that saves the checkpoint BEFORE MustFlush returns will, on a crash
// in that window, restart past lines that were never on disk. This test makes
// the window 3ms wide and watches for it from another goroutine.
func TestStage12CheckpointNeverRunsAheadOfFlush(t *testing.T) {
	dir := t.TempDir()
	logf := filepath.Join(dir, "app.log")
	cpPath := filepath.Join(dir, "cp.json")
	const lineLen = 8 // "L000001\n"

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
				continue // not written yet
			}
			var cps map[string]int64
			if json.Unmarshal(b, &cps) != nil {
				violation = "checkpoint file was not valid JSON when read: " + string(b)
				return
			}
			off := cps[logf]
			if flushed := int64(sink.flushedCount()) * lineLen; off > flushed { // checkpoint read FIRST, flushed after
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
		t.Fatalf("%s.\nOrder must be: MustAddRows, MustFlush, THEN save the checkpoint.", violation)
	}
	if maxSeen.Load() != int64(total)*lineLen {
		t.Fatalf("the checkpoint only reached %d of %d bytes: the tailer made no progress, so the\n"+
			"invariant above was never really tested", maxSeen.Load(), int64(total)*lineLen)
	}
	t.Logf("MEASUREMENT %d lines in 6 waves; the checkpoint never passed the flushed count", total)
}
