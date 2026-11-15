package integration

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/agent"
	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/storage"
)

func taggedRows(n, offset int) []storage.Row {
	rows := make([]storage.Row, n)
	for i := range rows {
		rows[i] = storage.Row{
			Timestamp: 1_000_000_000_000 + int64(offset+i),
			StreamID:  1,
			Fields: []storage.Field{
				{Name: "_msg", Value: "agent test row"},
				{Name: "seq", Value: fmt.Sprintf("%06d", offset+i)},
			},
		}
	}
	return rows
}

func (ns *nodeSet) seqsOnNode(i int) map[string]int {
	rows, _ := ns.storages[i].Search(&storage.Query{
		MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64,
	})
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		if v, ok := r.GetField("seq"); ok {
			out[v]++
		}
	}
	return out
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func TestAgentNoRowsLostAcrossOutage(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()

	const half = 20_000
	ag.MustAddRows(taggedRows(half, 0))
	ag.MustFlush()
	if !ag.WaitDrained(10 * time.Second) {
		t.Fatalf("healthy destination did not drain: %+v", ag.Stats())
	}

	ns.kill(0)
	start := time.Now()
	ag.MustAddRows(taggedRows(half, half))
	ag.MustFlush()
	acceptLatency := time.Since(start)

	if ag.WaitDrained(500 * time.Millisecond) {
		t.Fatalf("WaitDrained reported drained with the destination down")
	}
	st := ag.Stats()[0]
	if st.Queue.PendingBytes == 0 {
		t.Fatalf("destination is down and the queue reports nothing pending: %+v", st)
	}
	if st.Retries == 0 {
		t.Fatalf("destination is down and the sender never retried: %+v", st)
	}
	t.Logf("accepted %d rows with destination down in %v; pending=%d bytes, retries=%d", half, acceptLatency, st.Queue.PendingBytes, st.Retries)

	ns.revive(0)
	start = time.Now()
	if !ag.WaitDrained(30 * time.Second) {
		t.Fatalf("did not drain after revive: %+v", ag.Stats())
	}
	t.Logf("drained backlog in %v after revive", time.Since(start))

	ns.flushAll()
	if got := ns.rowsOnNode(0); got != 2*half {
		t.Fatalf("ingested %d rows across an outage, %d arrived.", 2*half, got)
	}
	if d := ag.Stats()[0].Queue.BlocksDropped; d != 0 {
		t.Fatalf("unbounded queue dropped %d blocks", d)
	}
}

func TestAgentSurvivesRestart(t *testing.T) {
	ns := newNodeSet(t, 1)
	dir := t.TempDir()

	ns.kill(0)
	ag1 := ns.agentFor(dir, agent.Config{}, 0)
	const n = 30_000
	ag1.MustAddRows(taggedRows(n, 0))
	ag1.MustFlush()
	pending := ag1.Stats()[0].Queue.PendingBytes
	ag1.MustClose()

	if pending == 0 {
		t.Fatalf("nothing pending after MustFlush with the destination down")
	}
	if !hasNonEmptyFile(dir) {
		t.Fatalf("MustFlush returned but nothing is on disk under %s.", dir)
	}

	ag2 := ns.agentFor(dir, agent.Config{}, 0)
	defer ag2.MustClose()
	if got := ag2.Stats()[0].Queue.PendingBytes; got != pending {
		t.Fatalf("reopened queue reports %d pending bytes, want %d.", got, pending)
	}

	ns.revive(0)
	if !ag2.WaitDrained(30 * time.Second) {
		t.Fatalf("reopened agent did not drain: %+v", ag2.Stats())
	}
	ns.flushAll()
	seqs := ns.seqsOnNode(0)
	if len(seqs) != n {
		t.Fatalf("%d distinct rows arrived after restart, want %d", len(seqs), n)
	}
	t.Logf("%d rows survived close/reopen with %d bytes queued on disk", n, pending)
}

func TestAgentRecoversTornWrite(t *testing.T) {
	ns := newNodeSet(t, 1)
	dir := t.TempDir()

	ns.kill(0)
	ag1 := ns.agentFor(dir, agent.Config{}, 0)
	const n = 10_000
	ag1.MustAddRows(taggedRows(n, 0))
	ag1.MustFlush()
	ag1.MustClose()

	chunk := newestChunkFile(t, dir)
	f, err := os.OpenFile(chunk, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open chunk: %v", err)
	}

	torn := append(make([]byte, 0, 13), 0, 0, 0x10, 0, 0, 0, 0, 0, 'x', 'x', 'x', 'x', 'x')
	if _, err := f.Write(torn); err != nil {
		t.Fatalf("append torn block: %v", err)
	}
	_ = f.Close()

	ag2 := ns.agentFor(dir, agent.Config{}, 0)
	defer ag2.MustClose()
	ns.revive(0)
	if !ag2.WaitDrained(30 * time.Second) {
		t.Fatalf("did not drain after torn-write recovery: %+v", ag2.Stats())
	}
	ns.flushAll()
	if got := len(ns.seqsOnNode(0)); got != n {
		t.Fatalf("%d distinct rows after torn-write recovery, want %d.", got, n)
	}
	t.Logf("torn trailing write truncated; %d rows delivered", n)
}

func TestAgentSkipsRejectedBlock(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()

	const poison, good = 1_000, 5_000

	ns.rejectInserts(0, true)
	ag.MustAddRows(taggedRows(poison, 0))
	ag.MustFlush()
	if !waitFor(10*time.Second, func() bool { return ag.Stats()[0].BlocksRejected >= 1 }) {
		t.Fatalf("destination answered 400 and BlocksRejected stayed 0: %+v", ag.Stats())
	}
	ns.rejectInserts(0, false)

	ag.MustAddRows(taggedRows(good, poison))
	ag.MustFlush()
	if !ag.WaitDrained(10 * time.Second) {
		t.Fatalf("queue did not drain after the poison block: %+v", ag.Stats())
	}
	ns.flushAll()
	seqs := ns.seqsOnNode(0)
	if len(seqs) != good {
		t.Fatalf("%d distinct rows arrived, want %d (the %d poison rows dropped)", len(seqs), good, poison)
	}
	if _, ok := seqs["000000"]; ok {
		t.Fatalf("a row from the rejected block arrived")
	}
	t.Logf("1 block rejected and dropped, %d rows behind it delivered", good)
}

func TestAgentDropsOldestAtDiskLimit(t *testing.T) {
	ns := newNodeSet(t, 1)

	const perBatch, batches, keep = 200, 6, 3
	blockLen := int64(len(internalapi.MarshalRowBatch(nil, taggedRows(perBatch, 0))))

	ns.kill(0)
	ag := ns.agentFor(t.TempDir(), agent.Config{
		MaxPendingBytes: keep*blockLen + 100,
	}, 0)
	defer ag.MustClose()

	for b := 0; b < batches; b++ {
		ag.MustAddRows(taggedRows(perBatch, b*perBatch))
		ag.MustFlush()
	}
	st := ag.Stats()[0].Queue
	if st.BlocksDropped != batches-keep {
		t.Fatalf("wrote %d blocks into room for %d and BlocksDropped=%d, want %d.", batches, keep, st.BlocksDropped, batches-keep)
	}

	ns.revive(0)
	if !ag.WaitDrained(30 * time.Second) {
		t.Fatalf("did not drain: %+v", ag.Stats())
	}
	ns.flushAll()
	seqs := ns.seqsOnNode(0)
	if len(seqs) != keep*perBatch {
		t.Fatalf("%d distinct rows arrived, want %d (the newest %d blocks)", len(seqs), keep*perBatch, keep)
	}
	oldestKept := fmt.Sprintf("%06d", (batches-keep)*perBatch)
	if _, ok := seqs[oldestKept]; !ok {
		t.Fatalf("seq %s (first row of the oldest surviving block) is missing", oldestKept)
	}
	if _, ok := seqs["000000"]; ok {
		t.Fatalf("seq 000000 arrived: the OLDEST block survived and a newer one was dropped.")
	}
	t.Logf("queue bound %d bytes: %d of %d blocks dropped (oldest), newest %d rows delivered", keep*blockLen+100, st.BlocksDropped, batches, keep*perBatch)
}

func TestAgentDeadDestinationDoesNotSlowOthers(t *testing.T) {
	ns := newNodeSet(t, 2)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0, 1)
	defer ag.MustClose()

	ns.kill(1)
	const n = 20_000
	ag.MustAddRows(taggedRows(n, 0))
	ag.MustFlush()

	start := time.Now()
	if !waitFor(5*time.Second, func() bool { return ag.Stats()[0].Queue.PendingBytes == 0 }) {
		t.Fatalf("live destination did not drain while the other was down: %+v", ag.Stats())
	}
	liveDrain := time.Since(start)
	if ag.Stats()[1].Queue.PendingBytes == 0 {
		t.Fatalf("dead destination reports nothing pending: %+v", ag.Stats())
	}
	if ag.WaitDrained(200 * time.Millisecond) {
		t.Fatalf("WaitDrained true with one destination down")
	}
	t.Logf("live replica drained in %v with the other replica dead", liveDrain)

	ns.revive(1)
	if !ag.WaitDrained(30 * time.Second) {
		t.Fatalf("dead replica did not catch up after revive: %+v", ag.Stats())
	}
	ns.flushAll()
	for i := 0; i < 2; i++ {
		if got := len(ns.seqsOnNode(i)); got != n {
			t.Fatalf("replica %d has %d distinct rows, want %d", i, got, n)
		}
	}
	t.Logf("both replicas hold all %d rows", n)
}

func TestAgentAtLeastOnce(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()

	const n = 5_000
	ns.storeThenFail(0, true)
	ag.MustAddRows(taggedRows(n, 0))
	ag.MustFlush()
	if !waitFor(10*time.Second, func() bool { return ag.Stats()[0].Retries >= 1 }) {
		t.Fatalf("destination answered 503 and the sender never retried: %+v", ag.Stats())
	}
	ns.storeThenFail(0, false)

	if !ag.WaitDrained(30 * time.Second) {
		t.Fatalf("did not drain after healing: %+v", ag.Stats())
	}
	ns.flushAll()
	seqs := ns.seqsOnNode(0)
	total := ns.rowsOnNode(0)
	if len(seqs) != n {
		t.Fatalf("%d distinct rows, want %d: at-least-once lost something", len(seqs), n)
	}
	if total <= n {
		t.Fatalf("the destination stored the block and failed the response, and no\nduplicate arrived (%d rows for %d sent). Either the sender is not", total, n)
	}
	t.Logf("store-then-fail: %d rows sent, %d stored, %d duplicates (%d retries)", n, total, total-n, ag.Stats()[0].Retries)

}

var chunkFileName = regexp.MustCompile(`^[0-9A-Fa-f]{16}$`)

func hasNonEmptyFile(dir string) bool {
	found := false
	_ = filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil && info.Size() > 0 {
			found = true
		}
		return nil
	})
	return found
}

func newestChunkFile(t *testing.T, dir string) string {
	t.Helper()
	var newest string
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !chunkFileName.MatchString(d.Name()) {
			return nil
		}
		if newest == "" || d.Name() > filepath.Base(newest) {
			newest = path
		}
		return nil
	})
	if newest == "" {
		t.Fatalf("no chunk file (16 hex digits) under %s; see the queue's on-disk layout", dir)
	}
	return newest
}
