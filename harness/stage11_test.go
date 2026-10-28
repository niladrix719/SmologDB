package harness

// ---------------------------------------------------------------------------
// STAGE 11 MEASUREMENTS -- COMPLETE.
//
//	go test ./harness -run TestStage11 -v
//	go test ./harness -run TestStage11 -race    <- run this one too
//
// Stage 7 wrote down a window in which process death loses rows and left it
// open. This stage closes it with a queue on disk, and then measures what the
// queue costs and what it cannot fix: an outage becomes latency, a restart
// becomes nothing, and a lost response becomes a duplicate.
//
// Every test here uses the independent oracle (rowsOnNode reads the node's
// storage directly) and tagged rows with a fixed-width "seq" field, so that
// "how many rows" and "which rows" are both checkable.
// ---------------------------------------------------------------------------

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/niladrix719/minilog/agent"
	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
)

// taggedRows returns n rows with seq = offset..offset+n-1 as a fixed-width
// field, on one stream, with timestamps chosen so every row's varint width
// is identical. Identical widths mean identical encoded block sizes for
// identical n, which the disk-bound test relies on.
func taggedRows(n, offset int) []minilog.Row {
	rows := make([]minilog.Row, n)
	for i := range rows {
		rows[i] = minilog.Row{
			Timestamp: 1_000_000_000_000 + int64(offset+i),
			StreamID:  1,
			Fields: []minilog.Field{
				{Name: "_msg", Value: "agent test row"},
				{Name: "seq", Value: fmt.Sprintf("%06d", offset+i)},
			},
		}
	}
	return rows
}

// seqsOnNode returns the multiset of seq values stored on node i.
func (ns *nodeSet) seqsOnNode(i int) map[string]int {
	rows, _ := ns.storages[i].Search(&minilog.Query{
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

// waitFor polls cond every 10ms until it is true or timeout passes.
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

// TestStage11NoRowsLostAcrossOutage is the reason the agent exists.
//
// The destination goes away in the middle of ingest. MustAddRows must keep
// returning -- the application does not stop logging because the log store
// is down -- and every row must arrive once the destination is back.
func TestStage11NoRowsLostAcrossOutage(t *testing.T) {
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

	// The rows are accepted and on disk. They are not delivered, and the
	// agent must say so rather than pretend.
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
	t.Logf("MEASUREMENT accepted %d rows with destination down in %v; pending=%d bytes, retries=%d",
		half, acceptLatency, st.Queue.PendingBytes, st.Retries)

	ns.revive(0)
	start = time.Now()
	if !ag.WaitDrained(30 * time.Second) {
		t.Fatalf("did not drain after revive: %+v", ag.Stats())
	}
	t.Logf("MEASUREMENT drained backlog in %v after revive", time.Since(start))

	ns.flushAll()
	if got := ns.rowsOnNode(0); got != 2*half {
		t.Fatalf("ingested %d rows across an outage, %d arrived.\n"+
			"Rows accepted while the destination is down must be written to the\n"+
			"queue and sent when it returns. Check that MustFlush writes to every\n"+
			"queue and that runSender retries until sendStored.",
			2*half, got)
	}
	if d := ag.Stats()[0].Queue.BlocksDropped; d != 0 {
		t.Fatalf("unbounded queue dropped %d blocks", d)
	}
}

// TestStage11SurvivesRestart is the contract stage 7 could not make.
//
// Everything is ingested while the destination is down, and the agent is
// then closed -- the backlog exists only on disk. A new agent on the same
// directory must find it and deliver it.
func TestStage11SurvivesRestart(t *testing.T) {
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
		t.Fatalf("MustFlush returned but nothing is on disk under %s.\n"+
			"MustFlush must not return until the block is in a chunk file.", dir)
	}

	// Still down. The backlog must be visible before any new row is added.
	ag2 := ns.agentFor(dir, agent.Config{}, 0)
	defer ag2.MustClose()
	if got := ag2.Stats()[0].Queue.PendingBytes; got != pending {
		t.Fatalf("reopened queue reports %d pending bytes, want %d.\n"+
			"mustOpenQueue must recover reader/writer offsets from metainfo and\n"+
			"the chunk files. If this is 0, the backlog was lost on restart --\n"+
			"which is the exact failure the agent exists to prevent.",
			got, pending)
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
	t.Logf("MEASUREMENT %d rows survived close/reopen with %d bytes queued on disk", n, pending)
}

// TestStage11RecoversTornWrite: a crash in the middle of writing a block.
//
// The agent is closed cleanly, then junk is appended to the newest chunk
// file -- what the file looks like if the process died with a partial block
// half-written. Reopening must truncate the torn tail and deliver every
// complete block, without panicking and without losing anything that was
// acknowledged as flushed.
func TestStage11RecoversTornWrite(t *testing.T) {
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
	// A header claims 1MB, and then the process died after 5 bytes of it.
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
		t.Fatalf("%d distinct rows after torn-write recovery, want %d.\n"+
			"mustOpenQueue must scan forward from metainfo's WriterOffset, adopt\n"+
			"complete blocks, and truncate at the first incomplete one.", got, n)
	}
	t.Logf("MEASUREMENT torn trailing write truncated; %d rows delivered", n)
}

// TestStage11PoisonBlockDoesNotStall is the 400 rule with the queue's
// consequence attached.
//
// A block the destination rejects is not going to be accepted next time
// either. If the sender retries it, it sits at the head of the queue and
// every block behind it waits forever. Dropping it is the only way the
// pipeline keeps moving without a human.
func TestStage11PoisonBlockDoesNotStall(t *testing.T) {
	ns := newNodeSet(t, 1)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)
	defer ag.MustClose()

	const poison, good = 1_000, 5_000

	ns.rejectInserts(0, true)
	ag.MustAddRows(taggedRows(poison, 0))
	ag.MustFlush()
	if !waitFor(10*time.Second, func() bool { return ag.Stats()[0].BlocksRejected >= 1 }) {
		t.Fatalf("destination answered 400 and BlocksRejected stayed 0: %+v\n"+
			"A 4xx must be classified sendRejected and dropped, not retried.",
			ag.Stats())
	}
	ns.rejectInserts(0, false)

	ag.MustAddRows(taggedRows(good, poison))
	ag.MustFlush()
	if !ag.WaitDrained(10 * time.Second) {
		t.Fatalf("queue did not drain after the poison block: %+v\n"+
			"The rejected block is still at the head. See sendResult.", ag.Stats())
	}
	ns.flushAll()
	seqs := ns.seqsOnNode(0)
	if len(seqs) != good {
		t.Fatalf("%d distinct rows arrived, want %d (the %d poison rows dropped)",
			len(seqs), good, poison)
	}
	if _, ok := seqs["000000"]; ok {
		t.Fatalf("a row from the rejected block arrived")
	}
	t.Logf("MEASUREMENT 1 block rejected and dropped, %d rows behind it delivered", good)
}

// TestStage11OldestDropAtDiskBound: the queue is full and the destination
// is down. Something has to give, and it must be the oldest data, and the
// count must be reported.
func TestStage11OldestDropAtDiskBound(t *testing.T) {
	ns := newNodeSet(t, 1)

	const perBatch, batches, keep = 200, 6, 3
	blockLen := int64(len(internalapi.MarshalRowBatch(nil, taggedRows(perBatch, 0))))

	ns.kill(0)
	ag := ns.agentFor(t.TempDir(), agent.Config{
		MaxPendingBytes: keep*blockLen + 100, // room for keep blocks plus headers, not keep+1
	}, 0)
	defer ag.MustClose()

	for b := 0; b < batches; b++ {
		ag.MustAddRows(taggedRows(perBatch, b*perBatch))
		ag.MustFlush()
	}
	st := ag.Stats()[0].Queue
	if st.BlocksDropped != batches-keep {
		t.Fatalf("wrote %d blocks into room for %d and BlocksDropped=%d, want %d.\n"+
			"MustWriteBlock must drop the OLDEST blocks until the new one fits, and\n"+
			"count every one. Silent drops are the one thing this stage forbids.",
			batches, keep, st.BlocksDropped, batches-keep)
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
		t.Fatalf("seq 000000 arrived: the OLDEST block survived and a newer one was dropped.\n" +
			"Drop from the reader end, not the writer end. During an outage the\n" +
			"newest rows are the ones about the outage.")
	}
	t.Logf("MEASUREMENT queue bound %d bytes: %d of %d blocks dropped (oldest), newest %d rows delivered",
		keep*blockLen+100, st.BlocksDropped, batches, keep*perBatch)
}

// TestStage11ReplicaIsolation: two destinations, one dead. The live one must
// not notice.
func TestStage11ReplicaIsolation(t *testing.T) {
	ns := newNodeSet(t, 2)
	ag := ns.agentFor(t.TempDir(), agent.Config{}, 0, 1)
	defer ag.MustClose()

	ns.kill(1)
	const n = 20_000
	ag.MustAddRows(taggedRows(n, 0))
	ag.MustFlush()

	start := time.Now()
	if !waitFor(5*time.Second, func() bool { return ag.Stats()[0].Queue.PendingBytes == 0 }) {
		t.Fatalf("live destination did not drain while the other was down: %+v\n"+
			"Each destination needs its own queue AND its own sender. A shared\n"+
			"sender blocks on the dead one.", ag.Stats())
	}
	liveDrain := time.Since(start)
	if ag.Stats()[1].Queue.PendingBytes == 0 {
		t.Fatalf("dead destination reports nothing pending: %+v", ag.Stats())
	}
	if ag.WaitDrained(200 * time.Millisecond) {
		t.Fatalf("WaitDrained true with one destination down")
	}
	t.Logf("MEASUREMENT live replica drained in %v with the other replica dead", liveDrain)

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
	t.Logf("MEASUREMENT both replicas hold all %d rows", n)
}

// TestStage11AtLeastOnceDuplicates is the honest measurement.
//
// The destination stores the block and then the response is lost. The agent
// cannot tell this from a block that was never stored, so it sends it again.
// The queue guarantees at-least-once; this system has no row identity; so
// "at least once" is visible as duplicate rows. Count them.
//
// There is no assertion on HOW MANY duplicates, because that is timing. There
// is an assertion that there is at least one -- if there is not, the agent is
// not retrying on a 5xx and the outage test only passed by luck -- and that
// no row is missing, because at-least-once is still the promise.
func TestStage11AtLeastOnceDuplicates(t *testing.T) {
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
		t.Fatalf("the destination stored the block and failed the response, and no\n"+
			"duplicate arrived (%d rows for %d sent). Either the sender is not\n"+
			"retrying a 5xx, or it is acking before the destination confirms.", total, n)
	}
	t.Logf("MEASUREMENT store-then-fail: %d rows sent, %d stored, %d duplicates (%d retries)",
		n, total, total-n, ag.Stats()[0].Retries)
	t.Logf("NOTE This is the cost of at-least-once in a store with no row identity.\n" +
		"     The alternatives are exactly-once (a deduplication key on every row,\n" +
		"     and the storage node remembering what it has seen) or at-most-once\n" +
		"     (ack before send, and lose the block on a lost response). VictoriaLogs\n" +
		"     chooses this one. Write down which you would choose for your logs.")
}

// TestStage11FlushCost measures what the queue costs: a disk write per block.
//
// Same rows, two block sizes. The per-block cost (fsync plus one request)
// is fixed; the per-row cost is not. The ratio between the two lines is the
// argument for batching in front of the queue, in numbers.
func TestStage11FlushCost(t *testing.T) {
	const n = 100_000
	t.Logf("MEASUREMENT ================= FLUSH COST =================")
	t.Logf("MEASUREMENT %10s %10s %12s %10s", "rows/block", "blocks", "ingest", "rows/s")
	for _, perBlock := range []int{100, 1_000, 10_000} {
		ns := newNodeSet(t, 1)
		ag := ns.agentFor(t.TempDir(), agent.Config{}, 0)

		start := time.Now()
		for off := 0; off < n; off += perBlock {
			ag.MustAddRows(taggedRows(perBlock, off))
			ag.MustFlush()
		}
		if !ag.WaitDrained(60 * time.Second) {
			t.Fatalf("did not drain: %+v", ag.Stats())
		}
		elapsed := time.Since(start)
		ag.MustClose()

		ns.flushAll()
		if got := ns.rowsOnNode(0); got != n {
			t.Fatalf("perBlock=%d: %d rows arrived, want %d", perBlock, got, n)
		}
		t.Logf("MEASUREMENT %10d %10d %12v %10.0f",
			perBlock, n/perBlock, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
	}
	t.Logf("MEASUREMENT ====================================================")
	t.Logf("NOTE If the three rows/s numbers are close, the queue is not syncing per\n" +
		"     block and FlushInterval is doing the batching. If they are far apart,\n" +
		"     the fsync is the cost, and DefaultMaxBlockSize is the knob. Either is\n" +
		"     defensible; write down which you have and why.")
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

var chunkFileName = regexp.MustCompile(`^[0-9A-Fa-f]{16}$`)

// hasNonEmptyFile reports whether any regular file under dir has data.
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

// newestChunkFile returns the highest-named chunk file under dir.
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
