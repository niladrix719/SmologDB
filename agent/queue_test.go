package agent

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func smallQueue(t *testing.T, dir string, bound int64) *queue {
	q := mustOpenQueue(dir, bound)
	q.chunkFileSize = 100
	q.maxBlockSize = 30
	return q
}

func blk(seq, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(seq)
	}
	return b
}

func chunkFiles(t *testing.T, dir string) []string {
	ents, _ := os.ReadDir(dir)
	var out []string
	for _, e := range ents {
		if len(e.Name()) == 16 {
			out = append(out, e.Name())
		}
	}
	return out
}

func drain(t *testing.T, q *queue) [][]byte {
	var out [][]byte
	for {
		b, ok := q.MustPeekBlock(nil)
		if !ok {
			return out
		}
		out = append(out, b)
		q.MustAck()
	}
}

func TestQueueExactTiling(t *testing.T) {
	dir := t.TempDir()
	q := smallQueue(t, dir, 0)
	sizes := []int{22, 24, 30, 10}
	for i, n := range sizes {
		q.MustWriteBlock(blk(i+1, n))
	}
	got := drain(t, q)
	if len(got) != len(sizes) {
		t.Fatalf("got %d blocks, want %d", len(got), len(sizes))
	}
	for i, n := range sizes {
		if !bytes.Equal(got[i], blk(i+1, n)) {
			t.Fatalf("block %d wrong: %v", i, got[i])
		}
	}
	t.Logf("chunks left: %v", chunkFiles(t, dir))
}

func TestQueueRandomNoBound(t *testing.T) {
	for seed := int64(0); seed < 12; seed++ {
		rng := rand.New(rand.NewSource(seed))
		dir := t.TempDir()
		q := smallQueue(t, dir, 0)
		var model [][]byte
		seq := 0
		for step := 0; step < 120; step++ {
			switch rng.Intn(3) {
			case 0, 1:
				seq++
				b := blk(seq%250+1, 1+rng.Intn(30))
				q.MustWriteBlock(b)
				model = append(model, b)
			case 2:
				got, ok := q.MustPeekBlock(nil)
				if ok != (len(model) > 0) {
					t.Fatalf("seed %d step %d: ok=%v model=%d", seed, step, ok, len(model))
				}
				if ok {
					if !bytes.Equal(got, model[0]) {
						t.Fatalf("seed %d step %d: peek mismatch got %v want %v", seed, step, got, model[0])
					}
					q.MustAck()
					model = model[1:]
				}
			}
		}
		rest := drain(t, q)
		if len(rest) != len(model) {
			t.Fatalf("seed %d: drained %d want %d", seed, len(rest), len(model))
		}
		for i := range rest {
			if !bytes.Equal(rest[i], model[i]) {
				t.Fatalf("seed %d: tail block %d mismatch", seed, i)
			}
		}
		if n := len(chunkFiles(t, dir)); n > 2 {
			t.Fatalf("seed %d: %d chunk files left, chunks are leaking: %v", seed, n, chunkFiles(t, dir))
		}
		if q.Stats().PendingBytes != 0 {
			t.Fatalf("seed %d: pending %d after drain", seed, q.Stats().PendingBytes)
		}
	}
}

func TestQueueRandomBound(t *testing.T) {
	for seed := int64(0); seed < 12; seed++ {
		rng := rand.New(rand.NewSource(seed))
		dir := t.TempDir()
		q := smallQueue(t, dir, int64(150+rng.Intn(200)))
		var written [][]byte
		for i := 0; i < 80; i++ {
			b := blk(i%250+1, 1+rng.Intn(30))
			q.MustWriteBlock(b)
			written = append(written, b)
		}
		got := drain(t, q)
		st := q.Stats()
		if int(st.BlocksDropped)+len(got) != len(written) {
			t.Fatalf("seed %d: dropped %d + delivered %d != written %d", seed, st.BlocksDropped, len(got), len(written))
		}
		off := len(written) - len(got)
		for i := range got {
			if !bytes.Equal(got[i], written[off+i]) {
				t.Fatalf("seed %d: survivor %d is not the expected newest-suffix block", seed, i)
			}
		}
		if len(got) == 0 {
			t.Fatalf("seed %d: everything dropped", seed)
		}
	}
}

func TestQueueAckAfterDrop(t *testing.T) {
	dir := t.TempDir()
	q := smallQueue(t, dir, 120)
	q.MustWriteBlock(blk(1, 20))
	if _, ok := q.MustPeekBlock(nil); !ok {
		t.Fatal("peek failed")
	}
	for i := 2; i < 12; i++ {
		q.MustWriteBlock(blk(i, 20))
	}
	q.MustAck()
	rest := drain(t, q)
	for i := 1; i < len(rest); i++ {
		if rest[i][0] != rest[i-1][0]+1 {
			t.Fatalf("gap or reorder after stale ack: %v then %v", rest[i-1][0], rest[i][0])
		}
	}
	if rest[len(rest)-1][0] != 11 {
		t.Fatalf("newest block missing, last=%d", rest[len(rest)-1][0])
	}
}

func TestQueueRestart(t *testing.T) {
	dir := t.TempDir()
	q := mustOpenQueue(dir, 0)
	for i := 0; i < 50; i++ {
		q.MustWriteBlock(blk(i+1, 1000))
	}
	pend := q.Stats().PendingBytes
	q.MustClose()

	q2 := mustOpenQueue(dir, 0)
	if q2.Stats().PendingBytes != pend {
		t.Fatalf("reopen pending %d want %d", q2.Stats().PendingBytes, pend)
	}
	got := drain(t, q2)
	if len(got) != 50 || got[49][0] != 50 {
		t.Fatalf("got %d blocks", len(got))
	}
}

func TestQueueTornWrite(t *testing.T) {
	dir := t.TempDir()
	q := mustOpenQueue(dir, 0)
	for i := 0; i < 10; i++ {
		q.MustWriteBlock(blk(i+1, 500))
	}
	q.MustClose()

	mi := readMetainfo(dir)
	mi.WriterOffset = 6 * 508
	q2 := &queue{dir: dir, readerOffset: mi.ReaderOffset, writerOffset: mi.WriterOffset}
	q2.mustWriteMetainfo()

	f, _ := os.OpenFile(filepath.Join(dir, "0000000000000000"), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 0x10, 0, 0, 0, 0, 0, 'x', 'x', 'x', 'x', 'x'})
	f.Close()

	q3 := mustOpenQueue(dir, 0)
	got := drain(t, q3)
	if len(got) != 10 {
		t.Fatalf("recovered %d blocks, want 10 (4 adopted past stale metainfo, torn tail dropped)", len(got))
	}

	q3.MustWriteBlock(blk(99, 10))
	g, ok := q3.MustPeekBlock(nil)
	if !ok || g[0] != 99 {
		t.Fatalf("write after recovery came back wrong: %v %v", g, ok)
	}
}

func TestQueueOversizedBlockKeepsBacklog(t *testing.T) {
	q := smallQueue(t, t.TempDir(), 36)
	q.MustWriteBlock(blk(1, 10))
	q.MustWriteBlock(blk(2, 10))
	q.MustWriteBlock(blk(3, 30))
	if st := q.Stats(); st.BlocksDropped != 1 {
		t.Fatalf("BlocksDropped=%d, want 1 (the oversized block itself)", st.BlocksDropped)
	}
	got := drain(t, q)
	if len(got) != 2 || got[0][0] != 1 || got[1][0] != 2 {
		t.Fatalf("backlog was disturbed by an oversized block: %v", got)
	}
}

func TestQueueSweepsOrphanChunks(t *testing.T) {
	dir := t.TempDir()
	off := int64(2 * DefaultChunkFileSize)
	(&queue{dir: dir, readerOffset: off, writerOffset: off}).mustWriteMetainfo()
	os.WriteFile(filepath.Join(dir, "0000000000000000"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(dir, "0000000004000000"), []byte("x"), 0o644)
	q := mustOpenQueue(dir, 0)
	defer q.MustClose()
	if got := chunkFiles(t, dir); len(got) != 1 || got[0] != "0000000008000000" {
		t.Fatalf("orphans not swept, chunks left: %v", got)
	}
}
