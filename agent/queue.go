package agent

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage11 -v
//
// Reference: lib/persistentqueue/persistentqueue.go in VictoriaMetrics (it is
// vendored into the VictoriaLogs repo). The real one is ~700 lines and has an
// in-memory fast path in front of it (fastqueue.go). This is the ~150 lines
// underneath that make it durable. Read theirs AFTER yours passes the
// restart test, and look specifically at what MustWriteBlock does when the
// queue is full.
// ---------------------------------------------------------------------------

// The problem this file solves was written down in stage 7 and then left
// alone:
//
//	cluster: MustAddRows returns  ->  rows are in a client-side buffer, and
//	                                  will not survive process death at all
//
// A queue on disk is the smallest thing that closes that window. Rows go to
// disk before MustAddRows returns; a sender reads them back and ships them;
// a block is removed from the queue only after the destination has said it
// has it. Process death between any two of those steps loses nothing.
//
// It costs a write to disk per block, and that is the whole tradeoff. Stage
// 11 measures it.

// DefaultChunkFileSize is the size at which the writer moves to a new file.
//
// VictoriaLogs uses ~512MB (16 max-size blocks plus headers). 64MB is enough
// here: the point of chunking is that disk is reclaimed by deleting whole
// files once the reader is past them, and a smaller chunk means faster
// reclaim on a small dataset. The number is not important; that there IS a
// number -- rather than one file that grows forever -- is.
const DefaultChunkFileSize = 64 << 20

// MaxBlockSize bounds a single block. A block larger than this is a bug in
// the caller, not something the queue should try to accommodate.
const MaxBlockSize = 32 << 20

// QueueStats is what a queue can honestly report about itself.
type QueueStats struct {
	// PendingBytes is writerOffset - readerOffset: bytes on disk not yet
	// acknowledged. This is the number an operator watches during an outage.
	PendingBytes int64

	BlocksWritten int64
	BlocksAcked   int64

	// BlocksDropped counts blocks removed to stay under maxPendingBytes. They
	// were never sent. Non-zero here is data loss and must never be silent.
	BlocksDropped int64
	BytesDropped  int64
}

// queue is a durable FIFO of opaque byte blocks, backed by a directory.
//
// On disk:
//
//	<dir>/metainfo.json     {"ReaderOffset": N, "WriterOffset": M}
//	<dir>/<16 hex digits>   chunk files, named by the logical offset of
//	                        their first byte
//
// Offsets are LOGICAL: a single counter that increases for the life of the
// queue, across chunk files. Chunk k holds bytes [k*chunkFileSize,
// (k+1)*chunkFileSize) of that logical stream, and its name is that start
// offset, so "which file is offset N in" is arithmetic, not a search.
//
// A block on disk is:
//
//	8 bytes   little-endian uint64 length
//	length    payload
//
// Fixed-width, not varint, on purpose: recovery below scans headers to find
// the last complete block after a crash, and a fixed header makes "is there a
// whole header here" a length check.
//
// Concurrency: NONE. The queue is single-threaded by contract, same as the
// real one. The caller (destination, in agent.go) owns the lock. Putting a
// mutex in here as well would be two locks for one invariant.
type queue struct {
	dir             string
	chunkFileSize   int64
	maxBlockSize    int64
	maxPendingBytes int64

	readerOffset int64
	writerOffset int64

	reader *os.File
	writer *os.File

	peekedOffset int64
	peekedSize   int64

	blocksWritten int64
	blocksAcked   int64
	blocksDropped int64
	bytesDropped  int64
}

type metainfo struct {
	ReaderOffset int64
	WriterOffset int64
}

func (q *queue) mustWriteMetainfo() {
	b, err := json.Marshal(&metainfo{
		ReaderOffset: q.readerOffset,
		WriterOffset: q.writerOffset,
	})
	if err != nil {
		panic(err)
	}
	mustWriteFileAtomic(filepath.Join(q.dir, "metainfo.json"), b)
}

func readMetainfo(dir string) metainfo {
	var f *os.File
	var b []byte
	var err error
	f, err = os.Open(filepath.Join(dir, "metainfo.json"))
	if err != nil {
		return metainfo{}
	}
	defer f.Close()
	b, err = io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	var mi metainfo
	err = json.Unmarshal(b, &mi)
	if err != nil {
		log.Printf("failed to parse metainfo")
		return metainfo{}
	}
	if mi.ReaderOffset > mi.WriterOffset {
		log.Printf("corrupt metainfo")
		return metainfo{}
	}
	return mi
}

// mustOpenQueue opens or creates the queue at dir.
//
// maxPendingBytes bounds writerOffset-readerOffset; 0 means unbounded.
//
// Recovery is the part worth thinking about before writing:
//
//  1. Read metainfo. Missing or unparseable metainfo on an existing dir is
//     the "we crashed before the first flush" case -- treat it as empty and
//     say so in the log.
//  2. WriterOffset in metainfo may be BEHIND the end of the last chunk file,
//     because you crashed after writing a block but before flushing metainfo.
//     Scan forward from WriterOffset: while there is a complete header and a
//     complete payload, adopt the block and advance. Stop at the first
//     incomplete one and truncate the file there. Those bytes were a block
//     that never finished being written; the caller never got an ack for
//     them, so dropping them breaks no promise.
//  3. Delete any chunk file entirely before ReaderOffset. It was acked; it is
//     garbage.
//
// Step 2 is the write-ahead-log recovery rule in miniature, and it is why the
// header is fixed-width. Step 3 is why chunks exist.
func mustOpenQueue(dir string, maxPendingBytes int64) *queue {
	var fi os.FileInfo
	var err error
	var ents []os.DirEntry
	err = os.MkdirAll(dir, 0o755)
	if err != nil {
		panic(err)
	}
	mi := readMetainfo(dir)

	q := &queue{
		dir:             dir,
		chunkFileSize:   DefaultChunkFileSize,
		maxBlockSize:    MaxBlockSize,
		maxPendingBytes: maxPendingBytes,
		peekedOffset:    -1,
		readerOffset:    mi.ReaderOffset,
		writerOffset:    mi.WriterOffset,
	}

	startReader := mi.ReaderOffset - mi.ReaderOffset%q.chunkFileSize
	q.reader, err = os.OpenFile(filepath.Join(dir, fmt.Sprintf("%016X", startReader)), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		panic(err)
	}
	startWriter := mi.WriterOffset - mi.WriterOffset%q.chunkFileSize
	q.writer, err = os.OpenFile(filepath.Join(dir, fmt.Sprintf("%016X", startWriter)), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		panic(err)
	}

	fi, err = q.writer.Stat()
	if err != nil {
		panic(err)
	}
	fileEnd := fi.Size()
	local := q.writerOffset % q.chunkFileSize
	prev := local

	hdr := make([]byte, 8)
	for fileEnd-local >= 8 {
		_, err = q.writer.ReadAt(hdr, local)
		if err != nil {
			break
		}
		n := int64(binary.LittleEndian.Uint64(hdr))
		if n <= 0 || n > q.maxBlockSize || fileEnd-local-8 < n {
			break //garbage
		}
		q.writerOffset += 8 + n
		local += 8 + n
	}
	if fileEnd > local {
		err = q.writer.Truncate(local)
		if err != nil {
			panic(fmt.Sprintf("truncate torn tail: %v", err))
		}
	}
	if local != prev {
		q.mustWriteMetainfo()
	}
	readerStart := q.readerOffset - q.readerOffset%q.chunkFileSize
	ents, err = os.ReadDir(q.dir)
	if err != nil {
		panic(err)
	}
	for _, e := range ents {
		start, err := strconv.ParseInt(e.Name(), 16, 64)
		if len(e.Name()) != 16 || err != nil || start >= readerStart {
			continue
		}
		err = os.Remove(filepath.Join(q.dir, fmt.Sprintf("%016X", start)))
		if err != nil && !os.IsNotExist(err) {
			panic(fmt.Sprintf("failed to delete chunk file: %v", err))
		}
	}
	return q
}

// MustWriteBlock appends block and makes it durable before returning.
//
// "Durable" here means: written and flushed (fsync) to the chunk file, and
// metainfo updated. Measure what the fsync costs (stage 11 asks you to) --
// then decide whether every block needs one, or whether the FlushInterval in
// agent.go already bounds the window well enough that a periodic sync is
// honest. VictoriaLogs syncs periodically. Write down which you chose and
// what the number was.
//
// If maxPendingBytes is set and this block would push pending over it, drop
// the OLDEST blocks until it fits, counting each one. Oldest, not newest:
// during an outage the newest logs are the ones about the outage, and they
// are the ones someone is going to want. (The real one does the same; see
// MustWriteBlock in persistentqueue.go.) If the block alone exceeds
// maxPendingBytes, drop the block itself and count it.
//
// The alternative to dropping is blocking the producer -- backpressure. That
// trades data loss for a stalled application. Neither is right in general;
// the stage 11 notes ask which one you would want for YOUR logs.
func (q *queue) MustWriteBlock(block []byte) {
	var err error
	if len(block) > int(q.maxBlockSize) {
		panic("over max block size")
	}
	if q.maxPendingBytes > 0 && int64(len(block)+8) > q.maxPendingBytes {
		q.blocksDropped++
		q.bytesDropped += int64(len(block))
		log.Printf("dropping block due to size being greater than maxPendingBytes of %d", q.maxPendingBytes)
		return
	}
	for q.maxPendingBytes > 0 &&
		q.writerOffset != q.readerOffset &&
		q.writerOffset+8+int64(len(block))-q.readerOffset > q.maxPendingBytes {
		n, _ := q.readerBlockLen(q.readerOffset % q.chunkFileSize)
		q.blocksDropped++
		q.bytesDropped += n
		q.advanceReader(n)
	}
	local := q.writerOffset % q.chunkFileSize
	if q.maxBlockSize+8+local > q.chunkFileSize {
		err = q.writer.Close()
		if err != nil {
			panic(err)
		}
		q.writerOffset += q.chunkFileSize - local
		q.mustWriteMetainfo()
		local = 0
		q.writer, err = os.OpenFile(filepath.Join(q.dir, fmt.Sprintf("%016X", q.writerOffset)), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			panic(err)
		}
	}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, uint64(len(block)))
	_, err = q.writer.WriteAt(hdr, local)
	if err != nil {
		panic(err)
	}
	_, err = q.writer.WriteAt(block, local+8)
	if err != nil {
		panic(err)
	}
	err = q.writer.Sync()
	if err != nil {
		panic(err)
	}
	oldChuckStart := q.writerOffset - q.writerOffset%q.chunkFileSize
	q.writerOffset += int64(len(block)) + 8
	q.mustWriteMetainfo()
	q.blocksWritten++

	if newChuckStart := q.writerOffset - q.writerOffset%q.chunkFileSize; oldChuckStart != newChuckStart {
		err = q.writer.Close()
		if err != nil {
			panic(err)
		}
		q.writer, err = os.OpenFile(filepath.Join(q.dir, fmt.Sprintf("%016X", q.writerOffset)), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			panic(err)
		}
	}
}

func (q *queue) readerBlockLen(local int64) (int64, int64) {
	var err error
	if q.maxBlockSize+8+local > q.chunkFileSize {
		old := q.readerOffset - local
		err = q.reader.Close()
		if err != nil {
			panic(err)
		}
		q.readerOffset += q.chunkFileSize - local
		q.mustWriteMetainfo()
		local = 0
		q.reader, err = os.OpenFile(filepath.Join(q.dir, fmt.Sprintf("%016X", q.readerOffset)), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			panic(err)
		}
		err = os.Remove(filepath.Join(q.dir, fmt.Sprintf("%016X", old)))
		if err != nil && !os.IsNotExist(err) {
			panic(fmt.Sprintf("failed remove the acked chunk: %v", err))
		}
		if q.readerOffset == q.writerOffset {
			return 0, local
		}
	}
	hdr := make([]byte, 8)
	_, err = q.reader.ReadAt(hdr, local)
	if err != nil {
		panic(err)
	}
	return int64(binary.LittleEndian.Uint64(hdr)), local
}

func (q *queue) advanceReader(n int64) {
	oldChuckStart := q.readerOffset - q.readerOffset%q.chunkFileSize
	q.readerOffset += n + 8
	q.mustWriteMetainfo()
	if newChuckStart := q.readerOffset - q.readerOffset%q.chunkFileSize; oldChuckStart != newChuckStart {
		var err error
		err = q.reader.Close()
		if err != nil {
			panic(err)
		}
		q.reader, err = os.OpenFile(filepath.Join(q.dir, fmt.Sprintf("%016X", q.readerOffset)), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			panic(err)
		}
		err = os.Remove(filepath.Join(q.dir, fmt.Sprintf("%016X", oldChuckStart)))
		if err != nil && !os.IsNotExist(err) {
			panic(fmt.Sprintf("failed to remove chunk file: %v", err))
		}
	}
}

// MustPeekBlock returns the oldest unacknowledged block without removing it,
// appending to dst. ok is false if the queue is empty.
//
// Peek, not Read: the block stays at the head until MustAck. That is what
// makes the sender's contract simple -- crash anywhere between peek and ack,
// and the block is still there on restart. The real queue reads destructively
// and writes the block BACK on failure; peek/ack is the same guarantee with
// one fewer way to get it wrong.
//
// Consequence, which stage 11 measures: crash after the destination stored
// the block but before ack, and the block is sent again. At-least-once. This
// system has no row identity, so "again" means duplicate rows in storage.
func (q *queue) MustPeekBlock(dst []byte) ([]byte, bool) {
	var err error
	if q.readerOffset == q.writerOffset {
		return dst, false
	}
	local := q.readerOffset % q.chunkFileSize
	var n int64
	n, local = q.readerBlockLen(local)
	if n <= 0 {
		return dst, false
	}
	body := make([]byte, n)
	_, err = q.reader.ReadAt(body, local+8)
	if err != nil {
		panic(err)
	}
	dst = append(dst, body...)
	q.peekedOffset = q.readerOffset
	q.peekedSize = n
	return dst, true
}

// MustAck removes the block returned by the last MustPeekBlock.
//
// Advance readerOffset past the header and payload, persist metainfo, and
// delete any chunk file the reader has now fully left behind.
//
// Calling MustAck without a preceding peek is a bug; panic on it.
//
// One interaction to get right: MustWriteBlock may have DROPPED the peeked
// block (oldest-drop) between the peek and this ack. Then readerOffset has
// already moved past it and there is nothing to ack -- make this a no-op in
// that case, not a double advance. Remember the offset you peeked at, and
// only advance if readerOffset still equals it.
func (q *queue) MustAck() {
	if q.peekedOffset < 0 {
		panic("invalid peeked offset")
	}
	if q.readerOffset != q.peekedOffset {
		q.peekedOffset = -1
		return
	}
	oldChuckStart := q.readerOffset - q.readerOffset%q.chunkFileSize
	q.readerOffset += q.peekedSize + 8
	q.peekedOffset = -1
	q.blocksAcked++
	q.mustWriteMetainfo()

	if newChuckStart := q.readerOffset - q.readerOffset%q.chunkFileSize; oldChuckStart != newChuckStart {
		var err error
		err = q.reader.Close()
		if err != nil {
			panic(err)
		}
		q.reader, err = os.OpenFile(filepath.Join(q.dir, fmt.Sprintf("%016X", newChuckStart)), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			panic(err)
		}
		err = os.Remove(filepath.Join(q.dir, fmt.Sprintf("%016X", oldChuckStart)))
		if err != nil && !os.IsNotExist(err) {
			panic(fmt.Sprintf("failed remove the acked chunk: %v", err))
		}
	}
}

// Stats reports the counters. Cheap; the sender's stats call it per block.
func (q *queue) Stats() QueueStats {
	return QueueStats{
		PendingBytes:  q.writerOffset - q.readerOffset,
		BlocksWritten: q.blocksWritten,
		BlocksAcked:   q.blocksAcked,
		BlocksDropped: q.blocksDropped,
		BytesDropped:  q.bytesDropped,
	}
}

// MustClose flushes and closes. Reopening the same dir must see every block
// that was written and not acked.
func (q *queue) MustClose() {
	var err error
	err = q.reader.Close()
	if err != nil {
		panic(err)
	}
	err = q.writer.Close()
	if err != nil {
		panic(err)
	}
}
