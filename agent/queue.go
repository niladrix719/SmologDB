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

const DefaultChunkFileSize = 64 << 20

const MaxBlockSize = 32 << 20

type QueueStats struct {
	PendingBytes int64

	BlocksWritten int64
	BlocksAcked   int64

	BlocksDropped int64
	BytesDropped  int64
}

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
			break
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
	// Chunks entirely before the reader were acked but not deleted before a crash.
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

func (q *queue) Stats() QueueStats {
	return QueueStats{
		PendingBytes:  q.writerOffset - q.readerOffset,
		BlocksWritten: q.blocksWritten,
		BlocksAcked:   q.blocksAcked,
		BlocksDropped: q.blocksDropped,
		BytesDropped:  q.bytesDropped,
	}
}

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
