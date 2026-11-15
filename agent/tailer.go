package agent

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/niladrix719/smologdb/storage"
)

const DefaultPollInterval = 250 * time.Millisecond

const maxReadPerPoll = 4 << 20

type TailerConfig struct {
	Paths []string

	CheckpointPath string

	PollInterval time.Duration
}

func readNewLines(path string, offset int64) (lines [][]byte, newOffset int64, err error) {
	var f *os.File
	var st os.FileInfo
	var nr int
	var e error
	f, e = os.Open(path)
	if errors.Is(e, os.ErrNotExist) {
		return nil, offset, nil
	}
	if e != nil {
		return nil, offset, e
	}
	defer f.Close()
	st, e = f.Stat()
	if e != nil {
		return nil, offset, e
	}
	if offset > st.Size() { // truncated or replaced: start over
		offset = 0
	}
	if offset == st.Size() {
		return nil, offset, nil
	}
	n := min(st.Size()-offset, maxReadPerPoll)
	buf := make([]byte, n)
	nr, e = f.ReadAt(buf, offset)
	if e != nil && e != io.EOF {
		return nil, offset, e
	}
	buf = buf[:nr]

	var lns [][]byte
	// Bytes after the last newline are a line still being written; leave them for the next poll.
	last := bytes.LastIndexByte(buf, '\n')
	if last == -1 {
		if nr < maxReadPerPoll {
			return nil, offset, nil
		}
		log.Printf("tailer: line in %s longer than %d bytes, splitting it", path, maxReadPerPoll)
		return [][]byte{buf}, offset + int64(nr), nil
	}
	nwOffset := offset + int64(last) + 1
	chunk := buf[:last]
	for _, p := range bytes.Split(chunk, []byte("\n")) {
		p = bytes.TrimSuffix(p, []byte("\r"))
		if len(p) == 0 {
			continue
		}
		lns = append(lns, p)
	}
	return lns, nwOffset, nil
}

func lineToRow(path string, line []byte, now int64) storage.Row {
	labels := []storage.Field{{Name: "file", Value: path}}
	return storage.Row{
		Timestamp: now,
		StreamID:  storage.StreamIDForLabels(labels),
		Fields: []storage.Field{
			{Name: "_msg", Value: string(line)},
			{Name: "file", Value: path},
		},
	}
}

// mustWriteFileAtomic replaces path so that a crash leaves either the old or the new contents, never a mix.
func mustWriteFileAtomic(path string, data []byte) {
	var f *os.File
	var err error
	f, err = os.Create(path + ".tmp")
	if err != nil {
		panic(fmt.Sprintf("failed to create tailer file: %v", err))
	}
	_, err = f.Write(data)
	if err != nil {
		panic(fmt.Sprintf("failed to write tailer file: %v", err))
	}
	err = f.Sync()
	if err != nil {
		panic(fmt.Sprintf("failed to sync tailer file: %v", err))
	}
	err = f.Close()
	if err != nil {
		panic(fmt.Sprintf("failed to close tailer file: %v", err))
	}
	err = os.Rename(path+".tmp", path)
	if err != nil {
		panic(fmt.Sprintf("failed to rename tailer file: %v", err))
	}
	f, err = os.Open(filepath.Dir(path))
	if err != nil {
		panic(fmt.Sprintf("failed to open tailer directory: %v", err))
	}
	err = f.Sync()
	if err != nil {
		panic(fmt.Sprintf("failed to sync tailer directory: %v", err))
	}
	err = f.Close()
	if err != nil {
		panic(fmt.Sprintf("failed to close tailer directory: %v", err))
	}
}

func loadCheckpoints(path string) map[string]int64 {
	var b []byte
	var err error
	b, err = os.ReadFile(path)
	if err != nil {
		return map[string]int64{}
	}
	m := map[string]int64{}
	err = json.Unmarshal(b, &m)
	if err != nil {
		log.Printf("ignoring corrupt checkpoint file %s: %v", path, err)
		return map[string]int64{}
	}
	return m
}

func mustSaveCheckpoints(path string, cps map[string]int64) {
	var b []byte
	var err error
	b, err = json.Marshal(cps)
	if err != nil {
		panic(err)
	}
	mustWriteFileAtomic(path, b)
}

type Tailer struct {
	cfg         TailerConfig
	sink        FlushSink
	checkpoints map[string]int64
	stop        chan struct{}
	wg          sync.WaitGroup
}

func (t *Tailer) run() {
	defer t.wg.Done()
	tick := time.NewTicker(t.cfg.PollInterval)
	defer tick.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-tick.C:
			t.pollOnce()
		}
	}
}

func NewTailer(cfg *TailerConfig, sink FlushSink) *Tailer {
	t := &Tailer{
		cfg:         *cfg,
		sink:        sink,
		checkpoints: loadCheckpoints(cfg.CheckpointPath),
		stop:        make(chan struct{}),
	}
	t.cfg.PollInterval = cmp.Or(t.cfg.PollInterval, DefaultPollInterval)
	t.wg.Add(1)
	go t.run()
	return t
}

func (t *Tailer) pollOnce() {
	var rows []storage.Row
	newCheckpoints := map[string]int64{}
	for _, path := range t.cfg.Paths {
		lines, newoffset, err := readNewLines(path, t.checkpoints[path])
		if err != nil {
			log.Printf("tailer: read %s: %v", path, err)
			continue
		}
		for _, line := range lines {
			row := lineToRow(path, line, time.Now().UnixNano())
			rows = append(rows, row)
		}
		if t.checkpoints[path] != newoffset {
			newCheckpoints[path] = newoffset
		}
	}
	if len(rows) > 0 {
		t.sink.MustAddRows(rows)
		t.sink.MustFlush()
	}
	if len(newCheckpoints) == 0 {
		return
	}
	// Offsets move only after MustFlush has put the rows on disk, so a crash repeats lines but never skips them.
	for p, off := range newCheckpoints {
		t.checkpoints[p] = off
	}
	mustSaveCheckpoints(t.cfg.CheckpointPath, t.checkpoints)
}

// MustClose stops polling, then reads once more so lines written just before shutdown aren't left behind.
func (t *Tailer) MustClose() {
	close(t.stop)
	t.wg.Wait()
	t.pollOnce()
}
