package minilog

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Day partitions, the parts.json manifest, and crash recovery.
//
// Verified by:  go test ./harness -run 'TestStage3|TestStage4' -v
//               go run ./cmd/crashtest
//
// Reference: lib/logstorage/partition.go, datadb.go, storage.go.
// ---------------------------------------------------------------------------

const (
	partsFilename = "parts.json"
	partsDirname  = "parts"
)

// partsJSON is the manifest of live parts in a partition.
//
// This single file is the transaction log of the whole storage engine. A part
// directory exists on disk iff it was created; a part is LIVE iff it is named
// here. Everything about crash safety reduces to updating this file
// atomically and in the right order relative to creating and deleting part
// directories.
type partsJSON struct {
	FormatVersion int      `json:"format_version"`
	PartNames     []string `json:"part_names"`
}

// partition holds all data for a single day.
//
// Day granularity is what makes retention free: dropping a day is
// os.RemoveAll on a directory, not a scan-and-delete. Feel the difference
// when you implement MustDropPartitionsBefore in storage.go -- it should be
// about four lines.
type partition struct {
	path string
	day  int64

	// Concurrency requirement: queries must be able to run while a merge is
	// in progress. Since parts are immutable, the only thing needing
	// synchronisation is the parts LIST. Reference-count open parts so a
	// merge cannot delete a part directory a running query is still reading
	// from -- on Linux the open fd would survive the unlink, but on Windows
	// it would not, and relying on that is how you get a bug that only
	// reproduces on someone else's machine.
	inmemRows  []Row
	parts      []*part
	nextPartId int64
}

// mustCreatePartition creates a new empty partition directory.
func mustCreatePartition(path string, day int64) {
	err := os.MkdirAll(filepath.Join(path, partsDirname), 0o755)
	if err != nil {
		panic(err)
	}
	mustWritePartsJSON(path, nil)
}

// mustOpenPartition opens an existing partition and RECOVERS FROM CRASHES.
//
// Recovery algorithm -- this is the heart of stage 3:
//
//  1. read parts.json. If it is missing or unparseable, that is a fatal
//     error, not something to paper over. (Which is why step 5 below
//     writes it atomically: it must never be observed half-written.)
//  2. for every name in parts.json, the part directory MUST exist and be
//     valid. If one is missing, data is genuinely lost -- panic.
//  3. for every directory under parts/ NOT named in parts.json: it is
//     garbage from a crashed flush or a crashed merge. Delete it.
//  4. delete any leftover "*.tmp" directories.
//  5. fsync the parts directory.
//
// Step 3 is why merges must publish the new part BEFORE deleting the old
// ones: if you crash between publish and delete, the old parts are garbage
// and get cleaned up here. If you deleted first and crashed, you would lose
// data and step 2 would panic.
//
// The crash test kills the process at a random microsecond 200 times and
// asserts this function recovers every time with no data loss. Write it
// carefully -- this is the part of the exercise that turns a toy into a
// storage engine.
func mustOpenPartition(path string, day int64) *partition {
	var f *os.File
	var err error
	var data []byte
	f, err = os.Open(filepath.Join(path, partsFilename))
	if err != nil {
		if os.IsNotExist(err) {
			os.RemoveAll(path)
			mustCreatePartition(path, day)
			return &partition{
				path:       path,
				day:        day,
				parts:      nil,
				nextPartId: 0,
			}
		} else {
			panic(err)
		}
	}
	data, err = io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	f.Close()
	p := &partsJSON{}
	err = json.Unmarshal(data, p)
	if err != nil {
		panic(err)
	}

	var parts []*part

	maxId := -1

	for _, name := range p.PartNames {
		part := mustOpenPart(filepath.Join(path, partsDirname, name))
		parts = append(parts, part)

		n, err := strconv.Atoi(strings.TrimPrefix(name, "part-"))
		if err != nil {
			panic(err)
		}
		if n > maxId {
			maxId = n
		}
	}

	return &partition{
		path:       path,
		day:        day,
		parts:      parts,
		nextPartId: int64(maxId) + 1,
	}
}

func (pt *partition) MustClose() {
	for _, part := range pt.parts {
		part.MustClose()
	}
}

// mustAddRows adds rows to the in-memory part, flushing if it is full.
//
// All rows must belong to this partition's day; the caller splits by day.
func (pt *partition) mustAddRows(rows []Row) {
	if len(pt.inmemRows)+len(rows) > InmemoryPartMaxRows {
		pt.mustFlushInmemoryPart()
	}
	pt.inmemRows = append(pt.inmemRows, rows...)
}

// mustFlushInmemoryPart seals the in-memory part and writes it to disk.
//
// Publication order (the same shape as the merge case):
//
//  1. write the new part to parts/<name>.tmp
//  2. fsync it, rename to parts/<name>, fsync parts dir
//  3. rewrite parts.json (temp + fsync + rename + fsync dir) including <name>
//  4. only now expose the new part to queries and drop the in-memory one
//
// Note what a crash between 2 and 3 leaves behind: a complete, valid, but
// unreferenced part directory. Recovery deletes it, and the rows in it are
// lost -- which is correct, because they were never acknowledged as durable.
// If you want them not to be lost you need a write-ahead log, which this
// exercise deliberately does not have. Write a note here about what a WAL
// would change and what it would cost.
func (pt *partition) mustFlushInmemoryPart() {
	if len(pt.inmemRows) == 0 {
		return
	}

	SortRows(pt.inmemRows)
	var blocks []block
	i := 0
	for i < len(pt.inmemRows) {
		j := i + 1
		for j < len(pt.inmemRows) && pt.inmemRows[i].StreamID == pt.inmemRows[j].StreamID && j-i < MaxRowsPerBlock {
			j++
		}
		var b block
		b.mustInitFromRows(pt.inmemRows[i:j])
		blocks = append(blocks, b)
		i = j
	}
	name := fmt.Sprintf("part-%06d", pt.nextPartId)
	pt.nextPartId++

	pw := mustCreatePartWriter(filepath.Join(pt.path, partsDirname, name))

	for _, block := range blocks {
		pw.MustWriteBlock(&block)
	}
	pw.MustClose()

	part := mustOpenPart(filepath.Join(pt.path, partsDirname, name))
	pt.parts = append(pt.parts, part)
	Metrics.BytesLiveOnDisk.Add(int64(part.md.SizeBytes))

	var names []string
	for _, part := range pt.parts {
		names = append(names, filepath.Base(part.path))
	}

	mustWritePartsJSON(pt.path, names)
	pt.mustMergePartsInBackground()
	pt.inmemRows = pt.inmemRows[:0]
	Metrics.PartsCreated.Add(1)
}

// mustMergePartsInBackground runs one merge cycle if partsToMerge says so.
//
// Publication order:
//
//  1. merge into parts/<new>.tmp, rename to parts/<new>
//  2. rewrite parts.json: add <new>, remove the sources -- ONE atomic write
//  3. only then delete the source part directories
//
// Step 2 must be a single atomic rewrite. If you add the new part in one
// write and remove the old ones in another, a crash in between leaves both
// live and every merged row is duplicated in query results. The correctness
// harness will catch this; the crash test will catch it faster.
func (pt *partition) mustMergePartsInBackground() {
	parts := partsToMerge(pt.parts)
	if parts == nil {
		return
	}
	newPath := filepath.Join(pt.path, partsDirname, fmt.Sprintf("part-%06d", pt.nextPartId))
	pt.nextPartId++
	partmd := mustMergeParts(newPath, parts)
	newPart := mustOpenPart(newPath)
	oldParts := parts
	pt.parts = []*part{newPart}
	Metrics.BytesLiveOnDisk.Add(int64(partmd.SizeBytes))

	var names []string
	for _, newPart = range pt.parts {
		names = append(names, filepath.Base(newPart.path))
	}
	mustWritePartsJSON(pt.path, names)
	for _, p := range oldParts {
		Metrics.BytesLiveOnDisk.Add(-int64(p.md.SizeBytes))
		p.MustClose()
		os.RemoveAll(p.path)
	}
	Metrics.PartsCreated.Add(1)
	Metrics.MergesCount.Add(1)
	Metrics.PartsDeleted.Add(int64(len(oldParts)))
	Metrics.RowsMerged.Add(int64(partmd.RowsCount))
}

// search returns rows in this partition matching q.
func (pt *partition) search(q *Query, ss *SearchStats) []Row {
	var rows []Row
	for _, part := range pt.parts {
		ss.PartsTotal++
		ss.PartsOpened++
		for _, bh := range part.searchBlockHeaders(q, ss) {
			rows = append(rows, part.mustReadBlockRows(bh, q, ss)...)
		}
	}
	return rows
}

// mustWritePartsJSON atomically replaces parts.json.
//
// Write ALL parts.json updates through this one function. The single most
// common crash-safety bug in this exercise is a code path that writes the
// manifest directly and forgets the fsync or the rename.
func mustWritePartsJSON(dir string, names []string) {
	var f *os.File
	var err error
	var pBytes []byte
	var n int
	f, err = os.Create(filepath.Join(dir, partsFilename+".tmp"))
	if err != nil {
		panic(err)
	}
	p := &partsJSON{
		FormatVersion: PartFormatVersion,
		PartNames:     names,
	}
	pBytes, err = json.Marshal(p)
	if err != nil {
		panic(err)
	}

	n, err = f.Write(pBytes)
	if err != nil {
		panic(err)
	}
	err = f.Sync()
	if err != nil {
		panic(err)
	}
	if n != len(pBytes) {
		panic("short write")
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}

	err = os.Rename(filepath.Join(dir, partsFilename+".tmp"), filepath.Join(dir, partsFilename))
	if err != nil {
		panic(err)
	}

	f, err = os.Open(dir)
	if err != nil {
		panic(err)
	}

	err = f.Sync()
	if err != nil {
		panic(err)
	}

	err = f.Close()
	if err != nil {
		panic(err)
	}
}

// dayForTimestamp returns the partition day for a unix-nanosecond timestamp.
func dayForTimestamp(timestamp int64) int64 {
	return timestamp / nsecsPerDay
}
