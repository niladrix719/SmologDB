package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	partsFilename = "parts.json"
	partsDirname  = "parts"
)

type partsJSON struct {
	FormatVersion int      `json:"format_version"`
	PartNames     []string `json:"part_names"`
}

type partition struct {
	path string
	day  int64

	inmemRows  []Row
	parts      []*part
	nextPartId int64
}

func mustCreatePartition(path string, day int64) {
	err := os.MkdirAll(filepath.Join(path, partsDirname), 0o755)
	if err != nil {
		panic(err)
	}
	mustWritePartsJSON(path, nil)
}

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

func (pt *partition) mustAddRows(rows []Row) {
	if len(pt.inmemRows)+len(rows) > InmemoryPartMaxRows {
		pt.mustFlushInmemoryPart()
	}
	pt.inmemRows = append(pt.inmemRows, rows...)
}

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

	// The part becomes visible only once parts.json names it; on open, unnamed parts are deleted.
	mustWritePartsJSON(pt.path, names)
	pt.maybeMergeParts()
	pt.inmemRows = pt.inmemRows[:0]
	Metrics.PartsCreated.Add(1)
}

// maybeMergeParts merges all parts into one once there are enough of them.
func (pt *partition) maybeMergeParts() {
	parts := partsToMerge(pt.parts)
	if parts == nil {
		return
	}
	newPath := filepath.Join(pt.path, partsDirname, fmt.Sprintf("part-%06d", pt.nextPartId))
	pt.nextPartId++
	partmd := mustMergeParts(newPath, parts)
	newPart := mustOpenPart(newPath)
	oldParts := parts
	mergedParts := make(map[*part]struct{}, len(oldParts))
	for _, p := range oldParts {
		mergedParts[p] = struct{}{}
	}
	var remainingParts []*part
	for _, p := range pt.parts {
		if _, ok := mergedParts[p]; !ok {
			remainingParts = append(remainingParts, p)
		}
	}
	pt.parts = append(remainingParts, newPart)
	Metrics.BytesLiveOnDisk.Add(int64(partmd.SizeBytes))

	var names []string
	for _, newPart = range pt.parts {
		names = append(names, filepath.Base(newPart.path))
	}
	// One parts.json write swaps sources for the merged part; sources are deleted only after it.
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

func (pt *partition) search(q *Query, ss *SearchStats) []Row {
	var rows []Row
	for _, row := range pt.inmemRows {
		if q.MatchesRow(&row) {
			rows = append(rows, row)
		}
	}
	for _, part := range pt.parts {
		ss.PartsTotal++
		if q.MaxTimestamp < part.md.MinTimestamp || q.MinTimestamp > part.md.MaxTimestamp {
			continue
		}
		ss.PartsOpened++
		for _, bh := range part.searchBlockHeaders(q, ss) {
			rows = append(rows, part.mustReadBlockRows(bh, q, ss)...)
		}
	}
	return rows
}

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

func dayForTimestamp(timestamp int64) int64 {
	return timestamp / nsecsPerDay
}
