package minilog

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run 'TestStage1|TestStage2' -v
//
// Reference: lib/logstorage/part.go, block_search.go, block_stream_reader.go.
// ---------------------------------------------------------------------------

// part is an open, immutable part directory.
//
// Concurrency contract: a *part is read-only after mustOpenPart returns and
// MUST be safe for concurrent use by many goroutines. Do not stash per-query
// scratch buffers on it. (The stage 4 sharding harness runs queries in
// parallel and `go test -race` will find you if you get this wrong.)
type part struct {
	path string
	md   *partMetadata

	// metaindex is read fully into memory at open. It is small by design.
	metaindex []indexBlockHeader

	// Keep index.bin / timestamps.bin / values.bin / bloom.bin as open file
	// handles and read ranges on demand. Do NOT read them fully into memory:
	// the entire exercise is about proving you can avoid reading most of them.
	//
	// Reading via os.File.ReadAt is the straightforward approach and is what
	// you should start with. mmap is a tempting optimisation -- resist it
	// until stage 5, then try it and measure. On a cold page cache the result
	// often surprises people.
	indexFile      *os.File
	timestampsFile *os.File
	valuesFile     *os.File
	bloomFile      *os.File
}

func (ibh *indexBlockHeader) unmarshal(dst []byte) ([]byte, error) {
	var u64 uint64
	var err error

	u64, dst, err = UnmarshalVarUint64(dst)
	if err != nil {
		return nil, err
	}
	ibh.minStreamID = u64
	u64, dst, err = UnmarshalVarUint64(dst)
	if err != nil {
		return nil, err
	}
	ibh.maxStreamID = u64
	u64, dst, err = UnmarshalVarUint64(dst)
	if err != nil {
		return nil, err
	}
	ibh.minTimestamp = int64(u64)
	u64, dst, err = UnmarshalVarUint64(dst)
	if err != nil {
		return nil, err
	}
	ibh.maxTimestamp = int64(u64)
	u64, dst, err = UnmarshalVarUint64(dst)
	if err != nil {
		return nil, err
	}
	ibh.indexOffset = u64
	u64, dst, err = UnmarshalVarUint64(dst)
	if err != nil {
		return nil, err
	}
	ibh.indexSize = u64
	u64, dst, err = UnmarshalVarUint64(dst)
	if err != nil {
		return nil, err
	}
	ibh.blocksCount = uint32(u64)

	return dst, nil
}

// mustOpenPart opens an existing part directory.
//
// Must reject, loudly, any part that is not complete and well-formed:
//   - missing or unparseable metadata.json
//   - FormatVersion != PartFormatVersion
//   - metaindex that does not decode cleanly
//
// "Loudly" means panic (the must* convention), not a silent skip. A part that
// exists on disk but is not in parts.json is garbage from a crashed write and
// should be deleted by the recovery path in partition.go, never opened here.
func mustOpenPart(path string) *part {
	var f *os.File
	var err error
	var data []byte
	var metaindex []indexBlockHeader
	f, err = os.Open(filepath.Join(path, metadataFilename))
	if err != nil {
		panic(err)
	}
	data, err = io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	f.Close()

	partMdata := &partMetadata{}
	err = json.Unmarshal(data, partMdata)
	if err != nil {
		panic(err)
	}
	if partMdata.FormatVersion != PartFormatVersion {
		panic(fmt.Sprintf("unsupported format version %d", partMdata.FormatVersion))
	}

	f, err = os.Open(filepath.Join(path, metaindexFilename))
	if err != nil {
		panic(err)
	}
	data, err = io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	f.Close()

	remaining := data
	for len(remaining) > 0 {
		var ibh indexBlockHeader
		remaining, err = ibh.unmarshal(remaining)
		if err != nil {
			panic(err)
		}
		metaindex = append(metaindex, ibh)
	}

	var timestampsfile *os.File
	var indexfile *os.File
	var valuesfile *os.File
	var bloomfile *os.File

	timestampsfile, err = os.Open(filepath.Join(path, timestampsFilename))
	if err != nil {
		panic(err)
	}
	indexfile, err = os.Open(filepath.Join(path, indexFilename))
	if err != nil {
		panic(err)
	}
	valuesfile, err = os.Open(filepath.Join(path, valuesFilename))
	if err != nil {
		panic(err)
	}
	bloomfile, err = os.Open(filepath.Join(path, bloomFilename))
	if err != nil {
		panic(err)
	}

	return &part{
		path:           path,
		md:             partMdata,
		metaindex:      metaindex,
		timestampsFile: timestampsfile,
		indexFile:      indexfile,
		valuesFile:     valuesfile,
		bloomFile:      bloomfile,
	}
}

func (p *part) MustClose() {
	p.timestampsFile.Close()
	p.indexFile.Close()
	p.valuesFile.Close()
	p.bloomFile.Close()
}

// searchBlockHeaders returns the headers of blocks in p that MIGHT match q.
//
// This is the hot path and the place where all the skipping happens. Do the
// cheap checks first -- each level should eliminate work for the next:
//
//  1. part metadata time range     -> skip the whole part
//  2. metaindex entry ranges       -> skip whole runs of block headers
//  3. blockHeader time range       -> ss.BlocksSkippedByTime++
//  4. blockHeader streamID         -> if q filters on stream
//  5. per-column bloom filters     -> ss.BlocksSkippedByBloom++     [stage 2]
//     ...for a dict column you can check the dict exactly instead
//
// Only blocks that survive all five reach the caller. Bump ss.BloomMaybe once
// per surviving block.
//
// Getting the counters right matters as much as the logic: stage 2's headline
// measurement is skipped/total, and stage 2's FP measurement is
// BloomMaybe - BloomTruePositive. If you forget to bump one, the harness
// reports a number that looks plausible and is wrong.
func (p *part) searchBlockHeaders(q *Query, ss *SearchStats) []*blockHeader {
	var blockHeaders []*blockHeader
	if q.MaxTimestamp < p.md.MinTimestamp || q.MinTimestamp > p.md.MaxTimestamp {
		return nil
	}
	for _, inbh := range p.metaindex {
		if q.MaxTimestamp < inbh.minTimestamp || q.MinTimestamp > inbh.maxTimestamp {
			continue
		}
		if q.StreamID != nil && (*q.StreamID < inbh.minStreamID || *q.StreamID > inbh.maxStreamID) {
			continue
		}
		inbhBuf := make([]byte, inbh.indexSize)
		_, err := p.indexFile.ReadAt(inbhBuf, int64(inbh.indexOffset))
		if err != nil {
			panic(err)
		}
		for len(inbhBuf) > 0 {
			bh := &blockHeader{}
			inbhBuf, err = bh.unmarshal(inbhBuf)
			if err != nil {
				panic(err)
			}
			ss.BlocksTotal++
			if q.MaxTimestamp < bh.minTimestamp || q.MinTimestamp > bh.maxTimestamp {
				ss.BlocksSkippedByTime++
				continue
			}
			if q.StreamID != nil && *q.StreamID != bh.streamID {
				continue
			}
			matches := true
			for _, f := range q.Filters {
				var ch *columnHeader
				var fi *Field
				for _, colh := range bh.columns {
					if colh.name == f.Column {
						ch = &colh
						break
					}
				}
				for _, ccolh := range bh.constColumns {
					if ccolh.Name == f.Column {
						fi = &ccolh
					}
				}
				if fi != nil {
					if !f.Matches(fi.Value) {
						matches = false
						break
					}
				} else if ch != nil {
					bloomBuf := make([]byte, ch.bloomSize)
					_, err := p.bloomFile.ReadAt(bloomBuf, int64(ch.bloomOffset))
					if err != nil {
						panic(err)
					}
					bl := &Bloom{}
					err = bl.Unmarshal(bloomBuf)
					if err != nil {
						panic(err)
					}
					if !bl.Contains(f.Token) {
						matches = false
						break
					}
				} else {
					matches = false
					break
				}
			}
			if !matches {
				ss.BlocksSkippedByBloom++
				continue
			}
			ss.BloomMaybe++
			blockHeaders = append(blockHeaders, bh)
		}

	}
	return blockHeaders
}

// mustReadBlockRows decodes a block and returns the rows matching q.
//
// This is where a bloom "maybe" is resolved into a yes or a no, so it is where
// you must bump ss.BloomTruePositive -- once per block in which the queried
// token really was present. That counter minus nothing is what makes the
// false-positive measurement possible, so be precise: it counts BLOCKS, not
// rows, and it must be bumped once per block regardless of how many rows
// matched.
//
// Only decode the columns q actually needs. Decoding all of them is the
// single most common way people accidentally make a column store perform like
// a row store; the stage 2 harness reports BytesReadFromDisk partly so you
// can catch yourself doing it.
func (p *part) mustReadBlockRows(bh *blockHeader, q *Query, ss *SearchStats) []Row {
	var ts []int64
	var vals []string
	var cols []column
	var allRows []Row
	var rows []Row
	tsBuf := make([]byte, bh.timestampsSize)
	_, err := p.timestampsFile.ReadAt(tsBuf, int64(bh.timestampsOffset))
	if err != nil {
		panic(err)
	}
	ts, err = UnmarshalTimestamps(nil, tsBuf, int(bh.rowsCount))
	if err != nil {
		panic(err)
	}
	for _, colh := range bh.columns {
		valuesBuf := make([]byte, colh.valuesSize)
		_, err = p.valuesFile.ReadAt(valuesBuf, int64(colh.valuesOffset))
		if err != nil {
			panic(err)
		}
		vals, err = DecodeValues(nil, valuesBuf, colh.valueType, colh.dict, int(bh.rowsCount))
		if err != nil {
			panic(err)
		}
		col := column{
			name:   colh.name,
			values: vals,
		}
		cols = append(cols, col)
	}
	b := block{
		streamID:     bh.streamID,
		timestamps:   ts,
		columns:      cols,
		constColumns: bh.constColumns,
	}
	allRows = b.appendRows(allRows)
	for _, row := range allRows {
		if q.MatchesRow(&row) {
			rows = append(rows, row)
		}
	}
	if len(rows) > 0 {
		ss.BloomTruePositive++
	}
	return rows
}

// mustReadAllRows returns every row in the part, in (streamID, timestamp)
// order.
//
// Used by the merger and by the stage 3 correctness harness. Keep it simple
// and obviously correct -- it is the oracle that other things are checked
// against, so it must not share clever code with the query path.
func (p *part) mustReadAllRows() []Row {
	var err error
	var data []byte
	var blockHeaders []blockHeader
	var allRows []Row
	_, err = p.indexFile.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}
	data, err = io.ReadAll(p.indexFile)
	if err != nil {
		panic(err)
	}
	remaining := data
	for len(remaining) > 0 {
		var bh blockHeader
		remaining, err = bh.unmarshal(remaining)
		if err != nil {
			panic(err)
		}
		blockHeaders = append(blockHeaders, bh)
	}

	for _, bh := range blockHeaders {
		tsBuf := make([]byte, bh.timestampsSize)
		_, err = p.timestampsFile.ReadAt(tsBuf, int64(bh.timestampsOffset))
		if err != nil {
			panic(err)
		}
		var timestamps []int64
		timestamps, err = UnmarshalTimestamps(nil, tsBuf, int(bh.rowsCount))
		if err != nil {
			panic(err)
		}

		var columns []column
		for _, colh := range bh.columns {
			valsBuf := make([]byte, colh.valuesSize)
			_, err = p.valuesFile.ReadAt(valsBuf, int64(colh.valuesOffset))
			if err != nil {
				panic(err)
			}
			var vals []string
			vals, err = DecodeValues(nil, valsBuf, colh.valueType, colh.dict, int(bh.rowsCount))
			if err != nil {
				panic(err)
			}
			columns = append(columns, column{
				name:   colh.name,
				values: vals,
			})
		}

		b := block{
			streamID:     bh.streamID,
			timestamps:   timestamps,
			columns:      columns,
			constColumns: bh.constColumns,
		}

		allRows = b.appendRows(allRows)
	}
	return allRows
}
