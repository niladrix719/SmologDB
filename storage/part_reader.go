package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type part struct {
	path string
	md   *partMetadata

	metaindex []indexBlockHeader

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
					if f.Matches(fi.Value) == f.Not {
						matches = false
						break
					}
				} else if ch != nil {
					if f.Not || f.Kind == MatchPrefix {
						continue
					}
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
					for _, tok := range Tokenize(nil, f.Token) {
						if !bl.Contains(tok) {
							matches = false
							break
						}
					}
				} else if !f.Not {
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

func (p *part) mustReadBlockRows(bh *blockHeader, q *Query, ss *SearchStats) []Row {
	var ts []int64
	var vals []string
	var cols []column
	var allRows []Row
	var rows []Row
	var n int
	var err error
	tsBuf := make([]byte, bh.timestampsSize)
	n, err = p.timestampsFile.ReadAt(tsBuf, int64(bh.timestampsOffset))
	ss.BytesReadFromDisk += int64(n)
	if err != nil {
		panic(err)
	}
	ts, err = UnmarshalTimestamps(nil, tsBuf, int(bh.rowsCount))
	if err != nil {
		panic(err)
	}
	for _, colh := range bh.columns {
		valuesBuf := make([]byte, colh.valuesSize)
		n, err = p.valuesFile.ReadAt(valuesBuf, int64(colh.valuesOffset))
		ss.BytesReadFromDisk += int64(n)
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
	ss.BlocksScanned++
	allRows = b.appendRows(allRows)
	for _, row := range allRows {
		ss.RowsScanned++
		if q.MatchesRow(&row) {
			ss.RowsMatched++
			rows = append(rows, row)
		}
	}
	if len(rows) > 0 {
		ss.BloomTruePositive++
	}
	return rows
}

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
