package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

const (
	metadataFilename   = "metadata.json"
	metaindexFilename  = "metaindex.bin"
	indexFilename      = "index.bin"
	timestampsFilename = "timestamps.bin"
	valuesFilename     = "values.bin"
	bloomFilename      = "bloom.bin"
)

type partMetadata struct {
	FormatVersion int    `json:"format_version"`
	RowsCount     uint64 `json:"rows_count"`
	BlocksCount   uint64 `json:"blocks_count"`
	MinTimestamp  int64  `json:"min_timestamp"`
	MaxTimestamp  int64  `json:"max_timestamp"`
	SizeBytes     uint64 `json:"size_bytes"`
}

type indexBlockHeader struct {
	minStreamID  uint64
	maxStreamID  uint64
	minTimestamp int64
	maxTimestamp int64

	indexOffset uint64
	indexSize   uint64

	blocksCount uint32
}

func (ibh *indexBlockHeader) marshal(dst []byte) []byte {
	dst = MarshalVarUint64(dst, ibh.minStreamID)
	dst = MarshalVarUint64(dst, ibh.maxStreamID)
	dst = MarshalVarUint64(dst, uint64(ibh.minTimestamp))
	dst = MarshalVarUint64(dst, uint64(ibh.maxTimestamp))
	dst = MarshalVarUint64(dst, ibh.indexOffset)
	dst = MarshalVarUint64(dst, ibh.indexSize)
	dst = MarshalVarUint64(dst, uint64(ibh.blocksCount))

	return dst
}

type fileWriter struct {
	f      *os.File
	bw     *bufio.Writer
	offset uint64
}

func (fw *fileWriter) write(data []byte) uint64 {
	start := fw.offset
	n, err := fw.bw.Write(data)
	if err != nil {
		panic(err)
	}
	if n != len(data) {
		panic("short write")
	}
	fw.offset += uint64(n)
	Metrics.BytesWrittenToDisk.Add(int64(n))
	return start
}

type partWriter struct {
	path    string
	tmpPath string

	metaindex  fileWriter
	index      fileWriter
	timestamps fileWriter
	values     fileWriter
	bloom      fileWriter

	indexBlockData []byte
	curIBH         indexBlockHeader

	md partMetadata
}

func mustCreatePartWriter(path string) *partWriter {
	var err error
	var metaindexFile *os.File
	var indexFile *os.File
	var timestampsFile *os.File
	var valuesFile *os.File
	var bloomFile *os.File

	err = os.MkdirAll(path+".tmp", 0o755)
	if err != nil {
		panic(fmt.Sprintf("failed to create directory: %s", err))
	}
	metaindexFile, err = os.Create(filepath.Join(path+".tmp", metaindexFilename))
	if err != nil {
		panic(fmt.Sprintf("failed to create file: %s", err))
	}
	indexFile, err = os.Create(filepath.Join(path+".tmp", indexFilename))
	if err != nil {
		panic(fmt.Sprintf("failed to create file: %s", err))
	}
	timestampsFile, err = os.Create(filepath.Join(path+".tmp", timestampsFilename))
	if err != nil {
		panic(fmt.Sprintf("failed to create file: %s", err))
	}
	valuesFile, err = os.Create(filepath.Join(path+".tmp", valuesFilename))
	if err != nil {
		panic(fmt.Sprintf("failed to create file: %s", err))
	}
	bloomFile, err = os.Create(filepath.Join(path+".tmp", bloomFilename))
	if err != nil {
		panic(fmt.Sprintf("failed to create file: %s", err))
	}

	return &partWriter{
		path:       path,
		tmpPath:    path + ".tmp",
		metaindex:  fileWriter{f: metaindexFile, bw: bufio.NewWriter(metaindexFile)},
		index:      fileWriter{f: indexFile, bw: bufio.NewWriter(indexFile)},
		timestamps: fileWriter{f: timestampsFile, bw: bufio.NewWriter(timestampsFile)},
		values:     fileWriter{f: valuesFile, bw: bufio.NewWriter(valuesFile)},
		bloom:      fileWriter{f: bloomFile, bw: bufio.NewWriter(bloomFile)},
		md: partMetadata{
			FormatVersion: PartFormatVersion,
			MinTimestamp:  math.MaxInt64,
			MaxTimestamp:  math.MinInt64,
		},
		curIBH: indexBlockHeader{
			minTimestamp: math.MaxInt64,
			maxTimestamp: math.MinInt64,
			minStreamID:  math.MaxUint64,
			maxStreamID:  0,
			blocksCount:  0,
		},
	}
}

func (pw *partWriter) MustWriteBlock(b *block) {
	tsBytes := MarshalTimestamps(nil, b.timestamps)
	tsOffset := pw.timestamps.write(tsBytes)
	colHeaders := make([]columnHeader, 0, len(b.columns))
	for _, col := range b.columns {
		colValueBytes, valtype, dct := EncodeValues(nil, col.values)

		tokens := TokenizeValues(nil, col.values)
		bloom := &Bloom{}
		bloom.Init(tokens)
		bloomBytes := bloom.Marshal(nil)

		bloomOffset := pw.bloom.write(bloomBytes)
		colValueOffset := pw.values.write(colValueBytes)
		ch := columnHeader{
			name:         col.name,
			valueType:    valtype,
			dict:         dct,
			valuesOffset: colValueOffset,
			valuesSize:   uint64(len(colValueBytes)),
			bloomOffset:  bloomOffset,
			bloomSize:    uint64(len(bloomBytes)),
		}
		colHeaders = append(colHeaders, ch)
	}
	bh := blockHeader{
		streamID:         b.streamID,
		minTimestamp:     b.timestamps[0],
		maxTimestamp:     b.timestamps[len(b.timestamps)-1],
		rowsCount:        uint32(len(b.timestamps)),
		timestampsOffset: tsOffset,
		timestampsSize:   uint64(len(tsBytes)),
		columns:          colHeaders,
		constColumns:     b.constColumns,
	}
	pw.indexBlockData = append(pw.indexBlockData, bh.marshal(nil)...)

	if bh.streamID < pw.curIBH.minStreamID {
		pw.curIBH.minStreamID = bh.streamID
	}
	if bh.streamID > pw.curIBH.maxStreamID {
		pw.curIBH.maxStreamID = bh.streamID
	}
	if bh.minTimestamp < pw.curIBH.minTimestamp {
		pw.curIBH.minTimestamp = bh.minTimestamp
	}
	if bh.maxTimestamp > pw.curIBH.maxTimestamp {
		pw.curIBH.maxTimestamp = bh.maxTimestamp
	}
	pw.curIBH.blocksCount++

	pw.md.RowsCount += uint64(bh.rowsCount)
	pw.md.BlocksCount++
	if bh.minTimestamp < pw.md.MinTimestamp {
		pw.md.MinTimestamp = bh.minTimestamp
	}
	if bh.maxTimestamp > pw.md.MaxTimestamp {
		pw.md.MaxTimestamp = bh.maxTimestamp
	}
}

func (pw *partWriter) MustClose() *partMetadata {
	indexOffset := pw.index.write(pw.indexBlockData)
	pw.curIBH.indexOffset = indexOffset
	pw.curIBH.indexSize = uint64(len(pw.indexBlockData))
	pw.metaindex.write(pw.curIBH.marshal(nil))

	err := pw.timestamps.bw.Flush()
	if err != nil {
		panic(err)
	}
	err = pw.timestamps.f.Sync()
	if err != nil {
		panic(err)
	}

	err = pw.values.bw.Flush()
	if err != nil {
		panic(err)
	}
	err = pw.values.f.Sync()
	if err != nil {
		panic(err)
	}

	err = pw.index.bw.Flush()
	if err != nil {
		panic(err)
	}
	err = pw.index.f.Sync()
	if err != nil {
		panic(err)
	}

	err = pw.metaindex.bw.Flush()
	if err != nil {
		panic(err)
	}
	err = pw.metaindex.f.Sync()
	if err != nil {
		panic(err)
	}

	err = pw.bloom.bw.Flush()
	if err != nil {
		panic(err)
	}
	err = pw.bloom.f.Sync()
	if err != nil {
		panic(err)
	}

	pw.md.SizeBytes = pw.timestamps.offset + pw.values.offset + pw.index.offset + pw.metaindex.offset + pw.bloom.offset

	metaBytes, er := json.Marshal(&pw.md)
	if er != nil {
		panic(er)
	}

	var metaJsonTmpfile *os.File
	var n int
	var tmpDir *os.File
	var dir *os.File

	metaJsonTmpfile, err = os.Create(filepath.Join(pw.tmpPath, "metadata.json.tmp"))
	if err != nil {
		panic(fmt.Sprintf("failed to create file: %s", err))
	}
	n, err = metaJsonTmpfile.Write(metaBytes)
	if err != nil {
		panic(fmt.Sprintf("failed to write file: %s", err))
	}
	if n != len(metaBytes) {
		panic("short write")
	}

	err = metaJsonTmpfile.Sync()
	if err != nil {
		panic(fmt.Sprintf("failed to sync file: %s", err))
	}

	err = metaJsonTmpfile.Close()
	if err != nil {
		panic(fmt.Sprintf("failed to close file: %s", err))
	}

	err = os.Rename(filepath.Join(pw.tmpPath, "metadata.json.tmp"), filepath.Join(pw.tmpPath, metadataFilename))
	if err != nil {
		panic(fmt.Sprintf("failed to rename file: %s", err))
	}

	tmpDir, err = os.Open(pw.tmpPath)
	if err != nil {
		panic(fmt.Sprintf("failed to open directory: %s", err))
	}

	err = tmpDir.Sync()
	if err != nil {
		panic(fmt.Sprintf("failed to sync directory: %s", err))
	}

	err = tmpDir.Close()
	if err != nil {
		panic(fmt.Sprintf("failed to close directory: %s", err))
	}

	err = os.Rename(pw.tmpPath, pw.path)
	if err != nil {
		panic(fmt.Sprintf("failed to rename directory: %s", err))
	}

	dir, err = os.Open(filepath.Dir(pw.path))
	if err != nil {
		panic(fmt.Sprintf("failed to open directory: %s", err))
	}

	err = dir.Sync()
	if err != nil {
		panic(fmt.Sprintf("failed to sync directory: %s", err))
	}

	err = dir.Close()
	if err != nil {
		panic(fmt.Sprintf("failed to close directory: %s", err))
	}

	return &pw.md
}
