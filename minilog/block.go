package minilog

import "fmt"

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage1 -v
//
// Reference: lib/logstorage/block.go and block_header.go in VictoriaLogs.
// ---------------------------------------------------------------------------

// block is a batch of up to MaxRowsPerBlock rows from a SINGLE stream,
// stored column-wise.
//
// Single-stream blocks are a deliberate constraint: it means every row in the
// block shares a StreamID, so the id is stored once in the header instead of
// once per row, and a stream filter can skip the block without touching any
// column data. VictoriaLogs does the same thing.
type block struct {
	streamID   uint64
	timestamps []int64
	columns    []column

	// constColumns hold fields whose value is identical across every row in
	// the block.
	//
	// Implement this AFTER the basic path works, then re-run the stage 1
	// harness and note the size delta. On realistic k8s-shaped logs a large
	// fraction of columns are constant within a stream's block, so this is
	// usually a bigger win than any encoding choice.
	//
	// Constraint to respect: const values live in the block header, which is
	// read on every block scan. Cap the value size (VictoriaLogs uses 256
	// bytes) or you will slow down scans to save space.
	constColumns []Field
}

// column is one field's values across every row in the block.
type column struct {
	name   string
	values []string
}

// blockHeader is the searchable metadata for one block.
//
// This is what lives in index.bin, and it is the thing that must let you skip
// a block WITHOUT reading values.bin. Anything the query planner needs to
// make a skip decision has to be in here.
//
// Sizing tension to be aware of: every byte you add here is read on every
// query. Every byte you leave out may force a read of values.bin. That
// tradeoff is the entire reason VictoriaLogs has both a columns_header_index
// and a columns_header file.
type blockHeader struct {
	streamID     uint64
	minTimestamp int64
	maxTimestamp int64
	rowsCount    uint32

	// offset/size of this block's timestamps within timestamps.bin
	timestampsOffset uint64
	timestampsSize   uint64

	columns      []columnHeader
	constColumns []Field
}

// columnHeader is per-column metadata within a block.
type columnHeader struct {
	name      string
	valueType ValueType

	// dict is non-nil iff valueType == ValueTypeDict.
	dict *Dict

	// offset/size within values.bin
	valuesOffset uint64
	valuesSize   uint64

	// offset/size within bloom.bin
	//
	// Question to answer for yourself in stage 2: should you write a bloom for
	// a dict-encoded column at all? The dict is already in the header and has
	// at most MaxDictLen entries, so you can answer "is token X present?"
	// exactly, for free, with no false positives. Decide, implement, and note
	// the size saving here.
	bloomOffset uint64
	bloomSize   uint64
}

// mustInitFromRows fills b from rows.
//
// Preconditions the caller guarantees:
//   - all rows have the same StreamID
//   - rows are sorted by Timestamp ascending
//   - len(rows) <= MaxRowsPerBlock
//   - each row's Fields are sorted by Name
//
// Because fields are sorted, building columns is a merge, not a map lookup
// per field. Rows in real logs are ragged (not every row has every field), so
// you must decide what a missing value becomes. Empty string is the simple
// answer; write down here why that is or is not acceptable for your filters.
func (b *block) mustInitFromRows(rows []Row) {
	b.reset()
	b.streamID = rows[0].StreamID
	for _, row := range rows {
		b.timestamps = append(b.timestamps, row.Timestamp)
	}

	cursors := make([]int, len(rows))

	for {
		found := false
		var name string
		for i, row := range rows {
			if cursors[i] >= len(row.Fields) {
				continue
			}
			n := row.Fields[cursors[i]].Name
			if !found || n < name {
				found = true
				name = n
			}
		}
		if !found {
			break
		}

		values := make([]string, len(rows))
		for i, row := range rows {
			if cursors[i] < len(row.Fields) && row.Fields[cursors[i]].Name == name {
				values[i] = row.Fields[cursors[i]].Value
				cursors[i]++
			}
		}

		isConst := true

		for i := 1; i < len(values); i++ {
			if values[i-1] != values[i] {
				isConst = false
				break
			}
		}

		if isConst && values[0] != "" {
			b.constColumns = append(b.constColumns, Field{
				Name:  name,
				Value: values[0],
			})
		} else {
			b.columns = append(b.columns, column{
				name:   name,
				values: values,
			})
		}
	}
}

// appendRows reconstructs rows from b and appends them to dst.
//
// This is the inverse of mustInitFromRows and the stage 1 harness asserts
// exact round-trip through both. Field order in the output must be sorted by
// name, matching the input precondition.
func (b *block) appendRows(dst []Row) []Row {
	for i := range b.timestamps {
		row := Row{
			StreamID:  b.streamID,
			Timestamp: b.timestamps[i],
		}

		cursorCol := 0
		cursorConstCol := 0

		for {
			if cursorCol >= len(b.columns) || cursorConstCol >= len(b.constColumns) {
				break
			}
			if b.columns[cursorCol].name < b.constColumns[cursorConstCol].Name {
				if b.columns[cursorCol].values[i] != "" {
					row.Fields = append(row.Fields, Field{
						Name:  b.columns[cursorCol].name,
						Value: b.columns[cursorCol].values[i],
					})
				}
				cursorCol++
			} else {
				if b.constColumns[cursorConstCol].Value != "" {
					row.Fields = append(row.Fields, Field{
						Name:  b.constColumns[cursorConstCol].Name,
						Value: b.constColumns[cursorConstCol].Value,
					})
				}
				cursorConstCol++
			}
		}
		if cursorCol >= len(b.columns) {
			for idx := cursorConstCol; idx < len(b.constColumns); idx++ {
				if b.constColumns[idx].Value != "" {
					row.Fields = append(row.Fields, Field{
						Name:  b.constColumns[idx].Name,
						Value: b.constColumns[idx].Value,
					})
				}
			}
		} else if cursorConstCol >= len(b.constColumns) {
			for idx := cursorCol; idx < len(b.columns); idx++ {
				if b.columns[idx].values[i] != "" {
					row.Fields = append(row.Fields, Field{
						Name:  b.columns[idx].name,
						Value: b.columns[idx].values[i],
					})
				}
			}
		}

		dst = append(dst, row)
	}
	return dst
}

func (b *block) reset() {
	b.streamID = 0
	b.timestamps = b.timestamps[:0]
	b.columns = b.columns[:0]
	b.constColumns = b.constColumns[:0]
}

// marshalHeader appends bh to dst.
//
// Keep this compact -- it is read in full on every query that touches the
// part. Varints everywhere, no fixed-width fields except where you genuinely
// need random access.
func (bh *blockHeader) marshal(dst []byte) []byte {
	dst = MarshalVarUint64(dst, bh.streamID)
	dst = MarshalVarUint64(dst, uint64(bh.minTimestamp))
	dst = MarshalVarUint64(dst, uint64(bh.maxTimestamp))
	dst = MarshalVarUint64(dst, uint64(bh.rowsCount))
	dst = MarshalVarUint64(dst, bh.timestampsOffset)
	dst = MarshalVarUint64(dst, bh.timestampsSize)
	dst = MarshalVarUint64(dst, uint64(len(bh.columns)))
	for _, col := range bh.columns {
		dst = MarshalString(dst, col.name)
		dst = append(dst, byte(col.valueType))
		if col.valueType == ValueTypeDict {
			dst = MarshalVarUint64(dst, uint64(len(col.dict.Values)))
			for _, val := range col.dict.Values {
				dst = MarshalString(dst, val)
			}
		}
		dst = MarshalVarUint64(dst, col.valuesOffset)
		dst = MarshalVarUint64(dst, col.valuesSize)
		dst = MarshalVarUint64(dst, col.bloomOffset)
		dst = MarshalVarUint64(dst, col.bloomSize)
	}
	dst = MarshalVarUint64(dst, uint64(len(bh.constColumns)))
	for _, col := range bh.constColumns {
		dst = MarshalString(dst, col.Name)
		dst = MarshalString(dst, col.Value)
	}
	return dst
}

// unmarshal parses a block header from src, returning the remainder.
func (bh *blockHeader) unmarshal(src []byte) ([]byte, error) {
	var u64 uint64
	var err error
	var str string
	var b byte

	u64, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.streamID = u64

	u64, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.minTimestamp = int64(u64)

	u64, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.maxTimestamp = int64(u64)

	u64, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.rowsCount = uint32(u64)

	u64, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.timestampsOffset = u64

	u64, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.timestampsSize = u64

	var colLen uint64
	colLen, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.columns = make([]columnHeader, colLen)

	for i := range bh.columns {
		str, src, err = UnmarshalString(src)
		if err != nil {
			return nil, err
		}
		bh.columns[i].name = str

		if len(src) == 0 {
			return nil, fmt.Errorf("truncated column header")
		}
		b = src[0]
		src = src[1:]
		bh.columns[i].valueType = ValueType(b)

		if bh.columns[i].valueType == ValueTypeDict {
			var dicLen uint64
			dicLen, src, err = UnmarshalVarUint64(src)
			if err != nil {
				return nil, err
			}
			bh.columns[i].dict = &Dict{Values: make([]string, dicLen)}
			for valIdx := range bh.columns[i].dict.Values {
				str, src, err = UnmarshalString(src)
				if err != nil {
					return nil, err
				}
				bh.columns[i].dict.Values[valIdx] = str
			}
		}
		u64, src, err = UnmarshalVarUint64(src)
		if err != nil {
			return nil, err
		}
		bh.columns[i].valuesOffset = u64

		u64, src, err = UnmarshalVarUint64(src)
		if err != nil {
			return nil, err
		}
		bh.columns[i].valuesSize = u64

		u64, src, err = UnmarshalVarUint64(src)
		if err != nil {
			return nil, err
		}
		bh.columns[i].bloomOffset = u64

		u64, src, err = UnmarshalVarUint64(src)
		if err != nil {
			return nil, err
		}
		bh.columns[i].bloomSize = u64
	}
	colLen, src, err = UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	bh.constColumns = make([]Field, colLen)
	for idx := range bh.constColumns {
		str, src, err = UnmarshalString(src)
		if err != nil {
			return nil, err
		}
		bh.constColumns[idx].Name = str
		str, src, err = UnmarshalString(src)
		if err != nil {
			return nil, err
		}
		bh.constColumns[idx].Value = str
	}

	return src, nil
}
