package minilog

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"strconv"
)

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage1 -v
//
// Reference: lib/logstorage/values_encoder.go and encoding.go in VictoriaLogs.
// ---------------------------------------------------------------------------

// ValueType is the encoding chosen for one column within one block.
//
// The key idea: the type is per (column, block), not per column. The same
// field can be dict-encoded in one block and string-encoded in the next,
// because the encoder looks only at the 8192 values in front of it. That is
// how a schema-free store still gets typed-column compression.
type ValueType byte

const (
	// ValueTypeString stores values as length-prefixed raw bytes.
	// The fallback when nothing better applies.
	ValueTypeString = ValueType(1)

	// ValueTypeDict stores an index into a per-block dictionary.
	//
	// Applies when the block has <= MaxDictLen distinct values totalling
	// <= MaxDictSizeBytes. One byte per row instead of the full string.
	//
	// This is the single highest-leverage encoding for logs: `level`,
	// `service`, `status`, `method` are all low cardinality.
	ValueTypeDict = ValueType(2)

	// ValueTypeUint64 stores values that all parse as unsigned integers.
	//
	// Extension worth doing once the basics work: split this into
	// Uint8/Uint16/Uint32/Uint64 by magnitude, as VictoriaLogs does, and
	// measure what it buys on a `status` or `bytes` column.
	ValueTypeUint64 = ValueType(3)
)

func (vt ValueType) String() string {
	switch vt {
	case ValueTypeString:
		return "string"
	case ValueTypeDict:
		return "dict"
	case ValueTypeUint64:
		return "uint64"
	default:
		return "unknown"
	}
}

// Dict is a per-block value dictionary for ValueTypeDict.
type Dict struct {
	Values []string
}

// EncodeValues encodes values into dst and reports which encoding it chose.
//
// It returns the extended dst, the chosen ValueType, and a non-nil *Dict iff
// the chosen type is ValueTypeDict.
//
// Order of attempts matters -- try the cheapest representation first:
//  1. dict, if distinct count <= MaxDictLen && total key bytes <= MaxDictSizeBytes
//  2. uint64, if every value parses
//  3. string
//
// A trap worth avoiding: do not choose uint64 for values with leading zeros
// ("007"), because you cannot round-trip them. The stage 1 harness feeds you
// exactly this case and asserts byte-identical round-trip, so a shortcut here
// fails loudly rather than silently corrupting data.
func EncodeValues(dst []byte, values []string) ([]byte, ValueType, *Dict) {
	index := make(map[string]int)
	seen := make(map[string]bool)
	nextIndex := 0
	var dictValues []string
	totalBytes := 0

	for _, val := range values {
		if seen[val] != true {
			index[val] = nextIndex
			nextIndex++
			seen[val] = true
			dictValues = append(dictValues, val)
			totalBytes += len(val)
		}
	}

	if len(dictValues) <= MaxDictLen && len(dictValues) < len(values) && totalBytes <= MaxDictSizeBytes {
		dict := &Dict{Values: dictValues}

		for _, v := range values {
			dst = MarshalVarUint64(dst, uint64(index[v]))
		}

		return dst, ValueTypeDict, dict
	}

	allUint := true
	uintVals := make([]uint64, 0, len(values))

	for _, v := range values {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || strconv.FormatUint(n, 10) != v {
			allUint = false
			break
		}
		uintVals = append(uintVals, n)
	}

	if allUint {
		for _, v := range uintVals {
			dst = MarshalVarUint64(dst, v)
		}

		return dst, ValueTypeUint64, nil
	}

	var strBtyes []byte
	for _, val := range values {
		strBtyes = MarshalString(strBtyes, val)
	}

	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		panic(err)
	}
	if _, err := w.Write(strBtyes); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	dst = append(dst, buf.Bytes()...)
	return dst, ValueTypeString, nil
}

// DecodeValues decodes count values of type vt from src, appending to dst.
//
// dict must be non-nil iff vt == ValueTypeDict.
//
// Must return an error, not panic, on malformed input -- see the crash test.
func DecodeValues(dst []string, src []byte, vt ValueType, dict *Dict, count int) ([]string, error) {
	switch vt {
	case ValueTypeDict:
		if dict == nil {
			return nil, fmt.Errorf("missing dictionary")
		}
		for range count {
			idx, rest, err := UnmarshalVarUint64(src)
			if err != nil {
				return nil, err
			}
			if int(idx) >= len(dict.Values) {
				return nil, fmt.Errorf("invalid dictionary index")
			}

			dst = append(dst, dict.Values[int(idx)])
			src = rest
		}
		return dst, nil

	case ValueTypeUint64:
		for range count {
			val, rest, err := UnmarshalVarUint64(src)
			if err != nil {
				return nil, err
			}

			dst = append(dst, strconv.FormatUint(val, 10))
			src = rest
		}
		return dst, nil

	case ValueTypeString:
		r := flate.NewReader(bytes.NewReader(src))
		defer r.Close()
		decompressed, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}

		for range count {
			val, rest, err := UnmarshalString(decompressed)
			if err != nil {
				return nil, err
			}

			dst = append(dst, val)
			decompressed = rest
		}
		return dst, nil

	default:
		return nil, fmt.Errorf("unknown value type")
	}
}

// ---------------------------------------------------------------------------
// Primitives. Implement these first -- everything else builds on them.
// ---------------------------------------------------------------------------

// MarshalVarUint64 appends u to dst as an LEB128 varint.
//
// Why varint and not fixed 8 bytes: almost every integer you write here
// (lengths, offsets, counts, dict indexes) is small. Fixed-width would roughly
// triple the size of your index files. Measure it if you doubt it.
func MarshalVarUint64(dst []byte, u uint64) []byte {
	for {
		bits := byte(u & 0x7F)
		u >>= 7

		if u != 0 {
			bits |= 0x80
		}

		dst = append(dst, bits)

		if u == 0 {
			break
		}
	}
	return dst
}

// UnmarshalVarUint64 reads a varint from src, returning it and the remainder.
//
// On malformed input (truncated, or more than 10 continuation bytes) it must
// return an error rather than looping or panicking.
func UnmarshalVarUint64(src []byte) (uint64, []byte, error) {
	var (
		shift uint
		value uint64
	)
	for i, b := range src {
		if i >= 10 {
			return 0, nil, fmt.Errorf("varint too long")
		}

		value |= uint64(b&0x7F) << shift

		if b&0x80 == 0 {
			return value, src[i+1:], nil
		}

		shift += 7
	}
	return 0, nil, fmt.Errorf("truncated varint")
}

// MarshalString appends a varint-length-prefixed string to dst.
func MarshalString(dst []byte, s string) []byte {
	dst = MarshalVarUint64(dst, uint64(len(s)))
	dst = append(dst, s...)
	return dst
}

// UnmarshalString reads a length-prefixed string from src.
func UnmarshalString(src []byte) (string, []byte, error) {
	strLen, rest, err := UnmarshalVarUint64(src)
	if err != nil {
		return "", nil, err
	}

	if uint64(len(rest)) < strLen {
		return "", nil, fmt.Errorf("truncated string")
	}

	value := string(rest[:strLen])

	return value, rest[strLen:], nil
}

// MarshalTimestamps appends timestamps to dst using delta encoding.
//
// Timestamps within a block are sorted ascending (blocks are built from rows
// sorted by (StreamID, Timestamp)), so deltas are small and non-negative.
//
// Write the first timestamp in full, then varint deltas. On a block of log
// lines a few milliseconds apart this collapses 8 bytes per row to 2-3.
//
// Measure the before/after in the stage 1 harness output. It is the clearest
// demonstration in the whole exercise of why sort order is a storage decision
// and not just a query convenience.
func MarshalTimestamps(dst []byte, timestamps []int64) []byte {
	if len(timestamps) == 0 {
		return dst
	}
	dst = MarshalVarUint64(dst, uint64(timestamps[0]))
	for i := range len(timestamps) - 1 {
		dst = MarshalVarUint64(dst, uint64((timestamps[i+1] - timestamps[i])))
	}
	return dst
}

// UnmarshalTimestamps decodes count timestamps from src, appending to dst.
func UnmarshalTimestamps(dst []int64, src []byte, count int) ([]int64, error) {
	if count == 0 {
		return dst, nil
	}

	first, rest, err := UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}

	dst = append(dst, int64(first))

	for i := 0; i < count-1; i++ {
		val, r, e := UnmarshalVarUint64(rest)
		if e != nil {
			return nil, e
		}

		next := dst[len(dst)-1] + int64(val)
		dst = append(dst, next)
		rest = r
	}

	return dst, nil
}
