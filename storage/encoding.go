package storage

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"strconv"
)

type ValueType byte

const (
	ValueTypeString = ValueType(1)

	ValueTypeDict = ValueType(2)

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

type Dict struct {
	Values []string
}

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

func MarshalString(dst []byte, s string) []byte {
	dst = MarshalVarUint64(dst, uint64(len(s)))
	dst = append(dst, s...)
	return dst
}

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
