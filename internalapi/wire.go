// Package internalapi is the binary protocol and HTTP API between routers and storage nodes.
package internalapi

import (
	"fmt"

	"github.com/niladrix719/smologdb/storage"
)

const ProtocolVersion = "v1"

const (
	InsertPath = "/internal/insert"

	SelectPath = "/internal/select/query"

	ForceFlushPath = "/internal/force_flush"

	VersionArg = "version"
)

func MarshalRow(dst []byte, r *storage.Row) []byte {
	dst = storage.MarshalVarUint64(dst, uint64(r.Timestamp))
	dst = storage.MarshalVarUint64(dst, r.StreamID)
	dst = storage.MarshalVarUint64(dst, uint64(len(r.Fields)))
	for _, field := range r.Fields {
		dst = storage.MarshalString(dst, field.Name)
		dst = storage.MarshalString(dst, field.Value)
	}
	return dst
}

func UnmarshalRow(r *storage.Row, src []byte) ([]byte, error) {
	var u uint64
	var n string
	var v string
	var rest []byte
	var err error
	u, rest, err = storage.UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	r.Timestamp = int64(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	r.StreamID = u
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	if u > 999 {
		return nil, fmt.Errorf("field count %d exceeds max", u)
	}
	r.Fields = r.Fields[:0]
	for i := 0; i < int(u); i++ {
		n, rest, err = storage.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		v, rest, err = storage.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		r.Fields = append(r.Fields, storage.Field{Name: n, Value: v})
	}
	return rest, nil
}

func MarshalRowBatch(dst []byte, rows []storage.Row) []byte {
	dst = storage.MarshalVarUint64(dst, uint64(len(rows)))
	for _, row := range rows {
		dst = MarshalRow(dst, &row)
	}
	return dst
}

func UnmarshalRowBatch(dst []storage.Row, src []byte) ([]storage.Row, []byte, error) {
	u, rest, err := storage.UnmarshalVarUint64(src)
	if err != nil {
		return nil, nil, err
	}
	if u > 10_000_000 {
		return nil, nil, fmt.Errorf("row count %d exceeds max", u)
	}
	for i := 0; i < int(u); i++ {
		var row storage.Row
		rest, err = UnmarshalRow(&row, rest)
		if err != nil {
			return nil, nil, err
		}
		dst = append(dst, row)
	}
	return dst, rest, nil
}

func MarshalQuery(dst []byte, q *storage.Query) []byte {
	dst = storage.MarshalVarUint64(dst, uint64(q.MinTimestamp))
	dst = storage.MarshalVarUint64(dst, uint64(q.MaxTimestamp))
	if q.StreamID != nil {
		dst = append(dst, 1)
		dst = storage.MarshalVarUint64(dst, *q.StreamID)
	} else {
		dst = append(dst, 0)
	}
	dst = storage.MarshalVarUint64(dst, uint64(len(q.Filters)))
	for _, filter := range q.Filters {
		dst = storage.MarshalString(dst, filter.Column)
		dst = storage.MarshalString(dst, filter.Token)
	}
	dst = storage.MarshalVarUint64(dst, uint64(q.Limit))
	var partial uint64
	if q.AllowPartialResponse {
		partial = 1
	}
	dst = storage.MarshalVarUint64(dst, partial)
	return dst
}

func UnmarshalQuery(q *storage.Query, src []byte) ([]byte, error) {
	var u uint64
	var stId uint64
	var c string
	var t string
	var rest []byte
	var err error

	u, rest, err = storage.UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	q.MinTimestamp = int64(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	q.MaxTimestamp = int64(u)
	if len(rest) < 1 {
		return nil, fmt.Errorf("truncated query missing stream id flag")
	}
	flag := rest[0]
	rest = rest[1:]
	if flag == 1 {
		stId, rest, err = storage.UnmarshalVarUint64(rest)
		if err != nil {
			return nil, err
		}
		q.StreamID = &stId
	}
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	if u > 999 {
		return nil, fmt.Errorf("filter count %d exceeds max", u)
	}
	q.Filters = q.Filters[:0]
	for i := 0; i < int(u); i++ {
		c, rest, err = storage.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		t, rest, err = storage.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		q.Filters = append(q.Filters, storage.Filter{Column: c, Token: t})
	}
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	q.Limit = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	q.AllowPartialResponse = u == 1
	return rest, nil
}

func MarshalQueryResponse(dst []byte, rows []storage.Row, ss *storage.SearchStats) []byte {
	dst = MarshalRowBatch(dst, rows)
	dst = MarshalSearchStats(dst, ss)
	return dst
}

func UnmarshalQueryResponse(src []byte) ([]storage.Row, *storage.SearchStats, error) {
	var rows []storage.Row
	var rest []byte
	var err error
	ss := storage.SearchStats{}
	rows, rest, err = UnmarshalRowBatch(rows, src)
	if err != nil {
		return nil, nil, err
	}
	_, err = UnmarshalSearchStats(&ss, rest)
	if err != nil {
		return nil, nil, err
	}
	return rows, &ss, nil
}

func MarshalSearchStats(dst []byte, ss *storage.SearchStats) []byte {
	dst = storage.MarshalVarUint64(dst, uint64(ss.PartitionsTotal))
	dst = storage.MarshalVarUint64(dst, uint64(ss.PartitionsOpened))
	dst = storage.MarshalVarUint64(dst, uint64(ss.PartsTotal))
	dst = storage.MarshalVarUint64(dst, uint64(ss.PartsOpened))
	dst = storage.MarshalVarUint64(dst, uint64(ss.BlocksTotal))
	dst = storage.MarshalVarUint64(dst, uint64(ss.BlocksSkippedByTime))
	dst = storage.MarshalVarUint64(dst, uint64(ss.BlocksSkippedByBloom))
	dst = storage.MarshalVarUint64(dst, uint64(ss.BlocksScanned))
	dst = storage.MarshalVarUint64(dst, uint64(ss.BloomMaybe))
	dst = storage.MarshalVarUint64(dst, uint64(ss.BloomTruePositive))
	dst = storage.MarshalVarUint64(dst, uint64(ss.RowsScanned))
	dst = storage.MarshalVarUint64(dst, uint64(ss.RowsMatched))
	dst = storage.MarshalVarUint64(dst, uint64(ss.BytesReadFromDisk))

	return dst
}

func UnmarshalSearchStats(ss *storage.SearchStats, src []byte) ([]byte, error) {
	var u uint64
	var rest []byte
	var err error
	u, rest, err = storage.UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	ss.PartitionsTotal = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.PartitionsOpened = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.PartsTotal = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.PartsOpened = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksTotal = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksSkippedByTime = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksSkippedByBloom = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksScanned = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BloomMaybe = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BloomTruePositive = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.RowsScanned = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.RowsMatched = int(u)
	u, rest, err = storage.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BytesReadFromDisk = int64(u)

	return rest, nil
}
