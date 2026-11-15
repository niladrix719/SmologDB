// Package internalapi is the protocol spoken between insert/select nodes and
// storage nodes.
//
// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage6 -v
// Also run:     go test ./harness -run FuzzStage6RowRoundTrip -fuzz . -fuzztime 60s
//
// Reference: lib/logstorage/log_rows.go (InsertRow.Marshal /
// UnmarshalInplace), app/vlstorage/netinsert/netinsert.go (ProtocolVersion).
// ---------------------------------------------------------------------------
//
// Two rules for everything in this file:
//
//  1. Do not reach for JSON. You already wrote the right tools in
//     minilog/encoding.go -- MarshalVarUint64, MarshalString,
//     MarshalTimestamps. Reusing them is not just tidiness: the wire format
//     and the on-disk format having the same shape is what lets a storage
//     node write what it received without a transcoding step.
//
//  2. Unmarshal must never panic on hostile input. It is reading bytes off a
//     socket. Every length prefix is attacker-controlled from the storage
//     node's point of view, and "the peer is a slightly older version of
//     ourselves" is the normal case, not the exceptional one. Return errors.
package internalapi

import (
	"fmt"

	"github.com/niladrix719/minilog/minilog"
)

// ProtocolVersion is the version of the insert/select protocol.
//
// It is sent as a ?version= query arg on every internal request, and the
// storage node rejects a mismatch with a 400 before reading a single byte of
// body. That check is the entire mechanism that makes a rolling upgrade safe:
// a node running v1 code that receives v2 bytes fails loudly instead of
// decoding them as v1 garbage and writing corrupt rows.
//
// Bump it whenever anything in this file changes shape. VictoriaLogs keeps a
// separate version per endpoint (see the const block at the top of
// app/vlstorage/netselect/netselect.go) so that changing the query protocol
// does not force insert clients to upgrade. One version for both is fine at
// this size -- but note the tradeoff you are accepting.
const ProtocolVersion = "v1"

// Endpoint paths.
//
// Names match VictoriaLogs so that when you go read the real handlers in
// app/vlinsert/internalinsert and app/vlselect/internalselect, the routing
// table looks familiar.
const (
	// InsertPath accepts a POSTed row batch. Body: MarshalRowBatch output.
	InsertPath = "/internal/insert"

	// SelectPath accepts a POSTed query. Body: MarshalQuery output.
	// Response body: MarshalQueryResponse output.
	SelectPath = "/internal/select/query"

	// ForceFlushPath makes previously inserted rows queryable. No body.
	//
	// Test-only, and it exists for exactly the reason DebugFlush exists in the
	// real repo: without it every cluster test has to sleep past the
	// background flush interval, and a test suite built on sleeps is a test
	// suite that is flaky on a loaded CI box.
	ForceFlushPath = "/internal/force_flush"

	// VersionArg is the query arg carrying ProtocolVersion.
	VersionArg = "version"
)

// ---------------------------------------------------------------------------
// rows
// ---------------------------------------------------------------------------

// MarshalRow appends the wire encoding of r to dst and returns the result.
//
// The format, which you should write down in this comment once you have
// settled on it:
//
//	varuint(streamID)
//	varuint(zigzag(timestamp))
//	varuint(len(fields))
//	repeated: string(name) string(value)
//
// Two decisions worth pausing on, because they are both forks in the road and
// the harness measures the consequence of each:
//
//   - Timestamps. MarshalVarUint64 of a raw unix-nanosecond int64 costs 9
//     bytes every time, because the high bits are always set. Zigzag first, or
//     delta-encode against the batch's first timestamp the way
//     MarshalTimestamps does against a block. The stage 6 measurement reports
//     your bytes-per-row; if it is not moving when you change this, you are
//     not doing what you think you are.
//
//   - Field names. Every row in a batch carries the same ~11 field names, so a
//     naive encoding ships "trace_id" tens of thousands of times per batch and
//     leans entirely on the transport compressor to fix it. A per-batch name
//     dictionary fixes it in the format instead. Do the simple thing first,
//     measure, then decide -- and record the two numbers, because "just let
//     zstd handle it" is a legitimate answer and you should be able to say by
//     how much.
//
// VictoriaLogs ships StreamTagsCanonical (the labels) rather than a stream id,
// and lets the storage node derive the id. You ship the uint64 id, because
// StreamIDForLabels is already a pure function and the router needs the id
// before it can choose a node anyway. Note what you gave up: the storage node
// now trusts a hash computed by its client, so a client bug becomes a storage
// corruption rather than a client error.
func MarshalRow(dst []byte, r *minilog.Row) []byte {
	dst = minilog.MarshalVarUint64(dst, uint64(r.Timestamp))
	dst = minilog.MarshalVarUint64(dst, r.StreamID)
	dst = minilog.MarshalVarUint64(dst, uint64(len(r.Fields)))
	for _, field := range r.Fields {
		dst = minilog.MarshalString(dst, field.Name)
		dst = minilog.MarshalString(dst, field.Value)
	}
	return dst
}

// UnmarshalRow decodes one row from src into r and returns the remaining tail.
//
// r is reused across calls by the batch decoder, so reset it rather than
// allocating. Fields must come back sorted by Name -- that is a precondition
// of the block writer, and the row was sorted when it was marshaled, so
// preserving order is enough. Do not re-sort defensively: if the order is
// wrong you want to know, not to have it quietly repaired on every row.
func UnmarshalRow(r *minilog.Row, src []byte) ([]byte, error) {
	var u uint64
	var n string
	var v string
	var rest []byte
	var err error
	u, rest, err = minilog.UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	r.Timestamp = int64(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	r.StreamID = u
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	if u > 999 {
		return nil, fmt.Errorf("field count %d exceeds max", u)
	}
	r.Fields = r.Fields[:0]
	for i := 0; i < int(u); i++ {
		n, rest, err = minilog.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		v, rest, err = minilog.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		r.Fields = append(r.Fields, minilog.Field{Name: n, Value: v})
	}
	return rest, nil
}

// MarshalRowBatch appends a length-prefixed batch of rows to dst.
func MarshalRowBatch(dst []byte, rows []minilog.Row) []byte {
	dst = minilog.MarshalVarUint64(dst, uint64(len(rows)))
	for _, row := range rows {
		dst = MarshalRow(dst, &row)
	}
	return dst
}

// UnmarshalRowBatch decodes a batch, appending to dst, and returns the result.
//
// Cap the row count you are willing to allocate for before you allocate: a
// corrupt or hostile length prefix must not turn into a multi-gigabyte make().
func UnmarshalRowBatch(dst []minilog.Row, src []byte) ([]minilog.Row, []byte, error) {
	u, rest, err := minilog.UnmarshalVarUint64(src)
	if err != nil {
		return nil, nil, err
	}
	if u > 10_000_000 {
		return nil, nil, fmt.Errorf("row count %d exceeds max", u)
	}
	for i := 0; i < int(u); i++ {
		var row minilog.Row
		rest, err = UnmarshalRow(&row, rest)
		if err != nil {
			return nil, nil, err
		}
		dst = append(dst, row)
	}
	return dst, rest, nil
}

// ---------------------------------------------------------------------------
// queries
// ---------------------------------------------------------------------------

// MarshalQuery appends the wire encoding of q to dst.
//
// Every field of minilog.Query must survive the round trip, including the ones
// added for the cluster: Limit and AllowPartialResponse. A query field that is
// silently dropped on the wire produces a result that is wrong in exactly the
// way that is hardest to notice -- more rows than asked for, or a partial
// answer reported as complete.
//
// StreamID is a *uint64. Encode presence explicitly (a leading byte), not by
// convention such as "0 means absent" -- 0 is a legal stream id.
func MarshalQuery(dst []byte, q *minilog.Query) []byte {
	dst = minilog.MarshalVarUint64(dst, uint64(q.MinTimestamp))
	dst = minilog.MarshalVarUint64(dst, uint64(q.MaxTimestamp))
	if q.StreamID != nil {
		dst = append(dst, 1)
		dst = minilog.MarshalVarUint64(dst, *q.StreamID)
	} else {
		dst = append(dst, 0)
	}
	dst = minilog.MarshalVarUint64(dst, uint64(len(q.Filters)))
	for _, filter := range q.Filters {
		dst = minilog.MarshalString(dst, filter.Column)
		dst = minilog.MarshalString(dst, filter.Token)
	}
	dst = minilog.MarshalVarUint64(dst, uint64(q.Limit))
	var partial uint64
	if q.AllowPartialResponse {
		partial = 1
	}
	dst = minilog.MarshalVarUint64(dst, partial)
	return dst
}

// UnmarshalQuery decodes a query from src into q and returns the tail.
func UnmarshalQuery(q *minilog.Query, src []byte) ([]byte, error) {
	var u uint64
	var stId uint64
	var c string
	var t string
	var rest []byte
	var err error

	u, rest, err = minilog.UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	q.MinTimestamp = int64(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
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
		stId, rest, err = minilog.UnmarshalVarUint64(rest)
		if err != nil {
			return nil, err
		}
		q.StreamID = &stId
	}
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	if u > 999 {
		return nil, fmt.Errorf("filter count %d exceeds max", u)
	}
	q.Filters = q.Filters[:0]
	for i := 0; i < int(u); i++ {
		c, rest, err = minilog.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		t, rest, err = minilog.UnmarshalString(rest)
		if err != nil {
			return nil, err
		}
		q.Filters = append(q.Filters, minilog.Filter{Column: c, Token: t})
	}
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	q.Limit = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	q.AllowPartialResponse = u == 1
	return rest, nil
}

// ---------------------------------------------------------------------------
// query responses
// ---------------------------------------------------------------------------

// MarshalQueryResponse appends a storage node's answer to dst: the matching
// rows followed by that node's SearchStats.
//
// This is the simple shape: one buffer, sent once. It is enough for stages
// 6-9, and stage 10 is where you get to feel why VictoriaLogs does not do it
// this way. The real protocol streams length-prefixed blocks (see the read
// loop in storageNode.runQuery in app/vlstorage/netselect/netselect.go) so
// that the select node can start merging before the storage node has finished
// scanning, and so that neither side ever holds a whole result set.
//
// When stage 10 plots peak RSS against node count, this function is the reason
// the line slopes.
func MarshalQueryResponse(dst []byte, rows []minilog.Row, ss *minilog.SearchStats) []byte {
	dst = MarshalRowBatch(dst, rows)
	dst = MarshalSearchStats(dst, ss)
	return dst
}

// UnmarshalQueryResponse decodes a storage node's answer.
func UnmarshalQueryResponse(src []byte) ([]minilog.Row, *minilog.SearchStats, error) {
	var rows []minilog.Row
	var rest []byte
	var err error
	ss := minilog.SearchStats{}
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

// MarshalSearchStats appends ss to dst.
//
// Only the fields a storage node can legitimately know about. NodesQueried,
// NodesFailed and BytesReceivedFromNodes are cluster-level and are filled in
// by netselect after the fan-out; a storage node that sets them is describing
// a cluster it is not a member of.
func MarshalSearchStats(dst []byte, ss *minilog.SearchStats) []byte {
	dst = minilog.MarshalVarUint64(dst, uint64(ss.PartitionsTotal))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.PartitionsOpened))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.PartsTotal))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.PartsOpened))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.BlocksTotal))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.BlocksSkippedByTime))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.BlocksSkippedByBloom))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.BlocksScanned))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.BloomMaybe))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.BloomTruePositive))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.RowsScanned))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.RowsMatched))
	dst = minilog.MarshalVarUint64(dst, uint64(ss.BytesReadFromDisk))

	return dst
}

// UnmarshalSearchStats decodes stats from src into ss and returns the tail.
func UnmarshalSearchStats(ss *minilog.SearchStats, src []byte) ([]byte, error) {
	var u uint64
	var rest []byte
	var err error
	u, rest, err = minilog.UnmarshalVarUint64(src)
	if err != nil {
		return nil, err
	}
	ss.PartitionsTotal = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.PartitionsOpened = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.PartsTotal = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.PartsOpened = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksTotal = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksSkippedByTime = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksSkippedByBloom = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BlocksScanned = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BloomMaybe = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BloomTruePositive = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.RowsScanned = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.RowsMatched = int(u)
	u, rest, err = minilog.UnmarshalVarUint64(rest)
	if err != nil {
		return nil, err
	}
	ss.BytesReadFromDisk = int64(u)

	return rest, nil
}
