// Package integration tests storage nodes, routers and agents together over real HTTP.
package integration

import (
	"fmt"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/storage"
)

func newStorage(t *testing.T, cfg *storage.Config) *storage.Storage {
	t.Helper()
	if cfg == nil {
		cfg = &storage.Config{ShardsCount: 1}
	}
	storage.ResetMetrics()
	s := storage.MustOpenStorage(t.TempDir(), cfg)
	t.Cleanup(s.MustClose)
	return s
}

func ingest(t *testing.T, s storage.LogStorage, ds *gen.Dataset, batchSize int) {
	t.Helper()
	rows := ds.Rows
	for len(rows) > 0 {
		n := batchSize
		if n > len(rows) {
			n = len(rows)
		}
		s.MustAddRows(rows[:n])
		rows = rows[n:]
	}
	s.MustForceFlush()
}

func bruteForce(rows []storage.Row, q *storage.Query) []storage.Row {
	var out []storage.Row
	for i := range rows {
		if q.MatchesRow(&rows[i]) {
			out = append(out, rows[i])
		}
	}
	storage.SortRows(out)
	return out
}

func rowsEqual(a, b []storage.Row) (bool, string) {
	if len(a) != len(b) {
		return false, fmt.Sprintf("length mismatch: got %d, want %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Timestamp != b[i].Timestamp {
			return false, fmt.Sprintf("row %d: timestamp %d != %d", i, a[i].Timestamp, b[i].Timestamp)
		}
		if a[i].StreamID != b[i].StreamID {
			return false, fmt.Sprintf("row %d: streamID %d != %d", i, a[i].StreamID, b[i].StreamID)
		}
		if len(a[i].Fields) != len(b[i].Fields) {
			return false, fmt.Sprintf("row %d: field count %d != %d", i, len(a[i].Fields), len(b[i].Fields))
		}
		for j := range a[i].Fields {
			if a[i].Fields[j] != b[i].Fields[j] {
				return false, fmt.Sprintf("row %d field %d: %v != %v", i, j, a[i].Fields[j], b[i].Fields[j])
			}
		}
	}
	return true, ""
}
