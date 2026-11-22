package storage_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/niladrix719/smologdb/storage"
)

func rowsFrom(start, n int) []storage.Row {
	rows := make([]storage.Row, n)
	for i := range rows {
		ts := int64(start+i+1) * 1e6
		rows[i] = testRow(uint64((start+i)%5+1), ts, "_msg", fmt.Sprintf("line %d", start+i))
	}
	return rows
}

func TestMergeLeavesBigPartAlone(t *testing.T) {
	s := newStorage(t, nil)
	s.MustAddRows(rowsFrom(0, 50_000))
	s.MustForceFlush()

	before := storage.Metrics.RowsMerged.Load()
	for i := 0; i < 64; i++ {
		s.MustAddRows(rowsFrom(50_000+i*10, 10))
		s.MustForceFlush()
	}
	merged := storage.Metrics.RowsMerged.Load() - before
	if merged >= 50_000 {
		t.Errorf("merging 64 flushes of 10 rows rewrote %d rows; the 50,000-row part should be left alone", merged)
	}

	got, _ := s.Search(&storage.Query{MaxTimestamp: math.MaxInt64})
	if len(got) != 50_640 {
		t.Fatalf("found %d rows after merging, want 50640: a part that wasn't merged was dropped", len(got))
	}
}
