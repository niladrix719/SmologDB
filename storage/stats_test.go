package storage_test

import (
	"math"
	"testing"

	"github.com/niladrix719/smologdb/storage"
)

func TestSearchStatsCountScannedBlocks(t *testing.T) {
	s := newStorage(t, nil)
	var rows []storage.Row
	for i := 0; i < 3000; i++ {
		msg := "routine request"
		if i%1000 == 0 {
			msg = "rare zqxfault happened"
		}
		rows = append(rows, testRow(uint64(i%3+1), int64(i+1)*1e6, "_msg", msg))
	}
	s.MustAddRows(rows)
	s.MustForceFlush()

	got, ss := s.Search(&storage.Query{
		MaxTimestamp: math.MaxInt64,
		Filters:      []storage.Filter{{Column: "_msg", Token: "zqxfault"}},
	})
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	read := ss.BlocksTotal - ss.BlocksSkippedByTime - ss.BlocksSkippedByBloom
	if ss.BlocksScanned == 0 || ss.BlocksScanned != read {
		t.Errorf("BlocksScanned = %d, want %d (every block that was read)", ss.BlocksScanned, read)
	}
	if ss.RowsMatched != len(got) {
		t.Errorf("RowsMatched = %d, want %d", ss.RowsMatched, len(got))
	}
	if ss.RowsScanned < 3000 {
		t.Errorf("RowsScanned = %d, want every row in the blocks read (3000)", ss.RowsScanned)
	}
	if ss.BytesReadFromDisk == 0 {
		t.Errorf("BytesReadFromDisk = 0, want the bytes the scanned blocks took")
	}
}
