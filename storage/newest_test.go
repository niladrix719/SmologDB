package storage_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/niladrix719/smologdb/storage"
)

func TestNewestLimit(t *testing.T) {
	s := newStorage(t, &storage.Config{ShardsCount: 2})
	var rows []storage.Row
	for i := 1; i <= 300; i++ {
		rows = append(rows, testRow(uint64(i%3+1), int64(i)*1e9, "_msg", fmt.Sprintf("line %d", i)))
	}
	s.MustAddRows(rows)
	s.MustForceFlush()

	got, _ := s.Search(&storage.Query{MaxTimestamp: math.MaxInt64, Limit: 5, Newest: true})
	if len(got) != 5 {
		t.Fatalf("got %d rows, want 5", len(got))
	}
	for i, r := range got {
		if want := int64(300-i) * 1e9; r.Timestamp != want {
			t.Fatalf("row %d has timestamp %d, want %d: the 5 newest rows across all streams, newest first", i, r.Timestamp, want)
		}
	}

	all, _ := s.Search(&storage.Query{MaxTimestamp: math.MaxInt64, Newest: true})
	if len(all) != 300 {
		t.Fatalf("Newest without a limit returned %d rows, want 300", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].Timestamp > all[i-1].Timestamp {
			t.Fatalf("rows %d and %d are out of order: want newest first", i-1, i)
		}
	}
}
