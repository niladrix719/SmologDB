package storage_test

import (
	"math"
	"testing"

	"github.com/niladrix719/smologdb/storage"
)

func TestUnflushedRowsAreSearchable(t *testing.T) {
	s := newStorage(t, &storage.Config{ShardsCount: 1})
	s.MustAddRows([]storage.Row{
		testRow(1, 1e9, "_msg", "hello world"),
		testRow(1, 2e9, "_msg", "goodbye world"),
	})

	hello := &storage.Query{MaxTimestamp: math.MaxInt64, Filters: []storage.Filter{{Column: "_msg", Token: "hello"}}}
	if got, _ := s.Search(hello); len(got) != 1 {
		t.Fatalf("found %d rows for 'hello' before any flush, want 1", len(got))
	}
	early := &storage.Query{MaxTimestamp: 1e9}
	if got, _ := s.Search(early); len(got) != 1 {
		t.Fatalf("time range over in-memory rows returned %d rows, want 1", len(got))
	}

	s.MustForceFlush()
	all := &storage.Query{MaxTimestamp: math.MaxInt64}
	if got, _ := s.Search(all); len(got) != 2 {
		t.Fatalf("after the flush found %d rows, want 2 (each row once, not from memory and disk both)", len(got))
	}
}
