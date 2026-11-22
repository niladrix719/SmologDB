package storage_test

import (
	"math"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/storage"
)

func TestRetentionDropsOldDataOnOpen(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UnixNano()
	day := int64(24 * time.Hour)

	s := storage.MustOpenStorage(dir, &storage.Config{ShardsCount: 1})
	s.MustAddRows([]storage.Row{testRow(1, now-10*day, "_msg", "old"), testRow(1, now, "_msg", "new")})
	s.MustClose()

	s = storage.MustOpenStorage(dir, &storage.Config{ShardsCount: 1, RetentionDays: 3})
	defer s.MustClose()
	got, _ := s.Search(&storage.Query{MaxTimestamp: math.MaxInt64})
	if m := messages(got); len(got) != 1 || m["new"] != 1 {
		t.Fatalf("after reopening with 3 days of retention found %v, want only the new row", m)
	}
	if days := s.PartitionDays(); len(days) != 1 {
		t.Fatalf("partitions for %d days remain, want 1", len(days))
	}
}

func TestRetentionRejectsOldRows(t *testing.T) {
	s := newStorage(t, &storage.Config{ShardsCount: 1, RetentionDays: 3})
	now := time.Now().UnixNano()
	day := int64(24 * time.Hour)
	s.MustAddRows([]storage.Row{testRow(1, now-10*day, "_msg", "old"), testRow(1, now, "_msg", "new")})
	s.MustForceFlush()

	got, _ := s.Search(&storage.Query{MaxTimestamp: math.MaxInt64})
	if m := messages(got); len(got) != 1 || m["new"] != 1 {
		t.Fatalf("stored %v, want only the row inside the retention", m)
	}
	if days := s.PartitionDays(); len(days) != 1 {
		t.Fatalf("a partition was created for the expired row: days %v", days)
	}
}
