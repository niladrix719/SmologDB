package storage_test

import (
	"math"
	"math/rand"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/storage"
)

func TestMergeCorrectness(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	cfg.Streams = 50
	ds := gen.Generate(cfg)

	want := make([]storage.Row, len(ds.Rows))
	copy(want, ds.Rows)
	storage.SortRows(want)

	fullScan := &storage.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}

	const schedules = 100
	rng := rand.New(rand.NewSource(1234))

	for sched := 0; sched < schedules; sched++ {
		s := newStorage(t, &storage.Config{ShardsCount: 1})

		rows := ds.Rows
		for len(rows) > 0 {
			n := 1 + rng.Intn(8000)
			if n > len(rows) {
				n = len(rows)
			}
			s.MustAddRows(rows[:n])
			rows = rows[n:]

			if rng.Intn(4) == 0 {
				s.MustForceFlush()
			}
			if rng.Intn(10) == 0 {
				s.MustForceMerge()
			}
		}
		s.MustForceFlush()

		gotBefore, _ := s.Search(fullScan)
		if ok, msg := rowsEqual(gotBefore, want); !ok {
			t.Fatalf("schedule %d: full scan wrong BEFORE final merge: %s", sched, msg)
		}

		s.MustForceMerge()
		gotAfter, _ := s.Search(fullScan)
		if ok, msg := rowsEqual(gotAfter, want); !ok {
			t.Fatalf("schedule %d: full scan wrong AFTER merge: %s", sched, msg)
		}
	}
	t.Logf("%d random merge schedules, all produced identical full scans", schedules)
}

func TestMergePreservesFilters(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 100_000
	ds := gen.Generate(cfg)

	s := newStorage(t, &storage.Config{ShardsCount: 1})
	ingest(t, s, ds, 3_000)

	q := &storage.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		Filters:      []storage.Filter{{Column: "_msg", Token: gen.RareToken}},
	}
	want := bruteForce(ds.Rows, q)

	before, ssBefore := s.Search(q)
	if ok, msg := rowsEqual(before, want); !ok {
		t.Fatalf("before merge: %s", msg)
	}

	s.MustForceMerge()

	after, ssAfter := s.Search(q)
	if ok, msg := rowsEqual(after, want); !ok {
		t.Fatalf("after merge, the selective query lost rows: %s", msg)
	}

	t.Logf("before merge: %s", ssBefore)
	t.Logf("after  merge: %s", ssAfter)
	t.Logf("blocks scanned %d -> %d after merge", ssBefore.BlocksScanned, ssAfter.BlocksScanned)
}
