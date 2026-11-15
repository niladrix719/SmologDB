package storage_test

import (
	"math"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/storage"
)

const nsPerDay = int64(24 * 3600 * 1e9)

func generate90Days(rows int) *gen.Dataset {
	cfg := gen.DefaultConfig()
	cfg.Rows = rows
	cfg.Streams = 100
	cfg.SpanNanos = 90 * nsPerDay
	return gen.Generate(cfg)
}

func TestPartitionPruning(t *testing.T) {
	ds := generate90Days(900_000)

	s := newStorage(t, &storage.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	days := s.PartitionDays()
	t.Logf("partitions created: %d (dataset spans 90 days)", len(days))
	if len(days) < 80 {
		t.Fatalf("expected ~90 daily partitions, got %d -- is dayForTimestamp being applied?", len(days))
	}

	mid := days[len(days)/2]
	start := mid*nsPerDay + 6*3600*1e9
	q := &storage.Query{MinTimestamp: start, MaxTimestamp: start + 3600*1e9}

	got, ss := s.Search(q)
	want := bruteForce(ds.Rows, q)
	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("1-hour query disagrees with brute force: %s", msg)
	}

	t.Logf("1-hour query over 90 days: %s", ss)
	t.Logf("  partitions opened: %d / %d", ss.PartitionsOpened, ss.PartitionsTotal)

	if ss.PartitionsTotal < 80 {
		t.Fatalf("PartitionsTotal is %d, want one count per partition considered", ss.PartitionsTotal)
	}
	if ss.PartitionsOpened > 2 {
		t.Errorf("a 1-hour query opened %d partitions, expected 1 (2 at a day boundary).", ss.PartitionsOpened)
	}

	qAll := &storage.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}
	_, ssAll := s.Search(qAll)
	if ssAll.PartitionsOpened != ssAll.PartitionsTotal {
		t.Errorf("full-range query opened %d/%d partitions -- pruning is dropping data", ssAll.PartitionsOpened, ssAll.PartitionsTotal)
	}
	t.Logf("pruning avoided %.0f%% of partitions on the selective query", (1-float64(ss.PartitionsOpened)/float64(ss.PartitionsTotal))*100)
}

func TestDropOldPartitions(t *testing.T) {
	ds := generate90Days(300_000)

	s := newStorage(t, &storage.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	before := s.PartitionDays()
	if len(before) < 80 {
		t.Fatalf("expected ~90 partitions, got %d", len(before))
	}

	cutoff := before[len(before)/2]

	start := time.Now()
	s.MustDropPartitionsBefore(cutoff)
	elapsed := time.Since(start)

	after := s.PartitionDays()
	for _, d := range after {
		if d < cutoff {
			t.Fatalf("partition for day %d survived a drop with cutoff %d", d, cutoff)
		}
	}

	dropped := len(before) - len(after)
	t.Logf("dropped %d partitions (~%d rows) in %v", dropped, len(ds.Rows)/2, elapsed)

	q := &storage.Query{MinTimestamp: cutoff * nsPerDay, MaxTimestamp: math.MaxInt64}
	got, _ := s.Search(q)
	want := bruteForce(ds.Rows, q)
	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("retention damaged surviving data: %s", msg)
	}
}

func TestStreamFilterHitsOneShard(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	cfg.Streams = 500
	ds := gen.Generate(cfg)

	s := newStorage(t, &storage.Config{ShardsCount: 8})
	ingest(t, s, ds, 10_000)

	streamID := ds.Streams[0].ID
	q := &storage.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
		StreamID:     &streamID,
	}

	got, ss := s.Search(q)
	want := bruteForce(ds.Rows, q)
	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("stream-filtered query disagrees with brute force: %s", msg)
	}

	t.Logf("stream-filtered query over 8 shards: %s", ss)

}
