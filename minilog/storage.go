package minilog

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
)

// ---------------------------------------------------------------------------
// STAGE 3 (basic) and STAGE 4 (partitions, sharding) -- YOUR IMPLEMENTATION.
//
// Verified by:  go test ./harness -v   (all stages use this API)
//
// Reference: lib/logstorage/storage.go, storage_search.go.
// ---------------------------------------------------------------------------

// Config configures a Storage.
type Config struct {
	// ShardsCount is the number of independent shards rows are hashed across
	// by StreamID.
	//
	// 0 or 1 means unsharded. The stage 4 harness sweeps 1/2/4/8 and plots
	// query latency, so this must actually create parallelism -- one
	// goroutine per shard at query time, results merged at the end.
	//
	// Sharding by StreamID (not by time, not round-robin) is the choice that
	// matters: it keeps a stream's rows co-located, so a stream-filtered
	// query touches exactly one shard. Try round-robin instead at some point
	// and measure how much worse a stream query gets. That comparison is the
	// lesson.
	ShardsCount int

	// RetentionDays, if > 0, drops partitions older than this many days.
	RetentionDays int
}

// Storage is the top-level handle.
//
// Directory layout:
//
//	<path>/
//	  shard-0000/
//	    20260727/            <- partition, day granularity
//	      parts.json
//	      parts/
//	        part-000001/
//	          metadata.json metaindex.bin index.bin
//	          timestamps.bin values.bin bloom.bin
//	        part-000002/
//	    20260728/
//	  shard-0001/
//	  ...

type shard struct {
	mu         sync.Mutex
	partitions map[int64]*partition
}

type Storage struct {
	path string
	cfg  *Config

	// You will need: per-shard partition maps keyed by day, a mutex, and a
	// background goroutine driving flushes and merges.
	shards []*shard
}

// MustOpenStorage opens or creates a storage at path.
//
// Must recover cleanly from a crash at any point -- it delegates that to
// mustOpenPartition for each partition it finds.
func MustOpenStorage(path string, cfg *Config) *Storage {
	var err error
	var dir []os.DirEntry
	var day int64

	shardsCount := max(cfg.ShardsCount, 1)

	shards := make([]*shard, shardsCount)

	for i := range shardsCount {
		shards[i] = &shard{
			partitions: make(map[int64]*partition),
		}
		err = os.MkdirAll(filepath.Join(path, fmt.Sprintf("shard-%04d", i)), 0o755)
		if err != nil {
			panic(err)
		}
		dir, err = os.ReadDir(filepath.Join(path, fmt.Sprintf("shard-%04d", i)))
		if err != nil {
			panic(err)
		}
		for idx := range dir {
			if !dir[idx].Type().IsDir() {
				continue
			}
			day, err = strconv.ParseInt(dir[idx].Name(), 10, 64)
			if err != nil {
				panic(err)
			}
			shards[i].partitions[day] = mustOpenPartition(filepath.Join(path, fmt.Sprintf("shard-%04d", i), dir[idx].Name()), day)
		}
	}

	return &Storage{
		path:   path,
		cfg:    cfg,
		shards: shards,
	}
}

// MustClose flushes everything and closes the storage.
//
// After MustClose returns, every row passed to MustAddRows must be durable
// and findable after a reopen. The harnesses reopen and re-query constantly,
// so a missing flush here shows up immediately.
func (s *Storage) MustClose() {
	s.MustForceFlush()
	for _, shard := range s.shards {
		shard.mu.Lock()
		for _, pt := range shard.partitions {
			pt.MustClose()
		}
		shard.mu.Unlock()
	}
}

// MustAddRows ingests rows.
//
// Responsibilities, in order:
//  1. bump Metrics.LogicalBytesIngested by the sum of Row.LogicalSizeBytes().
//     Do it here and nowhere else -- it is the denominator of write
//     amplification and it must count logical bytes exactly once.
//  2. route each row to a shard by StreamID
//  3. route each row to a partition by dayForTimestamp(Timestamp)
//  4. hand rows to partition.mustAddRows
//
// Rows do NOT arrive sorted. Sorting is the writer's job (SortRows), and
// where you choose to do it -- per call, or once at block-build time -- is a
// real performance decision worth measuring.
func (s *Storage) MustAddRows(rows []Row) {
	SortRows(rows)
	for _, row := range rows {
		Metrics.LogicalBytesIngested.Add(int64(row.LogicalSizeBytes()))
	}
	i := 0
	for i < len(rows) {
		j := i + 1

		for j < len(rows) && s.shardForStreamID(rows[i].StreamID) == s.shardForStreamID(rows[j].StreamID) && dayForTimestamp(rows[i].Timestamp) == dayForTimestamp(rows[j].Timestamp) {
			j++
		}

		shard := s.shardForStreamID(rows[i].StreamID)
		day := dayForTimestamp(rows[i].Timestamp)

		s.shards[shard].mu.Lock()
		_, exist := s.shards[shard].partitions[day]
		if !exist {
			partPath := filepath.Join(s.path, fmt.Sprintf("shard-%04d", shard), strconv.Itoa(int(day)))
			mustCreatePartition(partPath, day)
			s.shards[shard].partitions[day] = mustOpenPartition(partPath, day)
		}
		s.shards[shard].partitions[day].mustAddRows(rows[i:j])
		s.shards[shard].mu.Unlock()
		i = j
	}
}

// Search returns rows matching q, along with instrumentation.
//
// Partition pruning happens here and it is what stage 4 measures: a query
// over a 1-hour window against a 90-day dataset must open 1 partition, not
// 90. Bump ss.PartitionsTotal for every partition considered and
// ss.PartitionsOpened only for those that survive the time-range check.
//
// Shards are searched concurrently. Give each shard its own SearchStats and
// Merge() them, or you will have a data race that `go test -race` finds and
// a stats number that is quietly wrong before it does.
//
// Result ordering: return rows sorted by (StreamID, Timestamp) so results are
// deterministic and the brute-force oracle can compare directly.
func (s *Storage) Search(q *Query) ([]Row, *SearchStats) {
	var wg sync.WaitGroup
	shardRows := make([][]Row, len(s.shards))
	shardStats := make([]*SearchStats, len(s.shards))

	for i, sh := range s.shards {
		wg.Add(1)
		go func(i int, shard *shard) {
			defer wg.Done()
			var rows []Row
			ss := &SearchStats{}
			shard.mu.Lock()
			for _, pt := range shard.partitions {
				ss.PartitionsTotal++
				st := pt.day * nsecsPerDay
				ed := pt.day*nsecsPerDay + nsecsPerDay - 1
				if q.MaxTimestamp < st || q.MinTimestamp > ed {
					continue
				}
				ss.PartitionsOpened++
				rows = append(rows, pt.search(q, ss)...)
			}
			shard.mu.Unlock()

			shardRows[i] = rows
			shardStats[i] = ss
		}(i, sh)
	}
	wg.Wait()
	var rows []Row
	ss := &SearchStats{}

	for i := range s.shards {
		rows = append(rows, shardRows[i]...)
		ss.Merge(shardStats[i])
	}

	SortRows(rows)
	rows = q.ApplyLimit(rows)
	return rows, ss
}

// MustForceFlush seals and flushes all in-memory parts.
//
// Test-only hook. The harnesses call it so they do not have to sleep waiting
// for a background timer.
func (s *Storage) MustForceFlush() {
	for _, shard := range s.shards {
		shard.mu.Lock()
		for _, pt := range shard.partitions {
			pt.mustFlushInmemoryPart()
		}
		shard.mu.Unlock()
	}
}

// MustForceMerge merges all parts in every partition down to one.
//
// Test-only hook, used by the stage 3 correctness harness to force merges to
// happen at controlled points rather than whenever the background goroutine
// felt like it.
func (s *Storage) MustForceMerge() {
	for _, shard := range s.shards {
		shard.mu.Lock()
		for _, pt := range shard.partitions {
			if len(pt.parts) < 2 {
				continue
			}
			newPath := filepath.Join(pt.path, partsDirname, fmt.Sprintf("part-%06d", pt.nextPartId))
			pt.nextPartId++
			partmd := mustMergeParts(newPath, pt.parts)
			newPart := mustOpenPart(newPath)
			oldParts := pt.parts
			pt.parts = []*part{newPart}
			Metrics.BytesLiveOnDisk.Add(int64(partmd.SizeBytes))

			var names []string
			for _, newPart = range pt.parts {
				names = append(names, filepath.Base(newPart.path))
			}
			mustWritePartsJSON(pt.path, names)
			for _, p := range oldParts {
				Metrics.BytesLiveOnDisk.Add(-int64(p.md.SizeBytes))
				p.MustClose()
				os.RemoveAll(p.path)
			}
			Metrics.PartsCreated.Add(1)
			Metrics.MergesCount.Add(1)
			Metrics.PartsDeleted.Add(int64(len(oldParts)))
			Metrics.RowsMerged.Add(int64(partmd.RowsCount))
		}
		shard.mu.Unlock()
	}
}

// MustDropPartitionsBefore removes partitions older than the given day.
//
// This should be startlingly short: find the directories, os.RemoveAll them,
// drop them from the in-memory map. That brevity is the entire argument for
// time-partitioning a log store, and it is worth pausing on when you write
// it -- compare it against what "DELETE FROM logs WHERE ts < ?" costs in a
// row store with a B-tree index.
func (s *Storage) MustDropPartitionsBefore(day int64) {
	for _, shard := range s.shards {
		shard.mu.Lock()
		for _, pt := range shard.partitions {
			if pt.day < day {
				pt.MustClose()
				os.RemoveAll(pt.path)
				delete(shard.partitions, pt.day)
			}
		}
		shard.mu.Unlock()
	}
}

// PartitionDays returns the days that currently have partitions, ascending.
// Used by the stage 4 harness.
func (s *Storage) PartitionDays() []int64 {
	var days []int64
	seen := make(map[int64]bool)
	for _, shard := range s.shards {
		shard.mu.Lock()
		for _, pt := range shard.partitions {
			if seen[pt.day] {
				continue
			}
			days = append(days, pt.day)
			seen[pt.day] = true
		}
		shard.mu.Unlock()
	}
	slices.Sort(days)
	return days
}

// shardForStreamID picks a shard.
//
// StreamID is already a hash, but do not use its low bits directly: they came
// from xxhash and are fine, but if you ever change StreamID derivation this
// silently rebalances everything. Mix explicitly.
func (s *Storage) shardForStreamID(streamID uint64) int {
	mixed := streamID ^ streamID>>33
	return int(mixed % uint64(len(s.shards)))
}
