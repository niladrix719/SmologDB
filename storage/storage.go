// Package storage is the single-node log store: rows are grouped by stream into
// compressed column blocks with a bloom filter each, kept in one partition per day.
package storage

import (
	"cmp"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	ShardsCount int

	RetentionDays int

	// FlushInterval, if set, writes in-memory rows to a searchable part this often.
	// Zero means only when InmemoryPartMaxRows is reached.
	FlushInterval time.Duration
}

type shard struct {
	mu         sync.Mutex
	partitions map[int64]*partition
}

type Storage struct {
	path string
	cfg  *Config

	wg   sync.WaitGroup
	stop chan struct{}

	shards []*shard
}

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

	s := &Storage{
		path:   path,
		cfg:    cfg,
		shards: shards,
		stop:   make(chan struct{}),
	}
	if cfg.RetentionDays > 0 {
		s.MustDropPartitionsBefore(s.retentionCutoffDay())
		s.wg.Add(1)
		go s.retentionLoop()
	}
	if cfg.FlushInterval > 0 {
		s.wg.Add(1)
		go s.flushLoop()
	}
	return s
}

// retentionCutoffDay is the oldest day that is kept: today plus the RetentionDays-1 days
// before it. With retention off it is the lowest value, so no day is ever older than it.
func (s *Storage) retentionCutoffDay() int64 {
	if s.cfg.RetentionDays <= 0 {
		return math.MinInt64
	}

	return dayForTimestamp(time.Now().UnixNano()) - int64(s.cfg.RetentionDays) + 1
}

func (s *Storage) retentionLoop() {
	defer s.wg.Done()
	tick := time.NewTicker(1 * time.Hour)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C:
			s.MustDropPartitionsBefore(s.retentionCutoffDay())
		}
	}
}

func (s *Storage) flushLoop() {
	defer s.wg.Done()
	tick := time.NewTicker(s.cfg.FlushInterval)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C:
			s.MustForceFlush()
		}
	}
}

func (s *Storage) MustClose() {
	select {
	case <-s.stop: // already closed; a second MustClose is a no-op for the flusher
	default:
		close(s.stop)
	}
	s.wg.Wait()
	s.MustForceFlush()
	for _, shard := range s.shards {
		shard.mu.Lock()
		for _, pt := range shard.partitions {
			pt.MustClose()
		}
		shard.mu.Unlock()
	}
}

func (s *Storage) MustAddRows(rows []Row) {
	SortRows(rows)
	for _, row := range rows {
		Metrics.LogicalBytesIngested.Add(int64(row.LogicalSizeBytes()))
	}
	cutOff := s.retentionCutoffDay()
	i := 0
	for i < len(rows) {
		j := i + 1

		for j < len(rows) && s.shardForStreamID(rows[i].StreamID) == s.shardForStreamID(rows[j].StreamID) && dayForTimestamp(rows[i].Timestamp) == dayForTimestamp(rows[j].Timestamp) {
			j++
		}

		shard := s.shardForStreamID(rows[i].StreamID)
		day := dayForTimestamp(rows[i].Timestamp)
		if day < cutOff {
			i = j
			continue
		}

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
	if q.Newest {
		slices.SortStableFunc(rows, func(a, b Row) int {
			return cmp.Compare(b.Timestamp, a.Timestamp)
		})
	}
	rows = q.ApplyLimit(rows)
	return rows, ss
}

func (s *Storage) MustForceFlush() {
	for _, shard := range s.shards {
		shard.mu.Lock()
		for _, pt := range shard.partitions {
			pt.mustFlushInmemoryPart()
		}
		shard.mu.Unlock()
	}
}

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

func (s *Storage) shardForStreamID(streamID uint64) int {
	mixed := streamID ^ streamID>>33
	return int(mixed % uint64(len(s.shards)))
}
