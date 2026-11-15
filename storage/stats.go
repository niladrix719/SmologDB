package storage

import (
	"fmt"
	"sync/atomic"
)

type SearchStats struct {
	PartitionsTotal int

	PartitionsOpened int

	PartsTotal  int
	PartsOpened int

	BlocksTotal int

	BlocksSkippedByTime int

	BlocksSkippedByBloom int

	BlocksScanned int

	BloomMaybe        int
	BloomTruePositive int

	RowsScanned int
	RowsMatched int

	BytesReadFromDisk int64

	NodesQueried int
	NodesFailed  int

	BytesReceivedFromNodes int64
}

func (ss *SearchStats) Merge(src *SearchStats) {
	ss.PartitionsTotal += src.PartitionsTotal
	ss.PartitionsOpened += src.PartitionsOpened
	ss.PartsTotal += src.PartsTotal
	ss.PartsOpened += src.PartsOpened
	ss.BlocksTotal += src.BlocksTotal
	ss.BlocksSkippedByTime += src.BlocksSkippedByTime
	ss.BlocksSkippedByBloom += src.BlocksSkippedByBloom
	ss.BlocksScanned += src.BlocksScanned
	ss.BloomMaybe += src.BloomMaybe
	ss.BloomTruePositive += src.BloomTruePositive
	ss.RowsScanned += src.RowsScanned
	ss.RowsMatched += src.RowsMatched
	ss.BytesReadFromDisk += src.BytesReadFromDisk
	ss.NodesQueried += src.NodesQueried
	ss.NodesFailed += src.NodesFailed
	ss.BytesReceivedFromNodes += src.BytesReceivedFromNodes
}

func (ss *SearchStats) IsPartial() bool {
	return ss.NodesFailed > 0
}

func (ss *SearchStats) BloomFalsePositives() int {
	return ss.BloomMaybe - ss.BloomTruePositive
}

func (ss *SearchStats) SkipRatio() float64 {
	if ss.BlocksTotal == 0 {
		return 0
	}
	skipped := ss.BlocksSkippedByTime + ss.BlocksSkippedByBloom
	return float64(skipped) / float64(ss.BlocksTotal)
}

func (ss *SearchStats) String() string {
	return fmt.Sprintf(
		"partitions %d/%d, parts %d/%d, blocks total=%d skipTime=%d skipBloom=%d scanned=%d (skip %.2f%%), "+
			"bloom maybe=%d truePos=%d fp=%d, rows scanned=%d matched=%d, read=%.1fMiB",
		ss.PartitionsOpened, ss.PartitionsTotal,
		ss.PartsOpened, ss.PartsTotal,
		ss.BlocksTotal, ss.BlocksSkippedByTime, ss.BlocksSkippedByBloom, ss.BlocksScanned,
		ss.SkipRatio()*100,
		ss.BloomMaybe, ss.BloomTruePositive, ss.BloomFalsePositives(),
		ss.RowsScanned, ss.RowsMatched,
		float64(ss.BytesReadFromDisk)/(1<<20),
	) + ss.clusterSuffix()
}

func (ss *SearchStats) clusterSuffix() string {
	if ss.NodesQueried == 0 {
		return ""
	}
	partial := ""
	if ss.IsPartial() {
		partial = " PARTIAL"
	}
	return fmt.Sprintf(", nodes %d/%d ok, net=%.1fMiB%s",
		ss.NodesQueried-ss.NodesFailed, ss.NodesQueried,
		float64(ss.BytesReceivedFromNodes)/(1<<20), partial)
}

type GlobalStats struct {
	LogicalBytesIngested atomic.Int64

	BytesWrittenToDisk atomic.Int64

	BytesLiveOnDisk atomic.Int64

	PartsCreated atomic.Int64
	PartsDeleted atomic.Int64

	MergesCount atomic.Int64
	RowsMerged  atomic.Int64
}

var Metrics GlobalStats

func ResetMetrics() {
	Metrics.LogicalBytesIngested.Store(0)
	Metrics.BytesWrittenToDisk.Store(0)
	Metrics.BytesLiveOnDisk.Store(0)
	Metrics.PartsCreated.Store(0)
	Metrics.PartsDeleted.Store(0)
	Metrics.MergesCount.Store(0)
	Metrics.RowsMerged.Store(0)
}

func WriteAmplification() float64 {
	logical := Metrics.LogicalBytesIngested.Load()
	if logical == 0 {
		return 0
	}
	return float64(Metrics.BytesWrittenToDisk.Load()) / float64(logical)
}

func CompressionRatio() float64 {
	live := Metrics.BytesLiveOnDisk.Load()
	if live == 0 {
		return 0
	}
	return float64(Metrics.LogicalBytesIngested.Load()) / float64(live)
}
