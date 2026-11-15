package minilog

import (
	"fmt"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// The harness measurements read these counters; a code path that forgets to
// increment them makes the harness report zeros and fail.
//
// Every counter below has a comment naming the exact place the code bumps it.
// ---------------------------------------------------------------------------

// SearchStats is per-query instrumentation.
//
// A *SearchStats is threaded through the whole read path. It is NOT safe for
// concurrent use by default -- if you parallelise search across shards, give
// each shard its own SearchStats and Merge() them at the end.
type SearchStats struct {
	// PartitionsTotal is the number of partitions that exist in the storage.
	// Bump in: Storage.Search, when enumerating partitions.
	PartitionsTotal int

	// PartitionsOpened is the number of partitions whose parts were actually
	// read, i.e. those that survived time-range pruning.
	// Bump in: Storage.Search, after the time-range check passes.
	PartitionsOpened int

	// PartsTotal / PartsOpened are the same idea one level down: a part has a
	// (minTimestamp, maxTimestamp) in its metadata and can be skipped whole.
	// Bump in: partition.search.
	PartsTotal  int
	PartsOpened int

	// BlocksTotal is every block header considered.
	// Bump in: part.searchBlocks, once per block header read.
	BlocksTotal int

	// BlocksSkippedByTime is blocks eliminated by the query time range alone.
	// Bump in: part.searchBlocks, before consulting any bloom filter.
	BlocksSkippedByTime int

	// BlocksSkippedByBloom is blocks eliminated because a bloom filter said
	// "definitely not present".
	//
	// This is THE number for stage 2. If it stays at zero you are not
	// consulting your bloom filters.
	// Bump in: part.searchBlocks, when bloom.Contains returns false.
	BlocksSkippedByBloom int

	// BlocksScanned is blocks whose values were actually decoded.
	// Bump in: part.mustReadBlockRows.
	BlocksScanned int

	// BloomMaybe counts blocks where every bloom filter said "maybe".
	// BloomTruePositive counts those where the token really was present.
	//
	// The difference is your empirical false-positive count.
	// Bump BloomMaybe in: part.searchBlocks, when all filters pass.
	// Bump BloomTruePositive in: part.mustReadBlockRows, when the token is
	// actually found in the decoded values.
	BloomMaybe        int
	BloomTruePositive int

	// RowsScanned is rows decoded; RowsMatched is rows returned.
	// Bump in: part.mustReadBlockRows.
	RowsScanned int
	RowsMatched int

	// BytesReadFromDisk is bytes pulled off disk during this query.
	// Bump in: your file-read helper in part_reader.go.
	BytesReadFromDisk int64

	// --- cluster (stages 8-10) ------------------------------------------
	//
	// These are set by netselect.Storage.Search and are zero for a local
	// query. A local storage never touches them, which is why Merge can sum
	// them unconditionally.

	// NodesQueried is the number of storage nodes the query was sent to.
	// NodesFailed is how many of those did not return a usable answer.
	//
	// Set in: netselect.Storage.Search, ONCE, after the fan-out completes.
	// Do not set them per-node -- a per-node SearchStats describes that
	// node's local work and knows nothing about the cluster.
	NodesQueried int
	NodesFailed  int

	// BytesReceivedFromNodes is the total response body size, in bytes, read
	// back from storage nodes.
	//
	// This is the network-side twin of BytesReadFromDisk, and it is the
	// number stage 10 moves: pushing Limit down to the nodes should collapse
	// it without changing the answer.
	//
	// Bump in: netselect's per-node response reader.
	BytesReceivedFromNodes int64
}

// Merge folds src into ss. Use it when searching partitions/shards in parallel.
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

// IsPartial reports whether this result is missing data from at least one
// storage node.
//
// A caller that ignores this is the failure mode stage 9 exists to prevent: a
// partial answer that looks exactly like a complete one. Anything that
// renders, alerts on, or compares a query result must consult it.
func (ss *SearchStats) IsPartial() bool {
	return ss.NodesFailed > 0
}

// BloomFalsePositives is the number of blocks a bloom filter waved through
// that turned out not to contain the token.
func (ss *SearchStats) BloomFalsePositives() int {
	return ss.BloomMaybe - ss.BloomTruePositive
}

// SkipRatio is the fraction of blocks eliminated without decoding values.
// This is the headline number of the whole index-free design.
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

// clusterSuffix renders the cluster counters, and only when there are any, so
// that stage 1-4 log lines are unchanged.
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

// ---------------------------------------------------------------------------

// GlobalStats are process-wide counters, mostly about write amplification.
//
// There is one package-level instance, Metrics. It is safe for concurrent use.
type GlobalStats struct {
	// LogicalBytesIngested is the sum of Row.LogicalSizeBytes() for every row
	// handed to Storage.MustAddRows.
	// Bump in: Storage.MustAddRows.
	LogicalBytesIngested atomic.Int64

	// BytesWrittenToDisk is every byte your code writes to a file, including
	// bytes written during merges and then later deleted.
	//
	// BytesWrittenToDisk / LogicalBytesIngested is your write amplification.
	// Bump in: your file-write helper in part_writer.go. Bump it in ONE place
	// so you cannot forget a path.
	BytesWrittenToDisk atomic.Int64

	// BytesLiveOnDisk is the size of parts currently referenced by parts.json.
	// Set in: Storage/partition after each flush and merge.
	BytesLiveOnDisk atomic.Int64

	// PartsCreated counts every part directory ever created (flushes + merges).
	// PartsDeleted counts every part directory removed after a merge.
	PartsCreated atomic.Int64
	PartsDeleted atomic.Int64

	// MergesCount is the number of completed merges.
	// RowsMerged is the number of rows rewritten by merges.
	MergesCount atomic.Int64
	RowsMerged  atomic.Int64
}

// Metrics is the process-wide counter set.
var Metrics GlobalStats

// ResetMetrics zeroes the global counters. Harnesses call this between runs.
func ResetMetrics() {
	Metrics.LogicalBytesIngested.Store(0)
	Metrics.BytesWrittenToDisk.Store(0)
	Metrics.BytesLiveOnDisk.Store(0)
	Metrics.PartsCreated.Store(0)
	Metrics.PartsDeleted.Store(0)
	Metrics.MergesCount.Store(0)
	Metrics.RowsMerged.Store(0)
}

// WriteAmplification is bytes written to disk per logical byte ingested.
//
// An LSM tree always has this above 1: data is rewritten by every merge it
// passes through. Sweeping SmallPartsMergeThreshold moves this number in one
// direction and the query-time block count in the other. That tradeoff curve
// is the whole point of stage 3.
func WriteAmplification() float64 {
	logical := Metrics.LogicalBytesIngested.Load()
	if logical == 0 {
		return 0
	}
	return float64(Metrics.BytesWrittenToDisk.Load()) / float64(logical)
}

// CompressionRatio is logical bytes per live byte on disk. Higher is better.
func CompressionRatio() float64 {
	live := Metrics.BytesLiveOnDisk.Load()
	if live == 0 {
		return 0
	}
	return float64(Metrics.LogicalBytesIngested.Load()) / float64(live)
}
