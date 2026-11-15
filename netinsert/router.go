package netinsert

import (
	"math/rand/v2"
	"sync"
)

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage7 -v
//
// Reference: streamRowsTracker in app/vlstorage/netinsert/netinsert.go.
//
// Read that function AFTER you have implemented RoutingHybrid yourself and
// seen the tradeoff table. It is about fifteen lines and it will look obvious
// in hindsight; the value is in having derived why it needs to exist.
// ---------------------------------------------------------------------------

// RoutingPolicy decides which storage node a row is sent to.
//
// You already wrote a router once: Storage.shardForStreamID in
// minilog/storage.go, choosing among in-process shards. This is the same
// function one level up, and the reason it deserves a whole stage here and
// three lines there is that the cost of a bad choice is different. A skewed
// in-process shard costs you a lock; a skewed storage node costs you a machine.
type RoutingPolicy int

const (
	// RoutingHybrid is the default and is what VictoriaLogs does: a stream's
	// first StickyRowsPerStream rows go to one node chosen by its hash, and
	// everything after that is sprayed at random.
	//
	// The argument, which is worth reconstructing before you read theirs: a
	// stream with forty rows is a rounding error, so keeping it whole costs
	// nothing and makes a stream-filtered query cheap. A stream with forty
	// million rows is the query you actually care about, and you want every
	// node grinding on it at once. The threshold is where one turns into the
	// other.
	RoutingHybrid RoutingPolicy = iota

	// RoutingSticky sends every row of a stream to the same node, forever.
	//
	// Perfect locality, and it is a trap. Real log streams are Zipfian: a
	// handful of chatty services produce most of the volume. Sticky routing
	// puts each of those on exactly one node. Stage 7 measures how bad that
	// gets, and the number is larger than most people guess.
	RoutingSticky

	// RoutingSpray picks a node uniformly at random per row.
	//
	// Perfect balance, no locality. Every stream is smeared across every node,
	// so a single-stream query is a full fan-out and the merge has real work
	// to do. Implement it even though nobody would ship it: it is the control
	// in the experiment, and it is also the routing that will find the merge
	// bugs stage 8 cares about, because it guarantees streams are split.
	//
	// Random, not round-robin. Round-robin correlates node assignment with
	// arrival order, so if anything upstream batches by stream -- and
	// something always does -- you get a systematically skewed distribution
	// that looks perfectly balanced in a synthetic test.
	RoutingSpray
)

// String returns the policy name, for tables and error messages.
func (p RoutingPolicy) String() string {
	switch p {
	case RoutingHybrid:
		return "hybrid"
	case RoutingSticky:
		return "sticky"
	case RoutingSpray:
		return "spray"
	default:
		return "unknown"
	}
}

// DefaultStickyRowsPerStream is the hybrid threshold.
//
// 1000 is what VictoriaLogs uses. It is not derived from anything; it is a
// judgement about where "small stream" stops. Sweep it in stage 7 and find out
// whether your dataset agrees.
const DefaultStickyRowsPerStream = 1000

// router assigns rows to storage nodes.
//
// It is shared by every goroutine calling MustAddRows, so the hybrid policy's
// per-stream counter needs a mutex -- and that mutex is on the hot path of
// every single ingested row. Before you write it, predict what that costs, and
// afterwards measure it under `go test -race` and under a real benchmark.
//
// (VictoriaLogs takes the same lock, per row, in getNodeIdx. There is a fast
// path for the single-node case above it. Note where they put the fast path
// and why it is where it is.)
type router struct {
	policy     RoutingPolicy
	nodesCount int
	stickyRows int

	mu      sync.Mutex
	streams map[uint64]*streamState
}

type streamState struct {
	node int
	rows int
}

// newRouter returns a router over nodesCount nodes.
//
// stickyRows is the hybrid threshold; it is ignored by the other policies.
func newRouter(policy RoutingPolicy, nodesCount, stickyRows int) *router {
	return &router{
		policy:     policy,
		nodesCount: nodesCount,
		stickyRows: stickyRows,
		streams:    make(map[uint64]*streamState),
	}
}

// nodeForRow returns the index of the node the next row of streamID goes to.
//
// Called once per row. It is not a pure function under RoutingHybrid or
// RoutingSpray -- the same streamID can return different nodes on successive
// calls, by design. Do not memoise it.
//
// Do not use the low bits of streamID directly, for the same reason
// Storage.shardForStreamID does not: StreamIDForLabels is an xxhash, its low
// bits are fine today, and the day someone changes how stream ids are derived
// you want the two levels of routing to fail independently rather than
// silently correlate. Mix explicitly.
func (r *router) nodeForRow(streamID uint64) int {
	switch r.policy {
	case RoutingSpray:
		return rand.IntN(r.nodesCount)
	case RoutingSticky:
		mixed := streamID ^ streamID>>33
		return int(mixed % uint64(r.nodesCount))
	case RoutingHybrid:
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.streams[streamID] != nil {
			if r.streams[streamID].rows >= r.stickyRows {
				return rand.IntN(r.nodesCount)
			} else {
				r.streams[streamID].rows++
				n := r.streams[streamID].node
				return n
			}
		} else {
			mixed := streamID ^ streamID>>33
			n := int(mixed % uint64(r.nodesCount))
			r.streams[streamID] = &streamState{node: n, rows: 1}
			return n
		}
	default:
		return 0
	}
}

// activeStreams returns how many distinct streams the router is tracking.
//
// Under RoutingHybrid this map grows with stream cardinality and is never
// pruned -- which is a real memory leak dressed as a counter, and it is the
// same one VictoriaLogs has (see getActiveStreams). Note the bound: this is
// one map entry per distinct stream since process start. Work out what that
// costs at a million streams, then decide whether you care.
func (r *router) activeStreams() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}
