package netinsert

import (
	"math/rand/v2"
	"sync"
)

type RoutingPolicy int

const (
	RoutingHybrid RoutingPolicy = iota

	RoutingSticky

	RoutingSpray
)

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

const DefaultStickyRowsPerStream = 1000

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

func newRouter(policy RoutingPolicy, nodesCount, stickyRows int) *router {
	return &router{
		policy:     policy,
		nodesCount: nodesCount,
		stickyRows: stickyRows,
		streams:    make(map[uint64]*streamState),
	}
}

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

func (r *router) activeStreams() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.streams)
}
