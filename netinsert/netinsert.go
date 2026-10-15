// Package netinsert is the client half of the insert path: it routes rows to
// storage nodes, batches them, and ships them over HTTP.
//
// ---------------------------------------------------------------------------
// STAGE 7 (routing, batching) and STAGE 9 (failure, re-routing)
// -- YOUR IMPLEMENTATION GOES HERE.
//
// Verified by:  go test ./harness -run TestStage7 -v
//
//	go test ./harness -run TestStage9 -v
//	go run ./cmd/chaostest
//
// Reference: app/vlstorage/netinsert/netinsert.go.
// ---------------------------------------------------------------------------
package netinsert

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
)

// DefaultMaxInsertBlockSize is the per-node buffer size at which a batch is
// sent without waiting for the flush ticker. Same value as VictoriaLogs.
const DefaultMaxInsertBlockSize = 2 * 1024 * 1024

// DefaultFlushInterval bounds how long a row can sit in a client-side buffer
// before being sent.
//
// This is the visibility latency floor for the whole system: a row cannot be
// queryable sooner than this, no matter what the storage node does. Everyone
// who has ever asked "why don't my logs show up immediately" is asking about
// a constant like this one.
const DefaultFlushInterval = time.Second

// DefaultNodeDisableDuration is how long a node stays out of rotation after a
// failed send.
//
// The point is not the number, it is that there IS one. Without it, a dead
// node is retried on every batch, every batch pays a TCP timeout, and the
// whole ingest path collapses to the speed of the slowest failure. With it,
// one batch pays the timeout and the next few thousand do not.
const DefaultNodeDisableDuration = 10 * time.Second

// Config configures a Storage.
type Config struct {
	// Addrs are the storage node addresses, e.g. "127.0.0.1:9001".
	//
	// A plain slice from a command-line flag, edited by a human, read once at
	// startup. No discovery, no registry, no heartbeat protocol. If that feels
	// too simple, re-read the "bet" section of CLUSTER.md: because queries hit
	// every node, this list is not load-bearing for correctness, and a thing
	// that is not load-bearing for correctness does not need consensus.
	Addrs []string

	// Routing selects the routing policy. Zero value is RoutingHybrid.
	Routing RoutingPolicy

	// StickyRowsPerStream is the RoutingHybrid threshold.
	// 0 means DefaultStickyRowsPerStream.
	StickyRowsPerStream int

	// MaxInsertBlockSize is the per-node buffer size in bytes.
	// 0 means DefaultMaxInsertBlockSize.
	MaxInsertBlockSize int

	// FlushInterval bounds buffer residency. 0 means DefaultFlushInterval.
	FlushInterval time.Duration

	// NodeDisableDuration is the cool-off after a failed send.
	// 0 means DefaultNodeDisableDuration.
	NodeDisableDuration time.Duration

	// DisableCompression sends bodies uncompressed.
	//
	// Worth a measurement rather than an assumption: log rows compress
	// extremely well, so this usually trades a lot of network for a little
	// CPU. Find out which side your setup is on before you decide.
	DisableCompression bool
}

// Storage routes and ships rows to a set of storage nodes.
//
// It implements the insert half of minilog.LogStorage. Search is deliberately
// absent -- an insert node cannot answer queries, and netstorage.Storage is
// what pairs this with a netselect.Storage to make a whole.
type Storage struct {
	// You will need: a []*storageNode, a *router, a buffer pool, a stop
	// channel, and a WaitGroup for the background flushers.
	//
	nodes   []*storageNode
	router  *router
	bufPool sync.Pool
	stop    chan struct{}
	wg      sync.WaitGroup

	maxInsertBlockSize  int
	flushInterval       time.Duration
	nodeDisableDuration time.Duration
	disableCompression  bool
}

// storageNode is one remote node and the pending data destined for it.
type storageNode struct {
	// You will need: the address, an *http.Client, a mutex-guarded pending
	// buffer, the time of the last flush, and an atomic "disabled until"
	// timestamp.
	//
	// Reuse ONE http.Client per node and let it pool connections. A fresh
	// client per request means a fresh TCP handshake per request, and on a
	// batch-per-second workload that is most of your latency. Set
	// IdleConnTimeout below the server's idle timeout, or you will race the
	// server into closing a connection you are about to write to and see
	// spurious EOFs -- VictoriaLogs has a comment and a linked bug about
	// exactly this.
	//
	addr          string
	client        *http.Client
	pending       []minilog.Row
	mu            sync.Mutex
	pendingBytes  int
	routedCount   atomic.Int64
	reroutedCount atomic.Int64
	lastFlush     time.Time
	disabledUntil atomic.Int64
}

// NewStorage returns a Storage sending to cfg.Addrs.
//
// Call MustClose when done.
func NewStorage(cfg *Config) *Storage {
	var n []*storageNode
	for _, addr := range cfg.Addrs {
		n = append(n, &storageNode{
			addr:    addr,
			client:  &http.Client{Transport: &http.Transport{}},
			pending: make([]minilog.Row, 0),
		})
	}
	stickyRowsPerStream := cfg.StickyRowsPerStream
	if stickyRowsPerStream == 0 {
		stickyRowsPerStream = DefaultStickyRowsPerStream
	}
	r := newRouter(cfg.Routing, len(cfg.Addrs), stickyRowsPerStream)
	maxInsertBlockSize := cfg.MaxInsertBlockSize
	if maxInsertBlockSize == 0 {
		maxInsertBlockSize = DefaultMaxInsertBlockSize
	}
	flushInterval := cfg.FlushInterval
	if flushInterval == 0 {
		flushInterval = DefaultFlushInterval
	}
	nodeDisableDuration := cfg.NodeDisableDuration
	if nodeDisableDuration == 0 {
		nodeDisableDuration = DefaultNodeDisableDuration
	}
	return &Storage{
		nodes:               n,
		router:              r,
		stop:                make(chan struct{}),
		maxInsertBlockSize:  maxInsertBlockSize,
		flushInterval:       flushInterval,
		nodeDisableDuration: nodeDisableDuration,
		disableCompression:  cfg.DisableCompression,
	}
}

// MustAddRows routes rows to nodes and buffers them for sending.
//
// Returns as soon as the rows are buffered -- NOT once they are durable. That
// is a real weakening of the contract compared to the local
// Storage.MustAddRows, and it is worth being explicit about it here rather
// than discovering it in stage 9:
//
//	local:   MustAddRows returns  ->  rows are in an in-memory part, and will
//	                                  survive anything short of process death
//	cluster: MustAddRows returns  ->  rows are in a client-side buffer, and
//	                                  will not survive process death at all
//
// The window is bounded by FlushInterval. VictoriaLogs accepts exactly this
// window; vlagent exists partly to close it. Write down what you would have to
// build to close it here, and notice that you have just described a
// write-ahead log -- the thing the single-node exercise also deliberately
// omits.
func (s *Storage) MustAddRows(rows []minilog.Row) {
	for _, row := range rows {
		nIdx := s.router.nodeForRow(row.StreamID)
		s.nodes[nIdx].routedCount.Add(1)
		s.nodes[nIdx].mu.Lock()
		s.nodes[nIdx].pending = append(s.nodes[nIdx].pending, row)
		s.nodes[nIdx].pendingBytes += len(internalapi.MarshalRow(nil, &row))
		if s.nodes[nIdx].pendingBytes > s.maxInsertBlockSize {
			toSend := s.nodes[nIdx].pending
			s.nodes[nIdx].pending = nil
			s.nodes[nIdx].pendingBytes = 0
			s.nodes[nIdx].flush(s, toSend)
		}
		s.nodes[nIdx].mu.Unlock()
	}
}

// MustForceFlush drains every pending buffer and makes the rows queryable.
//
// Order matters and it is the thing to get right first:
//
//  1. drain every client-side buffer to its node and wait for the sends
//  2. THEN call ForceFlushPath on every node
//
// Doing it the other way round flushes nodes that have not yet received the
// data, returns, and leaves the harness querying for rows that are still
// sitting in a buffer in this process. The symptom is a cluster test that
// finds fewer rows than it ingested, intermittently, in proportion to how
// fast the machine is.
//
// See Storage.DebugFlush and storageNode.debugFlush in the real repo, which
// are this function and have the two steps in that order for this reason.
func (s *Storage) MustForceFlush() {
	var wg sync.WaitGroup
	for _, node := range s.nodes {
		wg.Add(1)
		go func(node *storageNode) {
			defer wg.Done()
			node.mu.Lock()
			toSend := node.pending
			node.pending = nil
			node.pendingBytes = 0
			node.mu.Unlock()
			node.flush(s, toSend)
		}(node)
	}
	wg.Wait()

	for _, node := range s.nodes {
		resp, err := node.client.Get(fmt.Sprintf("http://%s%s?version=%s", node.addr, internalapi.ForceFlushPath, internalapi.ProtocolVersion))
		if err != nil {
			log.Printf("force flush %s: %v", node.addr, err)
			continue
		}
		if resp.StatusCode/100 != 2 {
			log.Printf("force flush %s: status %d", node.addr, resp.StatusCode)
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
	}
}

// MustClose drains pending data and stops the background flushers.
//
// Bound the drain. A single unresponsive node must not be able to hang
// shutdown forever -- VictoriaLogs uses -insert.drainTimeout for this and logs
// loudly when it fires, because silently dropping buffered logs on shutdown is
// exactly the kind of thing that must never be silent.
func (s *Storage) MustClose() {
	close(s.stop)
	done := make(chan struct{})
	go func() {
		s.MustForceFlush()
		close(done)
	}()
	select {
	case <-done:
		// close cleanly
	case <-time.After(5 * time.Second):
		log.Printf("netinsert close timeout after 5s; exiting anyway")
	}
}

// NodeRowCounts returns rows routed to each node since start, by node index.
//
// Instrumentation for the stage 7 balance measurement. Count rows as they are
// ROUTED, not as they are sent, so that re-routed blocks in stage 9 do not
// distort the routing histogram -- these two numbers answer different
// questions and stage 9 wants to compare them.
func (s *Storage) NodeRowCounts() []int64 {
	counts := make([]int64, len(s.nodes))
	for i, n := range s.nodes {
		counts[i] = n.routedCount.Load()
	}
	return counts
}

// NodeReroutedRows returns rows that were sent to a node OTHER than the one
// the router picked, by destination node index.
//
// Zero until stage 9. Non-zero afterwards is not a bug -- it is the system
// staying up. The stage 9 measurement checks these are spread evenly across
// the survivors rather than all landing on whichever node happens to be first
// in the list.
func (s *Storage) NodeReroutedRows() []int64 {
	counts := make([]int64, len(s.nodes))
	for i, n := range s.nodes {
		counts[i] = n.reroutedCount.Load()
	}
	return counts
}

// ---------------------------------------------------------------------------
// STAGE 9
// ---------------------------------------------------------------------------

func (sn *storageNode) flush(s *Storage, rows []minilog.Row) {
	if len(rows) == 0 {
		return
	}
	rowsToSend := internalapi.MarshalRowBatch(nil, rows)
	err := sn.sendInsertRequest(rowsToSend)
	if err != nil {
		send := s.sendToAnyNode(rowsToSend, len(rows))
		if !send {
			log.Printf("failed to %s flush: %v", sn.addr, err)
			return
		}
	}
	sn.lastFlush = time.Now()
}

// sendInsertRequest ships one buffer to sn.
//
// Returns an error the caller can distinguish. There are three outcomes and
// conflating any two of them makes stage 9 impossible:
//
//	nil                    the node stored it
//	unavailable            no HTTP response at all: refused, reset, timeout
//	a real error response  the node answered 4xx/5xx
//
// The middle case is the only one that justifies re-routing to another node.
// A 400 means the request itself is wrong (wrong protocol version, most
// likely), and sending the identical bytes to a different node will fail
// identically -- so re-routing a 400 turns one clear error into N confusing
// ones.
func (sn *storageNode) sendInsertRequest(data []byte) error {
	var err error
	var resp *http.Response
	var b []byte
	resp, err = sn.client.Post(fmt.Sprintf("http://%s%s?version=%s", sn.addr, internalapi.InsertPath, internalapi.ProtocolVersion),
		"application/octet-stream",
		bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, err = io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("insert failed: status %d: %s", resp.StatusCode, b)
	}
	return nil
}

// sendToAnyNode re-routes a buffer to any node currently believed available.
//
// Pick a RANDOM starting index, not zero. Starting at zero means every client
// re-routes to the same survivor, and the node that replaces a dead one gets
// the load of the whole cluster. The stage 9 reroute-balance measurement is
// looking for exactly this bug.
//
// Returns false if no node took it. The caller retries; it does not drop.
func (s *Storage) sendToAnyNode(data []byte, rowsCount int) bool {
	start := rand.IntN(len(s.nodes))
	for i := range s.nodes {
		idx := (start + i) % len(s.nodes)
		if s.nodes[idx].disabledUntil.Load() < time.Now().UnixNano() {
			err := s.nodes[idx].sendInsertRequest(data)
			if err != nil {
				s.nodes[idx].disabledUntil.Store(time.Now().Add(s.nodeDisableDuration).UnixNano())
				continue
			}
			s.nodes[idx].reroutedCount.Add(int64(rowsCount))
			return true
		}
	}
	return false
}

// setDisabledTemporarily takes sn out of rotation for NodeDisableDuration.
func (sn *storageNode) setDisabledTemporarily(d time.Duration) {
	sn.disabledUntil.Store(time.Now().Add(d).UnixNano())
}
