// Package netinsert routes rows to storage nodes by stream and sends them in batches.
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

	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/storage"
)

const DefaultMaxInsertBlockSize = 2 * 1024 * 1024

const DefaultFlushInterval = time.Second

const DefaultNodeDisableDuration = 10 * time.Second

type Config struct {
	Addrs []string

	Routing RoutingPolicy

	StickyRowsPerStream int

	MaxInsertBlockSize int

	FlushInterval time.Duration

	NodeDisableDuration time.Duration

	DisableCompression bool
}

type Storage struct {
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

type storageNode struct {
	addr          string
	client        *http.Client
	pending       []storage.Row
	mu            sync.Mutex
	pendingBytes  int
	routedCount   atomic.Int64
	reroutedCount atomic.Int64
	lastFlush     time.Time
	disabledUntil atomic.Int64
}

func NewStorage(cfg *Config) *Storage {
	var n []*storageNode
	for _, addr := range cfg.Addrs {
		n = append(n, &storageNode{
			addr:    addr,
			client:  &http.Client{Transport: &http.Transport{}, Timeout: 30 * time.Second},
			pending: make([]storage.Row, 0),
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
	s := &Storage{
		nodes:               n,
		router:              r,
		stop:                make(chan struct{}),
		maxInsertBlockSize:  maxInsertBlockSize,
		flushInterval:       flushInterval,
		nodeDisableDuration: nodeDisableDuration,
		disableCompression:  cfg.DisableCompression,
	}
	for _, node := range s.nodes {
		s.wg.Add(1)
		go node.flushLoop(s)
	}
	return s
}

func (s *Storage) MustAddRows(rows []storage.Row) {
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
			s.nodes[nIdx].lastFlush = time.Now()
			s.nodes[nIdx].flush(s, toSend)
		}
		s.nodes[nIdx].mu.Unlock()
	}
}

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

func (s *Storage) MustClose() {
	close(s.stop)
	done := make(chan struct{})
	go func() {
		s.wg.Wait() // flush loops must stop before the final drain
		s.MustForceFlush()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("netinsert close timeout after 5s; exiting anyway")
	}
}

func (s *Storage) NodeRowCounts() []int64 {
	counts := make([]int64, len(s.nodes))
	for i, n := range s.nodes {
		counts[i] = n.routedCount.Load()
	}
	return counts
}

func (s *Storage) NodeReroutedRows() []int64 {
	counts := make([]int64, len(s.nodes))
	for i, n := range s.nodes {
		counts[i] = n.reroutedCount.Load()
	}
	return counts
}

func (sn *storageNode) flush(s *Storage, rows []storage.Row) {
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
}

func (sn *storageNode) flushLoop(s *Storage) {
	defer s.wg.Done()
	tick := time.NewTicker(s.flushInterval)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-tick.C:
			sn.mu.Lock()
			if time.Since(sn.lastFlush) < s.flushInterval { // a size-triggered flush just ran
				sn.mu.Unlock()
				continue
			}
			toSend := sn.pending
			sn.pending = nil
			sn.pendingBytes = 0
			sn.lastFlush = time.Now()
			sn.mu.Unlock()
			sn.flush(s, toSend)
		}
	}
}

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

func (sn *storageNode) setDisabledTemporarily(d time.Duration) {
	sn.disabledUntil.Store(time.Now().Add(d).UnixNano())
}
