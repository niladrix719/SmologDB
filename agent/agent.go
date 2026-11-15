// Package agent collects logs from files and HTTP and delivers them to routers through an on-disk queue.
package agent

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/storage"
)

const DefaultMaxBlockSize = 2 * 1024 * 1024

const DefaultFlushInterval = time.Second

const (
	DefaultRetryMinInterval = 100 * time.Millisecond
	DefaultRetryMaxInterval = 10 * time.Second
)

type Config struct {
	Addrs []string

	DataPath string

	MaxPendingBytes int64

	MaxBlockSize int

	FlushInterval time.Duration

	RetryMinInterval time.Duration
	RetryMaxInterval time.Duration
}

type DestStats struct {
	Addr string

	Queue QueueStats

	BlocksSent int64
	BytesSent  int64

	Retries int64

	BlocksRejected int64
}

type Agent struct {
	dests []*destination

	maxBlockSize  int
	flushInterval time.Duration

	mu      sync.Mutex
	rows    []storage.Row
	rowSize int

	stop chan struct{}
	wg   sync.WaitGroup
}

type destination struct {
	addr   string
	client *http.Client

	mu     sync.Mutex
	cond   *sync.Cond
	q      *queue
	closed bool

	retryMin, retryMax time.Duration

	blocksSent, bytesSent, retries, blockRejected atomic.Int64
}

func NewAgent(cfg *Config) *Agent {
	destinations := make([]*destination, len(cfg.Addrs))
	for i, addr := range cfg.Addrs {
		d := &destination{
			addr:     addr,
			client:   &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 30 * time.Second},
			q:        mustOpenQueue(filepath.Join(cfg.DataPath, addr), cfg.MaxPendingBytes),
			retryMin: cmp.Or(cfg.RetryMinInterval, DefaultRetryMinInterval),
			retryMax: cmp.Or(cfg.RetryMaxInterval, DefaultRetryMaxInterval),
		}
		d.cond = sync.NewCond(&d.mu)
		destinations[i] = d
	}
	a := &Agent{
		dests:         destinations,
		maxBlockSize:  cmp.Or(cfg.MaxBlockSize, DefaultMaxBlockSize),
		flushInterval: cmp.Or(cfg.FlushInterval, DefaultFlushInterval),
		stop:          make(chan struct{}),
	}
	for _, dest := range destinations {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			dest.runSender(a.stop)
		}()
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		t := time.NewTicker(a.flushInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				a.MustFlush()
			case <-a.stop:
				return
			}
		}
	}()
	return a
}

func (a *Agent) MustAddRows(rows []storage.Row) {
	a.mu.Lock()
	a.rows = append(a.rows, rows...)
	for _, row := range rows {
		a.rowSize += len(internalapi.MarshalRow(nil, &row))
	}
	full := a.rowSize >= a.maxBlockSize
	a.mu.Unlock()
	if full {
		a.MustFlush()
	}
}

func (a *Agent) MustFlush() {
	a.mu.Lock()
	rows := a.rows
	a.rows, a.rowSize = nil, 0
	a.mu.Unlock()
	if len(rows) == 0 {
		return
	}
	// Encoded once here and written to every destination's queue.
	data := internalapi.MarshalRowBatch(nil, rows)
	for _, dest := range a.dests {
		dest.mu.Lock()
		dest.q.MustWriteBlock(data)
		dest.cond.Signal()
		dest.mu.Unlock()
	}
}

func (a *Agent) WaitDrained(timeout time.Duration) bool {
	a.MustFlush()
	deadline := time.Now().Add(timeout)
	for {
		if a.drained() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (a *Agent) drained() bool {
	for _, dest := range a.dests {
		dest.mu.Lock()
		pending := dest.q.Stats().PendingBytes
		dest.mu.Unlock()
		if pending != 0 {
			return false
		}
	}
	return true
}

func (a *Agent) Stats() []DestStats {
	destStats := make([]DestStats, len(a.dests))
	for i, dest := range a.dests {
		destStats[i].Addr = dest.addr
		dest.mu.Lock()
		destStats[i].Queue = dest.q.Stats()
		dest.mu.Unlock()
		destStats[i].BlocksSent = dest.blocksSent.Load()
		destStats[i].BytesSent = dest.bytesSent.Load()
		destStats[i].Retries = dest.retries.Load()
		destStats[i].BlocksRejected = dest.blockRejected.Load()
	}
	return destStats
}

func (a *Agent) MustClose() {
	a.MustFlush()
	for _, dest := range a.dests {
		dest.mu.Lock()
		dest.closed = true
		dest.cond.Broadcast()
		dest.mu.Unlock()
	}
	close(a.stop)
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("agent: senders did not stop in 5s; leaving queue open")
		return
	}
	for _, dest := range a.dests {
		dest.mu.Lock()
		dest.q.MustClose()
		dest.mu.Unlock()
	}
}

type sendResult int

const (
	sendStored sendResult = iota
	sendUnavailable
	sendServerError
	sendRejected
)

func (d *destination) sendBlock(block []byte) (sendResult, error) {
	var resp *http.Response
	var b []byte
	var err error
	resp, err = d.client.Post(fmt.Sprintf("http://%s%s?version=%s", d.addr, internalapi.InsertPath, internalapi.ProtocolVersion),
		"application/octet-stream",
		bytes.NewReader(block))
	if err != nil {
		return sendUnavailable, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return sendStored, nil
	}
	b, _ = io.ReadAll(resp.Body)
	err = fmt.Errorf("%s: status: %d: %s", d.addr, resp.StatusCode, b)
	if resp.StatusCode == 400 || resp.StatusCode == 404 {
		return sendRejected, err // the block itself is bad; retrying can't help
	}
	return sendServerError, err
}

func (d *destination) runSender(stopCh <-chan struct{}) {
	for {
		d.mu.Lock()
		var block []byte
		for {
			if d.closed {
				d.mu.Unlock()
				return
			}
			var ok bool
			block, ok = d.q.MustPeekBlock(nil)
			if ok {
				break
			}
			d.cond.Wait()
		}
		d.mu.Unlock()
		for attempt := 0; ; attempt++ {
			result, err := d.sendBlock(block)
			if result == sendStored {
				d.blocksSent.Add(1)
				d.bytesSent.Add(int64(len(block)))
				break
			}
			if result == sendRejected {
				d.blockRejected.Add(1)
				log.Printf("server %s rejected a block dropping it: %v", d.addr, err)
				break
			}
			d.retries.Add(1)
			select {
			case <-stopCh:
				return
			case <-time.After(backoff(attempt, d.retryMin, d.retryMax)):
			}
			d.mu.Lock()
			b, ok := d.q.MustPeekBlock(block[:0])
			d.mu.Unlock()
			if !ok {
				break
			}
			block = b
		}
		d.mu.Lock()
		d.q.MustAck()
		d.mu.Unlock()
	}
}

func backoff(attempt int, minInterval, maxInterval time.Duration) time.Duration {
	d := minInterval
	for i := 0; i < attempt && d < maxInterval; i++ {
		d *= 2
	}
	return min(d, maxInterval)
}
