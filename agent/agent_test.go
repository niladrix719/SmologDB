package agent

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/internalapi"
)

func TestBackoff(t *testing.T) {
	min, max := 100*time.Millisecond, 10*time.Second
	for _, c := range []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{7, 10 * time.Second},
		{100, 10 * time.Second},
		{1_000_000, 10 * time.Second},
	} {
		if got := backoff(c.attempt, min, max); got != c.want {
			t.Fatalf("backoff(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestSendBlock(t *testing.T) {
	var gotPath, gotVersion string
	var gotBody []byte
	status := http.StatusNoContent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotVersion = r.URL.Query().Get(internalapi.VersionArg)
		gotBody, _ = io.ReadAll(r.Body)
		http.Error(w, "reason the log line should carry", status)
	}))
	d := &destination{
		addr:   srv.Listener.Addr().String(),
		client: &http.Client{Transport: &http.Transport{}, Timeout: 30 * time.Second},
	}
	block := []byte("opaque block bytes")

	for _, c := range []struct {
		status int
		want   sendResult
	}{
		{http.StatusNoContent, sendStored},
		{http.StatusBadRequest, sendRejected},
		{http.StatusNotFound, sendRejected},
		{http.StatusInternalServerError, sendServerError},
		{http.StatusServiceUnavailable, sendServerError},
		{http.StatusTooManyRequests, sendServerError},
	} {
		status = c.status
		got, _ := d.sendBlock(block)
		if got != c.want {
			t.Fatalf("status %d: got result %v, want %v", c.status, got, c.want)
		}
	}
	if gotPath != internalapi.InsertPath || gotVersion != internalapi.ProtocolVersion {
		t.Fatalf("request went to %q version=%q, want %q version=%q", gotPath, gotVersion, internalapi.InsertPath, internalapi.ProtocolVersion)
	}
	if !bytes.Equal(gotBody, block) {
		t.Fatalf("body %q, want the block unchanged", gotBody)
	}

	srv.Close()
	if got, err := d.sendBlock(block); got != sendUnavailable || err == nil {
		t.Fatalf("dead node: got (%v, %v), want (sendUnavailable, non-nil error)", got, err)
	}
}

func newTestDest(t *testing.T, srvAddr string) *destination {
	d := &destination{
		addr:     srvAddr,
		client:   &http.Client{Transport: &http.Transport{}},
		q:        mustOpenQueue(t.TempDir(), 0),
		retryMin: time.Millisecond,
		retryMax: 5 * time.Millisecond,
	}
	d.cond = sync.NewCond(&d.mu)
	return d
}

func (d *destination) enqueue(block []byte) {
	d.mu.Lock()
	d.q.MustWriteBlock(block)
	d.cond.Signal()
	d.mu.Unlock()
}

func (d *destination) pending() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.q.Stats().PendingBytes
}

func eventually(timeout time.Duration, cond func() bool) bool {
	for end := time.Now().Add(timeout); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

func TestRunSender(t *testing.T) {
	var reqs atomic.Int64
	var failFirst atomic.Int64
	var reject atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqs.Add(1)
		switch {
		case reject.Load():
			http.Error(w, "bad", http.StatusBadRequest)
		case n <= failFirst.Load():
			http.Error(w, "down", http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	d := newTestDest(t, srv.Listener.Addr().String())
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { d.runSender(stop); close(done) }()

	for i := 1; i <= 3; i++ {
		d.enqueue(blk(i, 20))
	}
	if !eventually(2*time.Second, func() bool { return d.pending() == 0 }) {
		t.Fatalf("queue did not drain: %d bytes pending", d.pending())
	}
	time.Sleep(50 * time.Millisecond)
	if got := reqs.Load(); got != 3 || d.blocksSent.Load() != 3 {
		t.Fatalf("3 blocks produced %d requests, blocksSent=%d; want 3 and 3 (resent after success?)", got, d.blocksSent.Load())
	}

	reqs.Store(0)
	failFirst.Store(2)
	d.enqueue(blk(4, 20))
	if !eventually(2*time.Second, func() bool { return d.pending() == 0 }) {
		t.Fatalf("block never delivered after transient failures")
	}
	time.Sleep(50 * time.Millisecond)
	if got := reqs.Load(); got != 3 || d.retries.Load() != 2 {
		t.Fatalf("requests=%d retries=%d, want 3 and 2", got, d.retries.Load())
	}

	reqs.Store(0)
	failFirst.Store(0)
	reject.Store(true)
	d.enqueue(blk(5, 20))
	if !eventually(2*time.Second, func() bool { return d.blockRejected.Load() == 1 }) {
		t.Fatalf("rejection not counted")
	}
	reject.Store(false)
	d.enqueue(blk(6, 20))
	if !eventually(2*time.Second, func() bool { return d.pending() == 0 }) {
		t.Fatalf("queue stalled behind a rejected block")
	}

	d.mu.Lock()
	d.closed = true
	d.cond.Broadcast()
	d.mu.Unlock()
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runSender did not exit after close")
	}
}
