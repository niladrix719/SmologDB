// Package netselect is the client half of the query path: it fans a query out
// to every storage node, merges the answers, and merges the stats.
//
// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage8 -v
//
//	go test ./harness -run TestStage9 -v
//	go test ./harness -run TestStage10 -v
//
// Reference: app/vlstorage/netselect/netselect.go.
// ---------------------------------------------------------------------------
package netselect

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
)

// Config configures a Storage.
type Config struct {
	// Addrs are the storage node addresses. Every query goes to all of them.
	//
	// All of them, every time, with no exceptions and no routing table. This
	// is the single decision that makes the rest of the cluster simple, and it
	// is also the one you pay for on every selective query. Stage 8 puts a
	// number on the bill.
	Addrs []string

	// DisableCompression asks nodes for uncompressed responses.
	DisableCompression bool
}

// Storage queries a set of storage nodes and merges their answers.
//
// It implements the select half of minilog.LogStorage. MustAddRows is absent:
// a select node cannot ingest. netstorage.Storage pairs this with a
// netinsert.Storage.
type Storage struct {
	nodes []*storageNode
}

type storageNode struct {
	addr   string
	client *http.Client
}

// NewStorage returns a Storage querying cfg.Addrs.
func NewStorage(cfg *Config) *Storage {
	nodes := make([]*storageNode, len(cfg.Addrs))
	for i, addr := range cfg.Addrs {
		nodes[i] = &storageNode{
			addr:   addr,
			client: &http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
		}
	}
	return &Storage{
		nodes: nodes,
	}
}

// MustClose releases resources.
func (s *Storage) MustClose() {
	done := make(chan struct{})
	go func() {
		s.MustForceFlush()
		close(done)
	}()
	select {
	case <-done:
		// close cleanly
	case <-time.After(5 * time.Second):
		log.Printf("netselect close timeout after 5s; exiting anyway")
	}
}

// Search runs q on every storage node in parallel and merges the results.
//
// The shape:
//
//  1. marshal q once; POST it to all N nodes concurrently
//  2. each node returns rows sorted by (StreamID, Timestamp) plus its stats
//  3. k-way merge the N sorted streams into one sorted stream
//  4. Merge() the N SearchStats, then set NodesQueried / NodesFailed /
//     BytesReceivedFromNodes on the result
//  5. apply q.Limit again (stage 10)
//
// Step 3 is code you already have. minilog/merger.go performs a k-way merge
// over parts using a heap on (streamID, timestamp); N node responses are N
// sorted streams with the same ordering. If the existing merger does not
// factor out cleanly, understand exactly why before you write a second one --
// "the block merger merges blocks and this merges rows" is a real answer, and
// noticing that is worth more than the reuse would have been.
//
// Step 4 is also code you already have: SearchStats.Merge exists and
// Storage.Search already uses it to fold per-shard stats. This is the same
// call one level up, which is a good sign that the stats model was right.
//
// On failure the behaviour depends on q.AllowPartialResponse -- see stage 9
// and handleNodeError below. Whatever you do, the returned SearchStats must
// tell the truth: NodesFailed > 0 means IsPartial() is true, and no caller
// should ever be able to mistake a partial answer for a complete one.
func (s *Storage) Search(q *minilog.Query) ([]minilog.Row, *minilog.SearchStats) {
	rowss := make([][]minilog.Row, len(s.nodes))
	sss := make([]*minilog.SearchStats, len(s.nodes))
	errs := make([]error, len(s.nodes))
	var wg sync.WaitGroup
	for i, n := range s.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rowss[i], sss[i], errs[i] = n.runQuery(q)
		}()
	}
	wg.Wait()
	failedNodes, err := resolveErrors(errs, q.AllowPartialResponse)
	if err != nil {
		log.Printf("search failed err:%v", err)
		return nil, &minilog.SearchStats{
			NodesQueried: len(s.nodes),
			NodesFailed:  failedNodes,
		}
	} else if failedNodes > 0 {
		log.Printf("search degraded: %d of %d nodes failed:%v", failedNodes, len(s.nodes), err)
	}
	rows := mergeSortedRows(rowss)
	rows = q.ApplyLimit(rows)
	ss := &minilog.SearchStats{}
	for _, stat := range sss {
		if stat == nil {
			continue
		}
		ss.Merge(stat)
	}
	ss.NodesQueried += len(s.nodes)
	ss.NodesFailed += failedNodes
	return rows, ss
}

// MustForceFlush calls ForceFlushPath on every node.
//
// A select node has nothing of its own to flush; this exists so that
// netstorage.Storage can satisfy minilog.LogStorage, and so the harness has
// one call that makes everything queryable.
func (s *Storage) MustForceFlush() {
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

// runQuery POSTs q to one node and returns its rows and stats.
//
// Count the response body bytes into the returned stats'
// BytesReceivedFromNodes. That counter is the entire measurement in stage 10,
// and counting it at the socket -- before decompression -- is the only place
// that gives the number a caller actually cares about.
func (sn *storageNode) runQuery(q *minilog.Query) ([]minilog.Row, *minilog.SearchStats, error) {
	var resp *http.Response
	var err error
	var b []byte
	var rows []minilog.Row
	var ss *minilog.SearchStats
	queryToSend := internalapi.MarshalQuery(nil, q)
	resp, err = sn.client.Post(fmt.Sprintf("http://%s%s?version=%s", sn.addr, internalapi.SelectPath, internalapi.ProtocolVersion),
		"application/octet-stream",
		bytes.NewReader(queryToSend))
	if err != nil {
		return nil, nil, &errUnavailable{err: err}
	}
	defer resp.Body.Close()
	b, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, nil, fmt.Errorf("search failed statuscode %d:%s", resp.StatusCode, b)
	}
	rows, ss, err = internalapi.UnmarshalQueryResponse(b)
	if err != nil {
		return nil, nil, err
	}
	ss.BytesReceivedFromNodes = int64(len(b))
	return rows, ss, nil
}

// mergeSortedRows merges N already-sorted row streams into one sorted stream.
//
// Ordering is (StreamID, Timestamp), matching minilog.SortRows.
//
// Do not concatenate and re-sort. It gives the right answer and it is the
// wrong algorithm: it is O(total log total) with the whole result resident,
// where a heap merge is O(total log N) streaming. Stage 8's scaling
// measurement predicts where the merge becomes the bottleneck from its
// measured throughput, so a sort here does not just cost time -- it moves the
// knee you are trying to find and makes the prediction wrong.
//
// Ties: two nodes can hold rows with identical (StreamID, Timestamp), because
// nothing in this system says a row is unique. Both must survive. A merge that
// advances both readers on a tie drops one, and a full scan will still look
// almost right. minilog/merger.go's correctness harness exists because of this
// exact bug class one level down.
func mergeSortedRows(streams [][]minilog.Row) []minilog.Row {
	idxs := make([]int, len(streams))
	var rows []minilog.Row
	for {
		var small minilog.Row
		sIdx := -1
		for i := range streams {
			if len(streams[i]) <= idxs[i] {
				continue
			}
			if sIdx == -1 || streams[i][idxs[i]].StreamID < small.StreamID || (streams[i][idxs[i]].StreamID == small.StreamID && streams[i][idxs[i]].Timestamp < small.Timestamp) {
				small = streams[i][idxs[i]]
				sIdx = i
			}
		}
		if sIdx == -1 {
			break
		}
		rows = append(rows, small)
		idxs[sIdx]++
	}
	return rows
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

// errUnavailable marks a node that did not respond at all, as opposed to one
// that responded with an error.
//
// This distinction is the whole of stage 9's select-side design, so give it a
// type rather than string-matching an error message later.
type errUnavailable struct {
	err error
}

// Error implements error.
func (e *errUnavailable) Error() string {
	return e.err.Error()
}

// isUnavailable reports whether err means "the node did not answer".
//
// Transport failures only: connection refused, reset, EOF, timeout, DNS. An
// HTTP response -- any HTTP response, including 500 -- means the node answered
// and is therefore available.
func isUnavailable(err error) bool {
	var eu *errUnavailable
	return errors.As(err, &eu)
}

// resolveErrors decides what a set of per-node outcomes means for the query.
//
// The policy, which is worth reading getFirstError in the real repo for after
// you have written your own:
//
//	allowPartial == false
//	    any error at all fails the whole query. A complete answer or none.
//
//	allowPartial == true
//	    unavailable nodes are tolerated -- that is what the flag is for.
//	    An error RESPONSE from a reachable node is NOT tolerated, even under
//	    partial. It almost always means misconfiguration or a version skew,
//	    and hiding it behind a degraded-but-successful query is how a cluster
//	    becomes undebuggable. Surface it.
//	    All nodes unavailable fails: an empty result from an empty cluster is
//	    indistinguishable from an empty result from a healthy one.
//
// Returns the number of failed nodes and, if the query cannot be served, an
// error explaining which node and why.
func resolveErrors(errs []error, allowPartial bool) (failed int, err error) {
	failedCount := 0
	for i := range errs {
		if errs[i] != nil {
			failedCount++
			if allowPartial {
				if isUnavailable(errs[i]) {
					continue
				}
			}
			return failedCount, fmt.Errorf("search error: %w", errs[i])
		}
	}
	if len(errs) > 0 && len(errs) == failedCount {
		return failedCount, fmt.Errorf("all %d nodes unavailable", len(errs))
	}
	return failedCount, nil
}
