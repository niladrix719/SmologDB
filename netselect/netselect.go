// Package netselect sends a query to every storage node and merges the sorted results.
package netselect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/storage"
)

type Config struct {
	Addrs []string

	DisableCompression bool

	QueryTimeout time.Duration
}

type Storage struct {
	nodes []*storageNode
}

type storageNode struct {
	addr    string
	client  *http.Client
	timeout time.Duration
}

func NewStorage(cfg *Config) *Storage {
	nodes := make([]*storageNode, len(cfg.Addrs))
	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	for i, addr := range cfg.Addrs {
		nodes[i] = &storageNode{
			addr:    addr,
			client:  &http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
			timeout: timeout,
		}
	}
	return &Storage{
		nodes: nodes,
	}
}

func (s *Storage) MustClose() {
	done := make(chan struct{})
	go func() {
		s.MustForceFlush()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("netselect close timeout after 5s; exiting anyway")
	}
}

func (s *Storage) Search(q *storage.Query) ([]storage.Row, *storage.SearchStats) {
	rowss := make([][]storage.Row, len(s.nodes))
	sss := make([]*storage.SearchStats, len(s.nodes))
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
		return nil, &storage.SearchStats{
			NodesQueried: len(s.nodes),
			NodesFailed:  failedNodes,
		}
	} else if failedNodes > 0 {
		log.Printf("search degraded: %d of %d nodes failed:%v", failedNodes, len(s.nodes), err)
	}
	// Each node already cut its sorted rows to Limit, so the first Limit rows of the merge are exact.
	rows := mergeSortedRows(rowss, q.Newest)
	rows = q.ApplyLimit(rows)
	ss := &storage.SearchStats{}
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

func (sn *storageNode) runQuery(q *storage.Query) ([]storage.Row, *storage.SearchStats, error) {
	var resp *http.Response
	var req *http.Request
	var err error
	var b []byte
	var rows []storage.Row
	var ss *storage.SearchStats
	ctx, cancel := context.WithTimeout(context.Background(), sn.timeout)
	defer cancel()
	queryToSend := internalapi.MarshalQuery(nil, q)
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s%s?version=%s", sn.addr, internalapi.SelectPath, internalapi.ProtocolVersion),
		bytes.NewReader(queryToSend))
	if err != nil {
		return nil, nil, &errUnavailable{err: err}
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err = sn.client.Do(req)
	if err != nil {
		return nil, nil, &errUnavailable{err: err}
	}
	defer resp.Body.Close()
	b, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, &errUnavailable{err: err}
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

func mergeSortedRows(streams [][]storage.Row, newest bool) []storage.Row {
	idxs := make([]int, len(streams))
	var rows []storage.Row
	for {
		var small storage.Row
		sIdx := -1
		for i := range streams {
			if len(streams[i]) <= idxs[i] {
				continue
			}
			var check bool
			if newest {
				check = streams[i][idxs[i]].Timestamp > small.Timestamp || (streams[i][idxs[i]].Timestamp == small.Timestamp && streams[i][idxs[i]].StreamID < small.StreamID)
			} else {
				check = streams[i][idxs[i]].StreamID < small.StreamID || (streams[i][idxs[i]].StreamID == small.StreamID && streams[i][idxs[i]].Timestamp < small.Timestamp)
			}
			if sIdx == -1 || check {
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

type errUnavailable struct {
	err error
}

func (e *errUnavailable) Error() string {
	return e.err.Error()
}

func isUnavailable(err error) bool {
	var eu *errUnavailable
	return errors.As(err, &eu)
}

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
