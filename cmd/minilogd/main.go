// Command minilogd is the one binary. It is a storage node, an insert+select
// node, or (stage 12) an agent, depending on which flags were passed.
//
// ---------------------------------------------------------------------------
// STAGE 6 -- YOUR IMPLEMENTATION GOES HERE.
//
// Verified by:  go run ./cmd/chaostest    (which spawns these)
//
//	and by hand, per the examples below.
//
// Reference: app/vlstorage/main.go.
// ---------------------------------------------------------------------------
//
// Storage node -- owns data, answers queries about its own shard:
//
//	./minilogd -storageDataPath=./data-1 -httpListenAddr=127.0.0.1:9001
//	./minilogd -storageDataPath=./data-2 -httpListenAddr=127.0.0.1:9002
//	./minilogd -storageDataPath=./data-3 -httpListenAddr=127.0.0.1:9003
//
// Insert+select node -- owns nothing, routes and fans out:
//
//	./minilogd -storageNode=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
//	           -httpListenAddr=127.0.0.1:9000
//
// Agent -- STAGE 12. Owns no data and no routing: accepts logs, queues them on
// disk, and delivers every block to every destination:
//
//	./minilogd -remoteWrite.addrs=127.0.0.1:9000 -tmpDataPath=./agent-data \
//	           -httpListenAddr=127.0.0.1:9429 -tail=/var/log/app.log
//
// One flag decides the role, and the same executable serves all three. That is not
// a packaging convenience -- it is what makes "migrate a single node into a
// cluster" a config change rather than a data migration. A running single node
// becomes a storage node the moment somebody points a -storageNode list at it,
// with its existing data intact and immediately queryable through the new
// select node.
//
// Note what is NOT here: no cluster identity, no join protocol, no node
// registry, no health-check endpoint that peers consult. A storage node does
// not know it is in a cluster and cannot find out.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/niladrix719/minilog/internalapi"
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netstorage"
)

func main() {
	storageNode := flag.String("storageNode", "", "storage node addr list")
	httpListenAddr := flag.String("httpListenAddr", "127.0.0.1:9000", "server addr")
	storageDataPath := flag.String("storageDataPath", "", "addr of the storage")
	// STAGE 12 -- agent mode.
	remoteWrite := flag.String("remoteWrite.addrs", "", "agent mode: comma-separated destinations; every block goes to every one")
	tmpDataPath := flag.String("tmpDataPath", "", "agent mode: directory for the on-disk queues and tail checkpoints")
	tail := flag.String("tail", "", "agent mode: comma-separated log files to follow")
	flag.Parse()
	if *remoteWrite != "" {
		runAgent(strings.Split(*remoteWrite, ","), *tmpDataPath, *tail, *httpListenAddr)
		return
	}
	if *storageNode != "" {
		storageNodesList := strings.Split(*storageNode, ",")
		runInsertSelectNode(storageNodesList, *httpListenAddr)
	} else {
		runStorageNode(*storageDataPath, *httpListenAddr)
	}
}

// runStorageNode opens a local minilog.Storage at -storageDataPath and serves
// internalapi.NewStorageNodeHandler over -httpListenAddr.
//
// Handle SIGINT/SIGTERM: MustClose must run, or every unflushed in-memory part
// is lost. cmd/crashtest already taught you what an un-closed storage costs;
// this is the version of that lesson you control.
//
// Do NOT try to make SIGKILL survivable here. It is not, by design -- that is
// what cmd/crashtest measures and what a WAL would change.
// runAgent runs the process as an agent. STAGE 12.
//
// Build, in this order:
//
//  1. agent.NewAgent with Addrs = destinations and DataPath = tmpDataPath.
//  2. agent.NewReceiver(ag, nil) served on listenAddr, so `curl` works.
//  3. if tailPaths != "": agent.NewTailer with Paths = the split list and
//     CheckpointPath = <tmpDataPath>/checkpoints.json.
//  4. wait for SIGINT/SIGTERM, then shut down in the order that loses nothing:
//     stop the HTTP server first (no new rows), then tailer.MustClose (its last
//     poll hands rows to the agent), then ag.MustClose (flush, stop senders).
//     Close them the other way round and the tailer's final rows go into an
//     agent that has already closed.
//
// Note what is absent: no "wait until drained" on shutdown. The backlog stays on
// disk and is sent after the next start. That is what vlagent does too.
func runAgent(destinations []string, tmpDataPath, tailPaths, listenAddr string) {
	panic("TODO stage 12: implement runAgent")
}

func runStorageNode(dataPath, listenAddr string) {
	s := minilog.MustOpenStorage(dataPath, &minilog.Config{ShardsCount: 0, RetentionDays: 0})
	handler := internalapi.NewStorageNodeHandler(s)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		err := http.ListenAndServe(listenAddr, handler)
		if err != nil {
			log.Printf("storage node http server stopped %v", err)
		}
	}()
	<-sigCh
	s.MustClose()
}

type netStorageHandler struct {
	s *netstorage.Storage
}

func (n *netStorageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var q minilog.Query
	var rows []minilog.Row
	var bytes []byte
	var resBytes []byte
	var err error
	switch r.Method {
	case "GET":
		bytes, err = io.ReadAll(r.Body)
		if err != nil {
			failedText := "failed to read request"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		err = json.Unmarshal(bytes, &q)
		if err != nil {
			failedText := "failed to unmarshal query"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		rows, ss := n.s.Search(&q)
		resp := struct {
			Rows  []minilog.Row        `json:"rows"`
			Stats *minilog.SearchStats `json:"stats"`
		}{Rows: rows, Stats: ss}
		resBytes, err = json.Marshal(resp)
		if err != nil {
			failedText := "failed to marshal response"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		_, err = w.Write(resBytes)
		if err != nil {
			log.Printf("failed to send response %v", err)
		}
	case "POST":
		bytes, err = io.ReadAll(r.Body)
		if err != nil {
			failedText := "failed to read request"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		err = json.Unmarshal(bytes, &rows)
		if err != nil {
			failedText := "failed to unmarshal rows"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		n.s.MustAddRows(rows)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, r)
		return
	}
}

// runInsertSelectNode builds a netstorage.Storage over the -storageNode
// addresses and serves a public API over -httpListenAddr.
//
// The public API is yours to keep minimal -- minilog has no query language and
// no ingestion protocols, so a JSON POST for insert and a JSON GET for query
// is plenty. Nothing in the harness depends on it; it exists so you can drive
// a real cluster by hand and watch it work, which is worth doing once before
// you trust the tests.
//
// Do not forget MustClose on shutdown here either, and note that it means
// something different on this side: the local node's close flushes to disk,
// this one drains buffered rows to other machines. Bound it -- a single
// unresponsive storage node must not hang your shutdown.
func runInsertSelectNode(storageNodeAddrs []string, listenAddr string) {
	s := netstorage.NewStorage(&netstorage.Config{Addrs: storageNodeAddrs})
	handler := &netStorageHandler{s: s}
	// STAGE 12: an agent POSTs MarshalRowBatch bodies to internalapi.InsertPath
	// on this node, and right now only a storage node answers that, so the
	// path agent -> insert node -> storage nodes does not exist in the binary.
	// netStorageHandler ignores the path, so it cannot just be extended.
	// Route by path instead:
	//
	//	mux := http.NewServeMux()
	//	mux.Handle("/internal/", internalapi.NewStorageNodeHandler(s))
	//	mux.Handle("/", handler)
	//
	// internalapi.NewStorageNodeHandler serves any minilog.LogStorage, and
	// netstorage.Storage is one -- see newInsertNode in harness/stage12_test.go
	// for the same wiring under test. Then serve mux instead of handler below.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		err := http.ListenAndServe(listenAddr, handler)
		if err != nil {
			log.Printf("netstorage node http server stopped %v", err)
		}
	}()
	<-sigCh
	done := make(chan struct{})
	go func() {
		s.MustClose()
		close(done)
	}()
	select {
	case <-done:
		// close cleanly
	case <-time.After(5 * time.Second):
		log.Printf("netstorage close timeout after 5s; exiting anyway")
	}
}
