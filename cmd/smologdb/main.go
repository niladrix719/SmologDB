// Command smologdb runs a storage node, a router or an agent.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/niladrix719/smologdb/agent"
	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/netstorage"
	"github.com/niladrix719/smologdb/storage"
)

const usage = `usage:
  smologdb storage -data DIR [-listen ADDR] [-retention-days N]
  smologdb router  -nodes ADDR,ADDR [-listen ADDR]
  smologdb agent   -send-to ADDR[,ADDR] -dir DIR [-tail FILE,FILE] [-listen ADDR]
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:9000", "HTTP listen address")
	switch os.Args[1] {
	case "storage":
		data := fs.String("data", "", "data directory")
		retention := fs.Int("retention-days", 0, "keep today and the days before it, this many days in all; 0 keeps everything")
		fs.Parse(os.Args[2:])
		if *data == "" {
			log.Fatal("-data is required")
		}
		if *retention < 0 {
			log.Fatal("-retention-days must be 0 or more")
		}
		runStorageNode(*data, *listen, *retention)
	case "router":
		nodes := fs.String("nodes", "", "comma-separated storage node addresses")
		fs.Parse(os.Args[2:])
		if *nodes == "" {
			log.Fatal("-nodes is required")
		}
		runRouter(strings.Split(*nodes, ","), *listen)
	case "agent":
		sendTo := fs.String("send-to", "", "comma-separated router addresses; every block goes to each")
		dir := fs.String("dir", "", "directory for the send queues and tail checkpoints")
		tail := fs.String("tail", "", "comma-separated log files to follow")
		fs.Parse(os.Args[2:])
		if *sendTo == "" || *dir == "" {
			log.Fatal("-send-to and -dir are required")
		}
		runAgent(strings.Split(*sendTo, ","), *dir, *tail, *listen)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}

func runAgent(destinations []string, dir, tailPaths, listenAddr string) {
	ag := agent.NewAgent(&agent.Config{
		Addrs:    destinations,
		DataPath: dir,
	})
	srv := &http.Server{Addr: listenAddr, Handler: withAdmin(agent.NewReceiver(ag, nil))}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("agent http server stopped: %v", err)
		}
	}()
	var tl *agent.Tailer
	if tailPaths != "" {
		tl = agent.NewTailer(&agent.TailerConfig{
			Paths:          strings.Split(tailPaths, ","),
			CheckpointPath: filepath.Join(dir, "checkpoints.json"),
		}, ag)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Stop HTTP, then the tailer, then the agent, so each one's last rows go into something still open.
	srv.Shutdown(ctx)
	if tl != nil {
		tl.MustClose()
	}
	ag.MustClose()
}

func runStorageNode(dataPath, listenAddr string, retentionDays int) {
	s := storage.MustOpenStorage(dataPath, &storage.Config{FlushInterval: 5 * time.Second, RetentionDays: retentionDays})
	handler := internalapi.NewStorageNodeHandler(s)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		err := http.ListenAndServe(listenAddr, withAdmin(handler))
		if err != nil {
			log.Printf("storage http server stopped: %v", err)
		}
	}()
	<-sigCh
	s.MustClose()
}

type routerHandler struct {
	s *netstorage.Storage
}

func (n *routerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var q storage.Query
	var rows []storage.Row
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
			Rows  []storage.Row        `json:"rows"`
			Stats *storage.SearchStats `json:"stats"`
		}{Rows: rows, Stats: ss}
		resBytes, err = json.Marshal(resp)
		if err != nil {
			failedText := "failed to marshal response"
			http.Error(w, failedText, http.StatusBadRequest)
			return
		}
		_, err = w.Write(resBytes)
		if err != nil {
			log.Printf("failed to send response: %v", err)
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

func runRouter(nodeAddrs []string, listenAddr string) {
	s := netstorage.NewStorage(&netstorage.Config{Addrs: nodeAddrs})
	handler := &routerHandler{s: s}
	mux := http.NewServeMux()
	mux.Handle("/internal/", internalapi.NewStorageNodeHandler(s))
	mux.Handle("/select", newSelectHandler(s))
	mux.Handle("/", handler)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		err := http.ListenAndServe(listenAddr, withAdmin(mux))
		if err != nil {
			log.Printf("router http server stopped: %v", err)
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
	case <-time.After(5 * time.Second):
		log.Printf("router close timed out after 5s; exiting anyway")
	}
}

// newSelectHandler serves GET /select?q=...&limit=N&since=DURATION as newline-delimited
// JSON, one row per line, newest first.
// TODO(#10): not implemented yet. See TODO.md for the parameters and the output format.
func newSelectHandler(s storage.LogStorage) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "/select is not implemented yet", http.StatusNotImplemented)
	})
}

// withAdmin adds GET /health and GET /metrics in front of a role's own handler.
// TODO(#12): not implemented yet; it passes every request straight to next.
func withAdmin(next http.Handler) http.Handler {
	return next
}
