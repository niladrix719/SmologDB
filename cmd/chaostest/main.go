// Command chaostest kills storage nodes during a distributed ingest and reports what survived.
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/storage"
)

var (
	iters    = flag.Int("iters", 200, "number of kill/restart events")
	nodes    = flag.Int("nodes", 4, "number of storage nodes")
	rows     = flag.Int("rows", 600_000, "rows to ingest")
	seed     = flag.Int64("seed", 1, "dataset seed")
	minDelay = flag.Duration("min-delay", 3*time.Millisecond, "minimum time between kills")
	maxDelay = flag.Duration("max-delay", 60*time.Millisecond, "maximum time between kills")
	restart  = flag.Duration("restart-delay", 30*time.Millisecond, "how long a node stays dead")

	child  = flag.Bool("child", false, "internal: run as a storage node")
	dir    = flag.String("dir", "", "internal: storage node data directory")
	listen = flag.String("listen", "", "internal: storage node listen address")
)

func main() {
	flag.Parse()
	if *child {
		runChild()
		return
	}
	runParent()
}

func runChild() {
	if *dir == "" || *listen == "" {
		fatal("child: -dir and -listen are required")
	}
	s := storage.MustOpenStorage(*dir, &storage.Config{ShardsCount: 1})

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fatal("child: listen %s: %v", *listen, err)
	}

	srv := &http.Server{
		Handler:           internalapi.NewStorageNodeHandler(s),
		ReadHeaderTimeout: 5 * time.Second,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		_ = srv.Close()
		s.MustClose()
		os.Exit(0)
	}()

	_ = srv.Serve(ln)
	select {}
}

type node struct {
	idx  int
	dir  string
	addr string
	cmd  *exec.Cmd
}

func runParent() {
	exe, err := os.Executable()
	if err != nil {
		fatal("cannot find own executable: %v", err)
	}

	root, err := os.MkdirTemp("", "smologdb-chaostest-")
	if err != nil {
		fatal("mkdtemp: %v", err)
	}
	defer os.RemoveAll(root)

	fmt.Printf("chaostest: %d nodes, %d rows, %d kill events, in %s\n", *nodes, *rows, *iters, root)

	ns := make([]*node, *nodes)
	for i := range ns {
		ns[i] = &node{
			idx:  i,
			dir:  filepath.Join(root, fmt.Sprintf("node-%02d", i)),
			addr: reserveAddr(),
		}
		if err := os.MkdirAll(ns[i].dir, 0o755); err != nil {
			fatal("mkdir: %v", err)
		}
		startNode(exe, ns[i])
	}
	defer func() {
		for _, n := range ns {
			stopNode(n, syscall.SIGKILL)
		}
	}()

	addrs := make([]string, len(ns))
	for i, n := range ns {
		addrs[i] = n.addr
	}

	cfg := gen.DefaultConfig()
	cfg.Rows = *rows
	cfg.Seed = *seed
	ds := gen.Generate(cfg)

	ins := netinsert.NewStorage(&netinsert.Config{
		Addrs:               addrs,
		Routing:             netinsert.RoutingHybrid,
		NodeDisableDuration: 100 * time.Millisecond,
		MaxInsertBlockSize:  256 << 10,
	})

	ackPath := filepath.Join(root, "acked.log")
	ackFile, err := os.OpenFile(ackPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fatal("open acked.log: %v", err)
	}

	var mu sync.Mutex
	killedSinceFlush := false
	stop := make(chan struct{})
	var chaosWG sync.WaitGroup
	var killEvents int

	chaosWG.Go(func() {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for killEvents < *iters {
			select {
			case <-stop:
				return
			case <-time.After(*minDelay + time.Duration(rng.Int63n(int64(*maxDelay-*minDelay)))):
			}

			i := rng.Intn(len(ns))
			mu.Lock()
			killedSinceFlush = true
			mu.Unlock()

			stopNode(ns[i], syscall.SIGKILL)
			time.Sleep(*restart)
			startNode(exe, ns[i])

			killEvents++
			if killEvents%25 == 0 {
				fmt.Printf("  %d/%d kills\n", killEvents, *iters)
			}
		}
	})

	const batch = 2000
	var ackedTotal, ackedSafe int
	for off := 0; off < len(ds.Rows); off += batch {
		end := off + batch
		if end > len(ds.Rows) {
			end = len(ds.Rows)
		}

		mu.Lock()
		killedSinceFlush = false
		mu.Unlock()

		ins.MustAddRows(ds.Rows[off:end])
		ins.MustForceFlush()

		ackedTotal = end
		mu.Lock()
		clean := !killedSinceFlush
		mu.Unlock()
		if clean {

			ackedSafe = end
		}
		fmt.Fprintf(ackFile, "%d %d\n", ackedTotal, ackedSafe)
		_ = ackFile.Sync()
	}

	close(stop)
	chaosWG.Wait()

	for _, n := range ns {
		if n.cmd == nil {
			startNode(exe, n)
		}
	}
	ins.MustForceFlush()
	ins.MustClose()
	_ = ackFile.Close()

	for _, n := range ns {
		stopNode(n, syscall.SIGKILL)
	}

	found, dupes := verify(ns)

	lostVsTotal := 0.0
	if ackedTotal > 0 {
		lost := ackedTotal - found
		if lost < 0 {
			lost = 0
		}
		lostVsTotal = float64(lost) / float64(ackedTotal) * 100
	}

	fmt.Printf("\n")
	if found < ackedSafe {
		fatal("DATA LOSS. %d rows were acknowledged by a flush during which every node\n"+
			"stayed alive, but only %d survived.\n\n"+
			"This is the hard invariant and it does not depend on timing. A flush that\n"+
			"completed with no node dying must leave every row durable somewhere.\n"+
			"Check, in order:\n"+
			"  - MustForceFlush drains this process's send buffers BEFORE asking any\n"+
			"    node to flush. The other order publishes nodes that have not yet\n"+
			"    received the data.\n"+
			"  - a failed send re-routes rather than drops, and keeps retrying while\n"+
			"    any node is up.\n"+
			"  - MustForceFlush does not return while a re-route is still pending.\n\n"+
			"Data kept for inspection: %s", ackedSafe, found, ns[0].dir)
	}
	if dupes > 0 {
		fatal("DUPLICATE ROWS: %d.\n\n"+
			"Some duplication is inherent to re-routing -- a send that timed out may\n"+
			"still have been applied, so re-routing is at-least-once. But %d is worth\n"+
			"investigating: re-route only after a send has definitively FAILED, never\n"+
			"speculatively, and never re-route a buffer you also left queued on the\n"+
			"original node.", dupes, dupes)
	}

	fmt.Printf("chaostest PASSED\n")
	fmt.Printf("  nodes:                 %d\n", len(ns))
	fmt.Printf("  kill events:           %d\n", killEvents)
	fmt.Printf("  rows acknowledged:     %d\n", ackedTotal)
	fmt.Printf("  of those, kill-free:   %d   <- the hard invariant\n", ackedSafe)
	fmt.Printf("  rows found on disk:    %d\n", found)
	fmt.Printf("\n")
	fmt.Printf("  lost on kill:          %.3f%% (rows still in a killed node's memory)\n", lostVsTotal)
}

func verify(ns []*node) (found, dupes int) {
	seen := make(map[string]struct{})
	for _, n := range ns {
		func() {
			defer func() {
				if r := recover(); r != nil {
					fatal("PANIC reopening %s after a kill: %v\n\n"+
						"A storage node must recover from SIGKILL at any point, exactly as in\n"+
						"cmd/crashtest -- being part of a cluster changes nothing about that.\n"+
						"Half-written part directories and parts not named in parts.json must\n"+
						"be cleaned up by mustOpenPartition, not tripped over.", n.dir, r)
				}
			}()
			s := storage.MustOpenStorage(n.dir, &storage.Config{ShardsCount: 1})
			defer s.MustClose()

			rows, _ := s.Search(&storage.Query{
				MinTimestamp: math.MinInt64,
				MaxTimestamp: math.MaxInt64,
			})
			for i := range rows {
				key := rowKey(&rows[i])
				if _, dup := seen[key]; dup {
					dupes++
					continue
				}
				seen[key] = struct{}{}
				found++
			}
		}()
	}
	return found, dupes
}

func rowKey(r *storage.Row) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d|%d", r.StreamID, r.Timestamp)
	for i := range r.Fields {
		sb.WriteByte('|')
		sb.WriteString(r.Fields[i].Name)
		sb.WriteByte('=')
		sb.WriteString(r.Fields[i].Value)
	}
	return sb.String()
}

func reserveAddr() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatal("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func startNode(exe string, n *node) {
	cmd := exec.Command(exe, "-child", "-dir", n.dir, "-listen", n.addr)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fatal("start node %d: %v", n.idx, err)
	}
	n.cmd = cmd
	go func() { _ = cmd.Wait() }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", n.addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	fatal("node %d never started listening on %s", n.idx, n.addr)
}

func stopNode(n *node, sig syscall.Signal) {
	if n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Signal(sig)
	n.cmd = nil

	time.Sleep(2 * time.Millisecond)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\nFAIL: "+format+"\n", args...)
	os.Exit(1)
}
