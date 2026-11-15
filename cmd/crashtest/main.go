// Command crashtest kills an ingesting process at random moments and checks
// that the storage recovers with no corruption and no loss of acknowledged
// data.
//
// Usage:
//
//	go run ./cmd/crashtest                 # 200 iterations
//	go run ./cmd/crashtest -iters 1000     # be mean about it
//
// How it works:
//
//	parent: spawns a child that ingests into a shared directory, waits a
//	        random interval, then SIGKILLs it. SIGKILL, not SIGTERM: no
//	        cleanup handlers run, no deferred Close, no flush. This is the
//	        moral equivalent of the process being OOM-killed mid-write.
//	child:  ingests rows in batches. After each MustForceFlush returns it
//	        appends the durable row count to acked.log and fsyncs it. That
//	        file is the contract: those rows were acknowledged as durable.
//	parent: reopens the storage and asserts
//	          (a) MustOpenStorage does not panic
//	          (b) a full scan returns at least the last acknowledged count
//	          (c) every returned row is well-formed and unique
//
// (b) is the durability property. (a) is the crash-recovery property: any
// half-written part directory or unreferenced part left behind by the kill
// must be cleaned up by mustOpenPartition, not tripped over.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/niladrix719/minilog/gen"
	"github.com/niladrix719/minilog/minilog"
)

var (
	iters    = flag.Int("iters", 200, "number of kill/recover iterations")
	child    = flag.Bool("child", false, "internal: run as the ingesting child")
	dir      = flag.String("dir", "", "storage directory")
	seed     = flag.Int64("seed", 1, "dataset seed")
	rows     = flag.Int("rows", 400_000, "rows the child tries to ingest")
	minDelay = flag.Duration("min-delay", 5*time.Millisecond, "minimum time before the kill")
	maxDelay = flag.Duration("max-delay", 900*time.Millisecond, "maximum time before the kill")
)

func main() {
	flag.Parse()
	if *child {
		runChild()
		return
	}
	runParent()
}

// ---------------------------------------------------------------------------
// child
// ---------------------------------------------------------------------------

func runChild() {
	if *dir == "" {
		fatal("child: -dir is required")
	}
	cfg := gen.DefaultConfig()
	cfg.Rows = *rows
	cfg.Seed = *seed
	ds := gen.Generate(cfg)

	s := minilog.MustOpenStorage(*dir, &minilog.Config{ShardsCount: 2})

	ackPath := filepath.Join(*dir, "acked.log")
	ackFile, err := os.OpenFile(ackPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fatal("child: open acked.log: %v", err)
	}

	const batch = 2000
	acked := 0
	for off := 0; off < len(ds.Rows); off += batch {
		end := off + batch
		if end > len(ds.Rows) {
			end = len(ds.Rows)
		}
		s.MustAddRows(ds.Rows[off:end])
		s.MustForceFlush()

		// Only now are these rows durable. Record it, durably.
		acked = end
		if _, err := fmt.Fprintf(ackFile, "%d\n", acked); err != nil {
			fatal("child: write acked.log: %v", err)
		}
		if err := ackFile.Sync(); err != nil {
			fatal("child: fsync acked.log: %v", err)
		}
	}

	s.MustClose()
	_ = ackFile.Close()
	os.Exit(0)
}

// ---------------------------------------------------------------------------
// parent
// ---------------------------------------------------------------------------

func runParent() {
	exe, err := os.Executable()
	if err != nil {
		fatal("cannot find own executable: %v", err)
	}

	root, err := os.MkdirTemp("", "minilog-crashtest-")
	if err != nil {
		fatal("mkdtemp: %v", err)
	}
	defer os.RemoveAll(root)

	fmt.Printf("crashtest: %d iterations in %s\n", *iters, root)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var (
		killed     int
		completed  int
		recovered  int
		maxLossPct float64
	)

	for i := 0; i < *iters; i++ {
		storeDir := filepath.Join(root, fmt.Sprintf("run-%04d", i))
		if err := os.MkdirAll(storeDir, 0o755); err != nil {
			fatal("mkdir %s: %v", storeDir, err)
		}

		cmd := exec.Command(exe,
			"-child",
			"-dir", storeDir,
			"-seed", strconv.FormatInt(int64(i), 10),
			"-rows", strconv.Itoa(*rows),
		)
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fatal("iter %d: start child: %v", i, err)
		}

		delay := *minDelay + time.Duration(rng.Int63n(int64(*maxDelay-*minDelay)))
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()

		select {
		case <-done:
			completed++
		case <-time.After(delay):
			// SIGKILL: no handlers, no flush, no mercy.
			_ = cmd.Process.Signal(syscall.SIGKILL)
			<-done
			killed++
		}

		acked := readAcked(filepath.Join(storeDir, "acked.log"))
		found, lossPct := verify(i, storeDir, acked, int64(i))
		recovered++
		if lossPct > maxLossPct {
			maxLossPct = lossPct
		}

		if (i+1)%20 == 0 {
			fmt.Printf("  iter %4d/%d  killed=%d completed=%d  last: acked=%d found=%d\n",
				i+1, *iters, killed, completed, acked, found)
		}
		os.RemoveAll(storeDir)
	}

	fmt.Printf("\ncrashtest PASSED\n")
	fmt.Printf("  iterations:        %d\n", *iters)
	fmt.Printf("  killed mid-write:  %d\n", killed)
	fmt.Printf("  ran to completion: %d\n", completed)
	fmt.Printf("  recovered:         %d\n", recovered)
	fmt.Printf("\nEvery acknowledged row survived every kill.\n")
	fmt.Printf("That is the durability property. Note what it does NOT cover:\n")
	fmt.Printf("  - power loss (fsync does not flush the drive cache on macOS;\n")
	fmt.Printf("    F_FULLFSYNC does). SIGKILL only tests process death.\n")
	fmt.Printf("  - torn writes at the sector level.\n")
	fmt.Printf("  - unacknowledged rows, which are correctly allowed to vanish --\n")
	fmt.Printf("    a write-ahead log is what would save those, and this exercise\n")
	fmt.Printf("    deliberately has none.\n")
}

// verify reopens the storage and checks the recovery invariants.
func verify(iter int, storeDir string, acked int, dsSeed int64) (int, float64) {
	defer func() {
		if r := recover(); r != nil {
			fatal("iter %d: PANIC reopening storage after a kill: %v\n\n"+
				"This is the crash-recovery bug the test exists to find. Check\n"+
				"mustOpenPartition: it must delete part directories that are not\n"+
				"named in parts.json, and any leftover *.tmp directories, instead\n"+
				"of trying to open them. Storage dir kept for inspection: %s",
				iter, r, storeDir)
		}
	}()

	s := minilog.MustOpenStorage(storeDir, &minilog.Config{ShardsCount: 2})
	defer s.MustClose()

	got, _ := s.Search(&minilog.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
	})

	if len(got) < acked {
		fatal("iter %d: DATA LOSS. acked.log says %d rows were durably flushed, "+
			"but only %d survived the crash.\n\n"+
			"An acknowledged write must never disappear. Check the publication\n"+
			"order in mustFlushInmemoryPart: the part directory must be fsynced\n"+
			"and renamed BEFORE parts.json names it, and parts.json must be\n"+
			"written to a temp file, fsynced, renamed, and its parent directory\n"+
			"fsynced. Storage dir kept for inspection: %s",
			iter, acked, len(got), storeDir)
	}

	// No duplicates: a merge that published before removing its sources, then
	// crashed, must not leave both live.
	seen := make(map[string]struct{}, len(got))
	for i := range got {
		key := fmt.Sprintf("%d|%d|%s", got[i].StreamID, got[i].Timestamp, fieldsKey(got[i].Fields))
		if _, dup := seen[key]; dup {
			fatal("iter %d: DUPLICATE ROW after recovery.\n\n"+
				"A merge published its output and crashed before parts.json removed\n"+
				"the sources, so both are live. parts.json must be updated in ONE\n"+
				"atomic write that both adds the new part and removes the old ones.\n"+
				"Storage dir kept for inspection: %s", iter, storeDir)
		}
		seen[key] = struct{}{}

		if len(got[i].Fields) == 0 {
			fatal("iter %d: recovered a row with no fields -- corrupt block decode. Dir: %s", iter, storeDir)
		}
	}

	lossPct := 0.0
	if acked > 0 {
		lossPct = float64(acked-min(len(got), acked)) / float64(acked) * 100
	}
	return len(got), lossPct
}

func fieldsKey(fields []minilog.Field) string {
	var sb strings.Builder
	for _, f := range fields {
		sb.WriteString(f.Name)
		sb.WriteByte('=')
		sb.WriteString(f.Value)
		sb.WriteByte(0)
	}
	return sb.String()
}

func readAcked(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0 // killed before the first flush completed
	}
	defer f.Close()

	last := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			// A torn final line is expected -- the process was killed
			// mid-write. Ignore it and keep the last complete one.
			continue
		}
		last = n
	}
	return last
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\nFAIL: "+format+"\n", args...)
	os.Exit(1)
}
