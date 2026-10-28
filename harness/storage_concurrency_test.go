package harness

// The local engine is shared by every goroutine that serves a request: a
// storage node handles each HTTP insert and each search on its own goroutine.
// So Storage must be safe for concurrent MustAddRows, Search and
// MustForceFlush. Run with -race; without it a lost row still fails the count.

import (
	"math"
	"sync"
	"testing"

	"github.com/niladrix719/minilog/minilog"
)

func TestStorageConcurrentAddSearchFlush(t *testing.T) {
	st := minilog.MustOpenStorage(t.TempDir(), &minilog.Config{ShardsCount: 4})
	defer st.MustClose()

	const (
		writers = 8
		batches = 20
		perBat  = 100
		day     = int64(86_400_000_000_000)
		base    = int64(1_700_000_000_000_000_000)
	)
	q := &minilog.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}

	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() { // flushes and searches the whole time the writers run
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				st.MustForceFlush()
				st.Search(q)
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				rows := make([]minilog.Row, perBat)
				for i := range rows {
					rows[i] = minilog.Row{
						// three different days, so partitions are being CREATED
						// (a map write) while the others iterate the map
						Timestamp: base + int64(i%3)*day + int64(w*batches*perBat+b*perBat+i),
						StreamID:  uint64(w),
						Fields:    []minilog.Field{{Name: "_msg", Value: "x"}},
					}
				}
				st.MustAddRows(rows)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	bg.Wait()

	st.MustForceFlush()
	rows, _ := st.Search(q)
	if want := writers * batches * perBat; len(rows) != want {
		t.Fatalf("%d rows stored, want %d: rows were lost to a race between MustAddRows and a flush", len(rows), want)
	}
}
