package storage_test

import (
	"math"
	"sync"
	"testing"

	"github.com/niladrix719/smologdb/storage"
)

func TestStorageConcurrentAddSearchFlush(t *testing.T) {
	st := storage.MustOpenStorage(t.TempDir(), &storage.Config{ShardsCount: 4})
	defer st.MustClose()

	const (
		writers = 8
		batches = 20
		perBat  = 100
		day     = int64(86_400_000_000_000)
		base    = int64(1_700_000_000_000_000_000)
	)
	q := &storage.Query{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64}

	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
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
				rows := make([]storage.Row, perBat)
				for i := range rows {
					rows[i] = storage.Row{

						Timestamp: base + int64(i%3)*day + int64(w*batches*perBat+b*perBat+i),
						StreamID:  uint64(w),
						Fields:    []storage.Field{{Name: "_msg", Value: "x"}},
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
