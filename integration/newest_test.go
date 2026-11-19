package integration

import (
	"fmt"
	"math"
	"testing"

	"github.com/niladrix719/smologdb/netstorage"
	"github.com/niladrix719/smologdb/storage"
)

func TestNewestLimitAcrossNodes(t *testing.T) {
	ns := newNodeSet(t, 3)
	c := ns.cluster(netstorage.Config{})
	var rows []storage.Row
	for i := 1; i <= 600; i++ {
		rows = append(rows, testRow(uint64(i%30+1), int64(i)*1e9, "_msg", fmt.Sprintf("line %d", i)))
	}
	c.MustAddRows(rows)
	c.MustForceFlush()

	got, ss := c.Search(&storage.Query{MaxTimestamp: math.MaxInt64, Limit: 7, Newest: true})
	if len(got) != 7 {
		t.Fatalf("got %d rows, want 7", len(got))
	}
	for i, r := range got {
		if want := int64(600-i) * 1e9; r.Timestamp != want {
			t.Fatalf("row %d has timestamp %d, want %d", i, r.Timestamp, want)
		}
	}
	_, full := c.Search(&storage.Query{MaxTimestamp: math.MaxInt64})
	if ss.BytesReceivedFromNodes*5 > full.BytesReceivedFromNodes {
		t.Fatalf("newest 7 moved %d bytes vs %d for everything: each node should send only its own newest 7",
			ss.BytesReceivedFromNodes, full.BytesReceivedFromNodes)
	}
}
