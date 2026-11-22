package storage

import (
	"fmt"
	"testing"
)

func sizedParts(sizes ...uint64) []*part {
	ps := make([]*part, len(sizes))
	for i, size := range sizes {
		ps[i] = &part{path: fmt.Sprintf("part-%06d", i), md: &partMetadata{SizeBytes: size, RowsCount: size / 64}}
	}
	return ps
}

func repeated(size uint64, n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = size
	}
	return out
}

func TestPartsToMergePicksSimilarSizes(t *testing.T) {
	const big, small = 500 << 20, 64 << 10

	parts := sizedParts(append([]uint64{big}, repeated(small, 8)...)...)
	got := partsToMerge(parts)
	if len(got) < 2 {
		t.Fatalf("picked %d parts from one big and eight small ones, want the small ones merged", len(got))
	}
	seen := map[*part]bool{}
	for _, p := range got {
		if p.md.SizeBytes == big {
			t.Fatalf("picked the 500MB part to merge with 64KB parts")
		}
		if seen[p] {
			t.Fatalf("picked %s twice", p.path)
		}
		seen[p] = true
	}

	if got := partsToMerge(sizedParts(small, small)); got != nil {
		t.Fatalf("merged %d parts out of 2; two small parts aren't worth a merge yet", len(got))
	}
	if got := partsToMerge(sizedParts(repeated(small, 8)...)); len(got) < 2 {
		t.Fatalf("eight equal small parts: picked %d, want them merged", len(got))
	}
}
