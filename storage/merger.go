package storage

import (
	"cmp"
	"slices"
)

func mustMergeParts(dstPath string, srcParts []*part) *partMetadata {
	var rows []Row
	var blocks []block
	for _, part := range srcParts {
		rows = append(rows, part.mustReadAllRows()...)
	}
	SortRows(rows)
	i := 0
	for i < len(rows) {
		j := i + 1
		for j < len(rows) && rows[i].StreamID == rows[j].StreamID && j-i < MaxRowsPerBlock {
			j++
		}
		var b block
		b.mustInitFromRows(rows[i:j])
		blocks = append(blocks, b)
		i = j
	}
	pw := mustCreatePartWriter(dstPath)
	for _, b := range blocks {
		pw.MustWriteBlock(&b)
	}
	pw.MustClose()
	return &pw.md
}

func partsToMerge(parts []*part) []*part {
	partsClone := slices.Clone(parts)
	slices.SortFunc(partsClone, func(a, b *part) int {
		return cmp.Compare(a.md.SizeBytes, b.md.SizeBytes)
	})
	for start := range partsClone {
		base := partsClone[start].md.SizeBytes
		end := start
		for end < len(partsClone) && partsClone[end].md.SizeBytes <= base*4 {
			end++
		}
		if end-start >= SmallPartsMergeThreshold {
			return partsClone[start:end]
		}
	}
	return nil
}
