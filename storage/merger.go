package storage

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
	if len(parts) < SmallPartsMergeThreshold {
		return nil
	}
	return parts
}
