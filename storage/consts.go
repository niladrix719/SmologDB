package storage

const (
	MaxRowsPerBlock = 8 * 1024

	MaxColumnsPerBlock = 256

	MaxDictLen = 8

	MaxDictSizeBytes = 256

	PartFormatVersion = 1
)

var (
	InmemoryPartMaxRows = 64 * 1024

	SmallPartsMergeThreshold = 8
)

const nsecsPerDay = 24 * 60 * 60 * 1e9
