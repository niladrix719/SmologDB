package minilog

// ---------------------------------------------------------------------------
// This is the seam the whole cluster hangs off. See CLUSTER.md, stage 6.
//
// Reference: app/vlstorage/main.go in VictoriaLogs, which holds a localStorage,
// a netstorageInsert and a netstorageSelect and picks between them based on
// whether -storageNode was passed. Same idea, four methods.
// ---------------------------------------------------------------------------

// LogStorage is everything above the storage layer is allowed to know about
// storage.
//
// Deliberately four methods. Every method that is NOT here is one that cannot
// be implemented over a network without inventing distributed semantics for
// it, and leaving them out is what makes the cluster tractable:
//
//	MustForceMerge          -- merging is per-node; there is no cluster-wide merge
//	MustDropPartitionsBefore -- retention is per-node, enforced locally
//	PartitionDays           -- partitions are a local storage concept
//
// VictoriaLogs makes the same cut: look at processForceMerge in
// app/vlstorage/main.go and note that it returns false (unhandled) when
// localStorage == nil. A select node genuinely cannot answer it.
//
// If you find yourself wanting to widen this interface, that is a signal worth
// listening to. Write down what you wanted and why before you add it.
type LogStorage interface {
	// MustAddRows ingests rows. Returns once the rows are accepted; not
	// necessarily once they are queryable (see MustForceFlush).
	MustAddRows(rows []Row)

	// Search returns rows matching q sorted by (StreamID, Timestamp), plus
	// instrumentation.
	Search(q *Query) ([]Row, *SearchStats)

	// MustForceFlush makes every accepted row queryable.
	//
	// Test-only hook, and it means something slightly different on each side
	// of the seam: locally it seals in-memory parts, and over the network it
	// must ALSO drain the client's per-node send buffers first. Getting that
	// order wrong (flushing nodes before draining buffers) is the single most
	// common reason a cluster harness sees fewer rows than it ingested.
	MustForceFlush()

	// MustClose flushes everything and releases resources.
	MustClose()
}

// The local engine is the reference implementation of the seam.
var _ LogStorage = (*Storage)(nil)
