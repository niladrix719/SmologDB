package minilog

// ---------------------------------------------------------------------------
// Deliberately tiny query model: a time range plus an AND of (column, token)
// filters. No parser, no pipes, no aggregation. That is enough to exercise
// every skipping mechanism and nothing more -- adding a query language would
// double the work and teach you nothing new about storage.
// ---------------------------------------------------------------------------

import "slices"

// Query selects rows by time range and an AND of token filters.
type Query struct {
	// MinTimestamp and MaxTimestamp are inclusive unix nanoseconds.
	MinTimestamp int64
	MaxTimestamp int64

	// StreamID, if non-nil, restricts the query to a single stream.
	// This is the cheapest possible filter: it is answered from the block
	// header alone, and from the metaindex before that.
	StreamID *uint64

	// Filters are ANDed together. Empty means "match every row in range".
	Filters []Filter

	// --- cluster (stages 9-10) ------------------------------------------

	// Limit caps the number of returned rows. 0 means unlimited.
	//
	// The rows returned are the FIRST Limit rows in (StreamID, Timestamp)
	// order, not an arbitrary Limit of them. That definition is what makes
	// the limit pushable to storage nodes in stage 10: each node's local
	// first-Limit rows are guaranteed to contain that node's contribution to
	// the global first-Limit, so merging N truncated streams and truncating
	// again gives exactly the untruncated answer.
	//
	// Convince yourself of that before you implement the pushdown. A limit
	// pushdown that returns the wrong rows is silent.
	Limit int

	// AllowPartialResponse lets a query succeed when some storage nodes are
	// unreachable, instead of failing the whole query.
	//
	// Ignored by local storage -- there is nothing to be partial about.
	//
	// The contract when this is set: the result is still sorted and still
	// contains no wrong rows, but it may be MISSING rows, and
	// SearchStats.IsPartial() reports true. Never present a partial result as
	// complete. See CLUSTER.md, stage 9.
	AllowPartialResponse bool
}

// Filter matches rows where Column contains Token as a word token.
//
// Token semantics, matching what your Tokenize produces: the filter matches
// if Token appears in the column value's token set. So Token "error" matches
// the value "connection error: timeout" but not "errors".
//
// This is exactly the shape of filter a bloom filter can answer negatively,
// which is why it is the one modelled here.
type Filter struct {
	Column string
	Token  string
}

// Matches reports whether a decoded value satisfies f.
//
// This is the ground-truth check, run only on blocks that survived bloom
// filtering. It must be independent of the bloom path: if a bug made this
// agree with a broken bloom filter, the false-positive measurement would be
// meaningless.
func (f *Filter) Matches(value string) bool {
	tokens := Tokenize(nil, value)
	return slices.Contains(tokens, f.Token)
}

// MatchesRow reports whether every filter in q matches the row, and the row
// is within the time range.
//
// The stage 1 and stage 3 harnesses use this as a brute-force oracle: they
// run a full scan with MatchesRow and compare against the result of the real
// indexed query path. Any divergence is a bug in the skipping logic -- a
// block was skipped that should not have been.
//
// Keep this dead simple. It is checked code, not fast code.
func (q *Query) MatchesRow(r *Row) bool {
	if (q.StreamID != nil && r.StreamID != *q.StreamID) || r.Timestamp > q.MaxTimestamp || r.Timestamp < q.MinTimestamp {
		return false
	}
	for _, f := range q.Filters {
		value, isPresent := r.GetField(f.Column)
		if !isPresent || !f.Matches(value) {
			return false
		}
	}
	return true
}

// ApplyLimit truncates rows to q.Limit, assuming they are already sorted.
//
// Used on both sides of the network in stage 10: the storage node applies it
// to its local result, and the select node applies it again after merging.
// Applying it twice must be identical to applying it once at the end -- that
// is the whole correctness argument for the pushdown.
func (q *Query) ApplyLimit(rows []Row) []Row {
	if q.Limit <= 0 || len(rows) <= q.Limit {
		return rows
	}
	return rows[:q.Limit]
}

// Columns returns the set of column names q reads.
//
// Used by mustReadBlockRows to decode only the needed columns -- this is
// projection pushdown in its simplest form, and it is the same idea as
// pipe.updateNeededFields in VictoriaLogs, just without the backward pass
// over a pipe chain.
func (q *Query) Columns() []string {
	seen := make(map[string]bool)
	var cols []string
	for _, f := range q.Filters {
		if !seen[f.Column] {
			seen[f.Column] = true
			cols = append(cols, f.Column)
		}
	}
	return cols
}
