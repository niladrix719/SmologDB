package storage

import "slices"

type Query struct {
	MinTimestamp int64
	MaxTimestamp int64

	StreamID *uint64

	Filters []Filter

	Limit int

	AllowPartialResponse bool
}

type Filter struct {
	Column string
	Token  string
}

func (f *Filter) Matches(value string) bool {
	tokens := Tokenize(nil, value)
	return slices.Contains(tokens, f.Token)
}

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

func (q *Query) ApplyLimit(rows []Row) []Row {
	if q.Limit <= 0 || len(rows) <= q.Limit {
		return rows
	}
	return rows[:q.Limit]
}

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
