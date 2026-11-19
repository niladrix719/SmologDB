package storage

import (
	"errors"
	"slices"
	"strings"
)

type Query struct {
	MinTimestamp int64
	MaxTimestamp int64

	StreamID *uint64

	Filters []Filter

	Limit int

	Newest bool

	AllowPartialResponse bool
}

// FilterKind says how a filter's Token is compared with a value.
type FilterKind uint8

const (
	MatchWord   FilterKind = iota // the value contains Token as a whole word
	MatchExact                    // the whole value equals Token
	MatchPrefix                   // some word in the value starts with Token
	MatchPhrase                   // Token's words appear in the value next to each other, in order
)

type Filter struct {
	Column string
	Token  string

	Kind FilterKind
	Not  bool
}

func (f *Filter) Matches(value string) bool {
	tokens := Tokenize(nil, value)
	switch f.Kind {
	case MatchExact:
		return value == f.Token
	case MatchPrefix:
		for _, tok := range tokens {
			if strings.HasPrefix(tok, f.Token) {
				return true
			}
		}
		return false
	case MatchPhrase:
		wantedTokens := Tokenize(nil, f.Token)
		for i := 0; i+len(wantedTokens) <= len(tokens); i++ {
			if slices.Equal(tokens[i:i+len(wantedTokens)], wantedTokens) {
				return true
			}
		}
		return false
	default:
		return slices.Contains(tokens, f.Token)
	}
}

func (q *Query) MatchesRow(r *Row) bool {
	if (q.StreamID != nil && r.StreamID != *q.StreamID) || r.Timestamp > q.MaxTimestamp || r.Timestamp < q.MinTimestamp {
		return false
	}
	for _, f := range q.Filters {
		value, isPresent := r.GetField(f.Column)
		matched := isPresent && f.Matches(value)
		if matched == f.Not {
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

// ParseFilters turns a search-box string like `ERROR -debug service:checkout "disk full"`
// into filters on defaultColumn and named columns.
// TODO(#9): not implemented yet; see TODO.md for the syntax.
func ParseFilters(s, defaultColumn string) ([]Filter, error) {
	return nil, errors.New("query syntax not implemented yet")
}
