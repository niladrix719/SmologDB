package storage

import (
	"errors"
	"fmt"
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

func ParseFilters(s, defaultColumn string) ([]Filter, error) {
	var filters []Filter
	var err error
	i := 0
	for {
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) {
			break
		}
		not := false
		if s[i] == '-' {
			i++
			not = true
		}
		colStart := i
		colEnd := colStart
		var columnName string
		var kind FilterKind
		var token string
		for colEnd < len(s) && s[colEnd] != ' ' && s[colEnd] != '"' && s[colEnd] != '*' {
			if s[colEnd] == ':' {
				columnName = s[colStart:colEnd]
				i = colEnd + 1
				if i < len(s) && s[i] == '=' {
					kind = MatchExact
					i++
				}
				break
			}
			colEnd++
		}
		if columnName == "" {
			columnName = defaultColumn
		}
		if i >= len(s) || s[i] == ' ' {
			return nil, fmt.Errorf("missing value after %q", columnName)
		}

		if s[i] == '"' {
			tokStart := i + 1
			tokEnd := tokStart
			for tokEnd < len(s) && s[tokEnd] != '"' {
				tokEnd++
			}
			if tokEnd == len(s) {
				return nil, fmt.Errorf("unclosed quote")
			}
			token = s[tokStart:tokEnd]
			i = tokEnd + 1
			if kind != MatchExact {
				kind = MatchPhrase
			}
		} else {
			tokStart := i
			tokEnd := tokStart
			for tokEnd < len(s) && s[tokEnd] != ' ' {
				tokEnd++
			}
			token = s[tokStart:tokEnd]
			i = tokEnd
			if kind != MatchExact && strings.HasSuffix(token, "*") {
				token = token[:len(token)-1]
				kind = MatchPrefix
			}
		}
		words := Tokenize(nil, token)
		if token == "" || (len(words) == 0 && kind != MatchExact) {
			return nil, fmt.Errorf("nothing to search for in %q", token)
		}
		tokens := []string{token}
		if kind == MatchWord {
			tokens = words
		}
		for _, tok := range tokens {
			filters = append(filters, Filter{Column: columnName, Token: tok, Kind: kind, Not: not})
		}
		if len(filters) > 999 {
			return nil, errors.New("too many terms")
		}
	}
	return filters, err
}
