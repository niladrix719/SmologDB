package storage_test

import (
	"maps"
	"math"
	"slices"
	"testing"

	"github.com/niladrix719/smologdb/storage"
)

var filterRows = []storage.Row{
	testRow(1, 1e9, "_msg", "GET /api/users 200", "level", "info"),
	testRow(1, 2e9, "_msg", "GET /api/orders 500", "level", "error"),
	testRow(2, 3e9, "_msg", "POST /api/users 201", "level", "info"),
	testRow(2, 4e9, "_msg", "payment failed for user 7", "level", "error"),
	testRow(3, 5e9, "_msg", "payment ok", "level", "info"),
	testRow(3, 6e9, "_msg", "failed payment retry", "level", "warn"),
}

func TestFilterKinds(t *testing.T) {
	s := newStorage(t, nil)
	s.MustAddRows(filterRows)
	s.MustForceFlush()

	cases := []struct {
		name    string
		filters []storage.Filter
		want    []string
	}{
		{"word", []storage.Filter{{Column: "_msg", Token: "users"}},
			[]string{"GET /api/users 200", "POST /api/users 201"}},
		{"exact field", []storage.Filter{{Column: "level", Token: "error", Kind: storage.MatchExact}},
			[]string{"GET /api/orders 500", "payment failed for user 7"}},
		{"exact message", []storage.Filter{{Column: "_msg", Token: "payment ok", Kind: storage.MatchExact}},
			[]string{"payment ok"}},
		{"prefix", []storage.Filter{{Column: "_msg", Token: "pay", Kind: storage.MatchPrefix}},
			[]string{"payment failed for user 7", "payment ok", "failed payment retry"}},
		{"phrase", []storage.Filter{{Column: "_msg", Token: "payment failed", Kind: storage.MatchPhrase}},
			[]string{"payment failed for user 7"}},
		{"not", []storage.Filter{{Column: "_msg", Token: "GET", Not: true}},
			[]string{"POST /api/users 201", "payment failed for user 7", "payment ok", "failed payment retry"}},
		{"not on a missing column", []storage.Filter{{Column: "missing", Token: "x", Not: true}},
			[]string{"GET /api/users 200", "GET /api/orders 500", "POST /api/users 201", "payment failed for user 7", "payment ok", "failed payment retry"}},
		{"not a word no block has", []storage.Filter{{Column: "_msg", Token: "zzzz", Not: true}},
			[]string{"GET /api/users 200", "GET /api/orders 500", "POST /api/users 201", "payment failed for user 7", "payment ok", "failed payment retry"}},
		{"word and not phrase", []storage.Filter{
			{Column: "_msg", Token: "payment"},
			{Column: "_msg", Token: "payment failed", Kind: storage.MatchPhrase, Not: true},
		}, []string{"payment ok", "failed payment retry"}},
	}
	for _, c := range cases {
		got, _ := s.Search(&storage.Query{MaxTimestamp: math.MaxInt64, Filters: c.filters})
		gotMsgs := slices.Sorted(maps.Keys(messages(got)))
		want := slices.Sorted(slices.Values(c.want))
		if !slices.Equal(gotMsgs, want) || len(got) != len(want) {
			t.Errorf("%s: got %q, want %q", c.name, gotMsgs, want)
		}
	}
}

func TestParseFilters(t *testing.T) {
	word := func(col, tok string) storage.Filter { return storage.Filter{Column: col, Token: tok} }
	cases := []struct {
		in   string
		want []storage.Filter
	}{
		{"", nil},
		{"ERROR", []storage.Filter{word("_msg", "ERROR")}},
		{"ERROR payment", []storage.Filter{word("_msg", "ERROR"), word("_msg", "payment")}},
		{"user=29", []storage.Filter{word("_msg", "user"), word("_msg", "29")}},
		{"level:error", []storage.Filter{word("level", "error")}},
		{"level:=error", []storage.Filter{{Column: "level", Token: "error", Kind: storage.MatchExact}}},
		{"-debug", []storage.Filter{{Column: "_msg", Token: "debug", Not: true}}},
		{"-level:=debug", []storage.Filter{{Column: "level", Token: "debug", Kind: storage.MatchExact, Not: true}}},
		{"pay*", []storage.Filter{{Column: "_msg", Token: "pay", Kind: storage.MatchPrefix}}},
		{`"payment failed"`, []storage.Filter{{Column: "_msg", Token: "payment failed", Kind: storage.MatchPhrase}}},
		{`service:"checkout api" ERROR`, []storage.Filter{
			{Column: "service", Token: "checkout api", Kind: storage.MatchPhrase},
			word("_msg", "ERROR"),
		}},
	}
	for _, c := range cases {
		got, err := storage.ParseFilters(c.in, "_msg")
		if err != nil {
			t.Errorf("ParseFilters(%q): %v", c.in, err)
			continue
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("ParseFilters(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
	for _, bad := range []string{`"unclosed`, "level:", "*", "-"} {
		if _, err := storage.ParseFilters(bad, "_msg"); err == nil {
			t.Errorf("ParseFilters(%q) accepted it, want an error", bad)
		}
	}
}
