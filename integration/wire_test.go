package integration

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/internalapi"
	"github.com/niladrix719/smologdb/storage"
)

func codecTestRows() []storage.Row {
	return []storage.Row{

		{Timestamp: 1750000000123456789, StreamID: 0xdeadbeefcafe,
			Fields: []storage.Field{{Name: "_msg", Value: "hello"}, {Name: "level", Value: "info"}}},

		{Timestamp: 1, StreamID: 0,
			Fields: []storage.Field{{Name: "a", Value: "b"}}},

		{Timestamp: math.MaxInt64, StreamID: math.MaxUint64,
			Fields: []storage.Field{{Name: "a", Value: "b"}}},

		{Timestamp: 0, StreamID: 7, Fields: []storage.Field{{Name: "a", Value: "b"}}},
		{Timestamp: -1500000000000000000, StreamID: 7,
			Fields: []storage.Field{{Name: "a", Value: "b"}}},

		{Timestamp: 5, StreamID: 8, Fields: []storage.Field{{Name: "empty", Value: ""}}},

		{Timestamp: 6, StreamID: 9, Fields: []storage.Field{
			{Name: "binary", Value: "\x00\xff\xfe mid\x00dle"},
			{Name: "unicode", Value: "日本語 ☃ café"},
		}},

		{Timestamp: 7, StreamID: 10, Fields: []storage.Field{
			{Name: "big", Value: strings.Repeat("x", 200000)},
		}},

		{Timestamp: 8, StreamID: 11, Fields: manyFields(64)},
	}
}

func manyFields(n int) []storage.Field {
	out := make([]storage.Field, n)
	for i := range out {
		out[i] = storage.Field{Name: fmt.Sprintf("f%03d", i), Value: fmt.Sprintf("v%d", i*7)}
	}
	return out
}

func TestRowCodecRoundTrip(t *testing.T) {
	for i, want := range codecTestRows() {
		buf := internalapi.MarshalRow(nil, &want)

		var got storage.Row
		tail, err := internalapi.UnmarshalRow(&got, buf)
		if err != nil {
			t.Fatalf("row %d: UnmarshalRow: %v", i, err)
		}
		if len(tail) != 0 {
			t.Fatalf("row %d: UnmarshalRow left %d trailing bytes -- the decoder is not", i, len(tail))
		}
		if ok, msg := rowsEqual([]storage.Row{got}, []storage.Row{want}); !ok {
			t.Fatalf("row %d did not survive the round trip: %s", i, msg)
		}
	}

	prefix := []byte("SENTINEL")
	r := codecTestRows()[0]
	out := internalapi.MarshalRow(prefix, &r)
	if !strings.HasPrefix(string(out), "SENTINEL") {
		t.Fatalf("MarshalRow overwrote dst instead of appending to it")
	}
}

func TestBatchCodecRoundTrip(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 50_000
	ds := gen.Generate(cfg)
	storage.SortRows(ds.Rows)

	buf := internalapi.MarshalRowBatch(nil, ds.Rows)

	got, tail, err := internalapi.UnmarshalRowBatch(nil, buf)
	if err != nil {
		t.Fatalf("UnmarshalRowBatch: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("UnmarshalRowBatch left %d trailing bytes", len(tail))
	}
	if ok, msg := rowsEqual(got, ds.Rows); !ok {
		t.Fatalf("batch round trip: %s", msg)
	}

	if len(got) > 1 && got[0].Timestamp == got[len(got)-1].Timestamp &&
		ds.Rows[0].Timestamp != ds.Rows[len(ds.Rows)-1].Timestamp {
		t.Fatalf("every decoded row is identical -- UnmarshalRowBatch is appending the")
	}

	t.Logf("batch: %d rows -> %d wire bytes", len(ds.Rows), len(buf))
}

func TestQueryCodecRoundTrip(t *testing.T) {
	sid := uint64(0)
	bigSid := uint64(math.MaxUint64)

	cases := []*storage.Query{
		{MinTimestamp: math.MinInt64, MaxTimestamp: math.MaxInt64},
		{MinTimestamp: -5, MaxTimestamp: 5, StreamID: &sid},
		{MinTimestamp: 1, MaxTimestamp: 2, StreamID: &bigSid},
		{MinTimestamp: 1, MaxTimestamp: 2, Limit: 100},
		{MinTimestamp: 1, MaxTimestamp: 2, AllowPartialResponse: true},
		{
			MinTimestamp: 1750000000000000000, MaxTimestamp: 1750000003600000000,
			StreamID:             &bigSid,
			Limit:                42,
			AllowPartialResponse: true,
			Filters: []storage.Filter{
				{Column: "_msg", Token: "error"},
				{Column: "level", Token: ""},
				{Column: "", Token: "weird"},
				{Column: "unicode", Token: "日本語"},
			},
		},
	}

	for i, want := range cases {
		buf := internalapi.MarshalQuery(nil, want)

		var got storage.Query
		tail, err := internalapi.UnmarshalQuery(&got, buf)
		if err != nil {
			t.Fatalf("query %d: UnmarshalQuery: %v", i, err)
		}
		if len(tail) != 0 {
			t.Fatalf("query %d: %d trailing bytes after decode", i, len(tail))
		}

		if got.MinTimestamp != want.MinTimestamp || got.MaxTimestamp != want.MaxTimestamp {
			t.Fatalf("query %d: time range %d..%d != %d..%d", i, got.MinTimestamp, got.MaxTimestamp, want.MinTimestamp, want.MaxTimestamp)
		}
		if (got.StreamID == nil) != (want.StreamID == nil) {
			t.Fatalf("query %d: StreamID presence lost (got nil=%v, want nil=%v).", i, got.StreamID == nil, want.StreamID == nil)
		}
		if got.StreamID != nil && *got.StreamID != *want.StreamID {
			t.Fatalf("query %d: StreamID %d != %d", i, *got.StreamID, *want.StreamID)
		}
		if got.Limit != want.Limit {
			t.Fatalf("query %d: Limit %d != %d -- a dropped limit returns too many rows,", i, got.Limit, want.Limit)
		}
		if got.AllowPartialResponse != want.AllowPartialResponse {
			t.Fatalf("query %d: AllowPartialResponse %v != %v", i, got.AllowPartialResponse, want.AllowPartialResponse)
		}
		if len(got.Filters) != len(want.Filters) {
			t.Fatalf("query %d: %d filters != %d", i, len(got.Filters), len(want.Filters))
		}
		for j := range got.Filters {
			if got.Filters[j] != want.Filters[j] {
				t.Fatalf("query %d filter %d: %+v != %+v", i, j, got.Filters[j], want.Filters[j])
			}
		}
	}
}

func TestDecoderRejectsGarbage(t *testing.T) {
	r := codecTestRows()[0]
	good := internalapi.MarshalRow(nil, &r)

	bad := map[string][]byte{
		"empty":              {},
		"truncated-half":     good[:len(good)/2],
		"truncated-one-byte": good[:len(good)-1],
		"single-zero":        {0x00},
		"huge-length-prefix": {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		"all-ones":           {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	}

	for name, src := range bad {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("UnmarshalRow PANICKED on %s input: %v", name, p)
				}
			}()
			var got storage.Row
			if _, err := internalapi.UnmarshalRow(&got, src); err == nil {
				t.Fatalf("UnmarshalRow accepted %s input without an error", name)
			}
		})
	}
}

func FuzzRowRoundTrip(f *testing.F) {
	f.Add(uint64(1), int64(0), "a", "b", "c", "d")
	f.Add(uint64(0), int64(-1), "", "", "", "")
	f.Add(uint64(math.MaxUint64), int64(math.MaxInt64), "_msg", "\x00\xff", "z", "日本語")

	f.Fuzz(func(t *testing.T, sid uint64, ts int64, n1, v1, n2, v2 string) {

		if n1 >= n2 {
			t.Skip()
		}
		want := storage.Row{
			Timestamp: ts, StreamID: sid,
			Fields: []storage.Field{{Name: n1, Value: v1}, {Name: n2, Value: v2}},
		}

		buf := internalapi.MarshalRow(nil, &want)
		var got storage.Row
		tail, err := internalapi.UnmarshalRow(&got, buf)
		if err != nil {
			t.Fatalf("round trip failed: %v (row %+v)", err, want)
		}
		if len(tail) != 0 {
			t.Fatalf("%d trailing bytes (row %+v)", len(tail), want)
		}
		if ok, msg := rowsEqual([]storage.Row{got}, []storage.Row{want}); !ok {
			t.Fatalf("%s (row %+v)", msg, want)
		}
	})
}

func TestProtocolVersionIsChecked(t *testing.T) {
	ns := newNodeSet(t, 1)

	r := codecTestRows()[0]
	body := internalapi.MarshalRowBatch(nil, []storage.Row{r})

	for _, version := range []string{"", "v0", "v99", "V1", "v1 "} {
		url := fmt.Sprintf("http://%s%s?%s=%s", ns.addrs[0], internalapi.InsertPath,
			internalapi.VersionArg, version)
		code, respBody := httpPost(t, url, body)
		if code == 200 || code == 204 {
			t.Fatalf("storage node ACCEPTED an insert with version=%q (it speaks %q).", version, internalapi.ProtocolVersion)
		}
		if code != 400 {
			t.Errorf("version=%q rejected with status %d; want 400 (the REQUEST is\n400 and 5xx meaning different things). Body: %s", version, code, respBody)
		}
	}

	url := fmt.Sprintf("http://%s%s?%s=%s", ns.addrs[0], internalapi.InsertPath,
		internalapi.VersionArg, internalapi.ProtocolVersion)
	if code, respBody := httpPost(t, url, body); code/100 != 2 {
		t.Fatalf("storage node REJECTED the correct version %q with status %d: %s", internalapi.ProtocolVersion, code, respBody)
	}

	t.Logf("version check: 5 bad versions rejected with 400, %q accepted", internalapi.ProtocolVersion)
}

func TestClusterMatchesLocalStorage(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	local := newStorage(t, &storage.Config{ShardsCount: 1})
	ingest(t, local, ds, 10_000)
	runConformance(t, local, ds, "local")

	ns := newNodeSet(t, 1)
	remote := newDirectNode(ns.addrs[0])
	ingest(t, remote, ds, 10_000)
	runConformance(t, remote, ds, "over HTTP")

	for i, q := range conformanceQueries(ds) {
		gotLocal, ssLocal := local.Search(q)
		gotRemote, ssRemote := remote.Search(q)
		if ok, msg := rowsEqual(gotRemote, gotLocal); !ok {
			t.Fatalf("query %d: local and remote disagree: %s", i, msg)
		}
		if i == 0 {
			t.Logf("full scan local:  %s", ssLocal)
			t.Logf("full scan remote: %s", ssRemote)
		}
	}

	t.Logf("%d conformance queries: local == over-HTTP, both == brute force", len(conformanceQueries(ds)))

}
