package storage_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/storage"
)

func TestVarint(t *testing.T) {
	values := []uint64{
		0, 1, 2, 127, 128, 129, 255, 256, 16383, 16384,
		1 << 20, 1 << 31, 1 << 32, 1<<63 - 1, math.MaxUint64,
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10000; i++ {
		values = append(values, rng.Uint64()>>uint(rng.Intn(64)))
	}

	for _, want := range values {
		buf := storage.MarshalVarUint64(nil, want)
		got, tail, err := storage.UnmarshalVarUint64(buf)
		if err != nil {
			t.Fatalf("UnmarshalVarUint64(%d): unexpected error: %v", want, err)
		}
		if got != want {
			t.Fatalf("varint round-trip: got %d, want %d", got, want)
		}
		if len(tail) != 0 {
			t.Fatalf("varint round-trip for %d: %d trailing bytes", want, len(tail))
		}
	}

	full := storage.MarshalVarUint64(nil, math.MaxUint64)
	for n := 0; n < len(full); n++ {
		if _, _, err := storage.UnmarshalVarUint64(full[:n]); err == nil {
			t.Fatalf("UnmarshalVarUint64 accepted truncated input of len %d", n)
		}
	}
}

func TestStringEncoding(t *testing.T) {
	inputs := []string{"", "a", "hello world", "üñïçødé", string(make([]byte, 100000))}
	var buf []byte
	for _, s := range inputs {
		buf = storage.MarshalString(buf, s)
	}
	tail := buf
	for _, want := range inputs {
		got, rest, err := storage.UnmarshalString(tail)
		if err != nil {
			t.Fatalf("UnmarshalString: unexpected error: %v", err)
		}
		if got != want {
			t.Fatalf("string round-trip: got %q, want %q", got, want)
		}
		tail = rest
	}
	if len(tail) != 0 {
		t.Fatalf("%d trailing bytes after decoding all strings", len(tail))
	}
}

func TestTimestampEncoding(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	base := int64(1750000000 * 1e9)

	for _, count := range []int{1, 2, 100, 8192} {
		ts := make([]int64, count)
		cur := base
		for i := range ts {
			cur += rng.Int63n(5 * 1e6)
			ts[i] = cur
		}

		buf := storage.MarshalTimestamps(nil, ts)
		got, err := storage.UnmarshalTimestamps(nil, buf, count)
		if err != nil {
			t.Fatalf("UnmarshalTimestamps: unexpected error: %v", err)
		}
		if len(got) != count {
			t.Fatalf("timestamp count: got %d, want %d", len(got), count)
		}
		for i := range ts {
			if got[i] != ts[i] {
				t.Fatalf("timestamp %d: got %d, want %d", i, got[i], ts[i])
			}
		}

		if count == 8192 {
			bytesPerTS := float64(len(buf)) / float64(count)
			t.Logf("delta-encoded timestamps: %.2f bytes/row (raw int64 = 8.00)", bytesPerTS)
			if bytesPerTS >= 8 {
				t.Errorf("delta encoding is not helping: %.2f bytes/row, want < 8", bytesPerTS)
			}
		}
	}
}

func TestValueEncoding(t *testing.T) {
	cases := []struct {
		name     string
		values   []string
		wantType storage.ValueType
	}{
		{
			name:     "low cardinality picks dict",
			values:   repeatCycle([]string{"info", "warn", "error"}, 8192),
			wantType: storage.ValueTypeDict,
		},
		{
			name:     "plain integers pick uint64",
			values:   []string{"0", "1", "42", "999999", "18446744073709551615"},
			wantType: storage.ValueTypeUint64,
		},
		{
			name:     "high cardinality strings fall back to string",
			values:   uniqueStrings(1000),
			wantType: storage.ValueTypeString,
		},
		{

			name:     "leading zeros must not become uint64",
			values:   []string{"007", "042", "100"},
			wantType: storage.ValueTypeString,
		},
		{
			name:     "empty values",
			values:   []string{"", "", ""},
			wantType: storage.ValueTypeDict,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf, vt, dict := storage.EncodeValues(nil, c.values)

			got, err := storage.DecodeValues(nil, buf, vt, dict, len(c.values))
			if err != nil {
				t.Fatalf("DecodeValues: unexpected error: %v", err)
			}
			if len(got) != len(c.values) {
				t.Fatalf("value count: got %d, want %d", len(got), len(c.values))
			}
			for i := range c.values {
				if got[i] != c.values[i] {
					t.Fatalf("value %d: got %q, want %q (encoded as %s)", i, got[i], c.values[i], vt)
				}
			}

			if vt != c.wantType {
				t.Errorf("chose %s encoding, expected %s", vt, c.wantType)
			}

			raw := 0
			for _, v := range c.values {
				raw += len(v) + 1
			}
			t.Logf("%s: %s, %d bytes for %d values (raw %d, ratio %.2fx)", c.name, vt, len(buf), len(c.values), raw, float64(raw)/float64(max(len(buf), 1)))
		})
	}
}

func TestPartRoundTrip(t *testing.T) {
	cfg := gen.DefaultConfig()
	ds := gen.Generate(cfg)

	s := newStorage(t, &storage.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	q := &storage.Query{
		MinTimestamp: math.MinInt64,
		MaxTimestamp: math.MaxInt64,
	}
	got, ss := s.Search(q)

	want := make([]storage.Row, len(ds.Rows))
	copy(want, ds.Rows)
	storage.SortRows(want)

	if ok, msg := rowsEqual(got, want); !ok {
		t.Fatalf("full scan is not byte-identical to input: %s", msg)
	}

	ratio := storage.CompressionRatio()
	t.Logf("rows=%d logical=%.1fMiB onDisk=%.1fMiB ratio=%.2fx", len(ds.Rows),
		float64(ds.LogicalBytes)/(1<<20),
		float64(storage.Metrics.BytesLiveOnDisk.Load())/(1<<20),
		ratio)
	t.Logf("search stats: %s", ss)

	if ratio < 5 {
		t.Errorf("compression ratio %.2fx is below the 5x target.", ratio)
	}
}

func repeatCycle(vals []string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = vals[i%len(vals)]
	}
	return out
}

func uniqueStrings(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("trace-%016x-%d", uint64(i)*0x9e3779b97f4a7c15, i)
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
