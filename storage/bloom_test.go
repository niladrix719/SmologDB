package storage_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/storage"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	for _, n := range []int{1, 2, 10, 1000, 100_000} {
		tokens := gen.AbsentTokens(n, 7)
		var bf storage.Bloom
		bf.Init(tokens)

		for i, tok := range tokens {
			if !bf.Contains(tok) {
				t.Fatalf("FALSE NEGATIVE with n=%d at token %d (%q).", n, i, tok)
			}
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const (
		nInserted = 100_000
		nProbes   = 2_000_000
	)

	k := float64(storage.BloomHashesCount)
	bitsPerItem := float64(storage.BloomBitsPerItem)
	theory := math.Pow(1-math.Exp(-k/bitsPerItem), k)

	inserted := gen.AbsentTokens(nInserted, 11)
	var bf storage.Bloom
	bf.Init(inserted)

	probes := gen.AbsentTokens(nProbes, 22)
	insertedSet := make(map[string]struct{}, nInserted)
	for _, tok := range inserted {
		insertedSet[tok] = struct{}{}
	}

	hits := 0
	checked := 0
	for _, tok := range probes {
		if _, ok := insertedSet[tok]; ok {
			continue
		}
		checked++
		if bf.Contains(tok) {
			hits++
		}
	}

	measured := float64(hits) / float64(checked)

	wantBytes := float64(nInserted) * bitsPerItem / 8
	gotBytes := float64(bf.SizeBytes())

	t.Logf("bloom k=%d bitsPerItem=%d n=%d probes=%d", storage.BloomHashesCount, storage.BloomBitsPerItem, nInserted, checked)
	t.Logf("  theoretical FP rate: %.4f%%", theory*100)
	t.Logf("  measured    FP rate: %.4f%%  (%d hits)", measured*100, hits)
	t.Logf("  filter size: %.1f KiB (expected ~%.1f KiB, %.2f bytes/item)", gotBytes/1024, wantBytes/1024, gotBytes/float64(nInserted))

	switch {
	case hits == 0:
		t.Fatalf("measured FP rate is EXACTLY ZERO over %d probes, but theory predicts %.4f%% (~%.0f hits).", checked, theory*100, theory*float64(checked))

	case measured < theory*0.4:
		t.Errorf("measured FP rate %.4f%% is far BELOW theory %.4f%%.\n    ~%.2f, and yours is %.2f.", measured*100, theory*100, bitsPerItem/8, gotBytes/float64(nInserted))

	case measured > theory*2.5:
		t.Errorf("measured FP rate %.4f%% is far ABOVE theory %.4f%%.\n  - the bit array is smaller than n*bitsPerItem (bytes/item is %.2f,\n    expected ~%.2f).", measured*100, theory*100, gotBytes/float64(nInserted), bitsPerItem/8)

	default:
		t.Logf("measured/theory FP ratio = %.2f", measured/theory)
	}

	if gotBytes > wantBytes*1.6 {
		t.Errorf("filter is %.2f bytes/item, expected ~%.2f", gotBytes/float64(nInserted), bitsPerItem/8)
	}
}

func TestBloomMarshal(t *testing.T) {
	tokens := gen.AbsentTokens(5000, 33)
	var bf storage.Bloom
	bf.Init(tokens)

	buf := bf.Marshal(nil)

	var bf2 storage.Bloom
	if err := bf2.Unmarshal(buf); err != nil {
		t.Fatalf("Unmarshal of a freshly marshaled filter failed: %v", err)
	}
	for _, tok := range tokens {
		if !bf2.Contains(tok) {
			t.Fatalf("token %q lost across Marshal/Unmarshal", tok)
		}
	}
	if bf2.SizeBytes() != bf.SizeBytes() {
		t.Fatalf("size changed across round-trip: %d -> %d", bf.SizeBytes(), bf2.SizeBytes())
	}

	for n := 0; n < len(buf); n += max(1, len(buf)/50) {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Unmarshal PANICKED on truncated input of len %d: %v.", n, r)
				}
			}()
			var bf3 storage.Bloom
			_ = bf3.Unmarshal(buf[:n])
		}()
	}
}

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"hello", []string{"hello"}},
		{"hello world", []string{"hello", "world"}},
		{"GET /api/v1/users 200", []string{"GET", "api", "v1", "users", "200"}},
		{"connection error: timeout after 30s", []string{"connection", "error", "timeout", "after", "30s"}},
		{"...", nil},
		{"a-b_c", []string{"a", "b", "c"}},
	}

	for _, c := range cases {
		got := storage.Tokenize(nil, c.in)
		if len(got) != len(c.want) {
			t.Errorf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
			continue
		}
		for i := range got {

			if !equalFold(got[i], c.want[i]) {
				t.Errorf("Tokenize(%q) = %v, want %v", c.in, got, c.want)
				break
			}
		}
	}

	values := repeatCycle([]string{"info", "warn", "error"}, 10_000)
	toks := storage.TokenizeValues(nil, values)
	if len(toks) != 3 {
		t.Errorf("TokenizeValues over 10000 values with 3 distinct tokens returned %d tokens, want 3.", len(toks))
	}

	f := storage.Filter{Column: "_msg", Token: "error"}
	if !f.Matches("connection error: timeout") {
		t.Errorf("Filter.Matches disagrees with Tokenize: %q should match token %q", "connection error: timeout", "error")
	}
	if f.Matches("connection errors: timeout") {
		t.Errorf("Filter.Matches is doing substring matching, not token matching: %q must not match token %q", "connection errors: timeout", "error")
	}
}

func equalFold(a, b string) bool {
	return fmt.Sprintf("%s", lower(a)) == lower(b)
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
