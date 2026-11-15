package storage_test

import (
	"math"
	"testing"

	"github.com/niladrix719/smologdb/gen"
	"github.com/niladrix719/smologdb/storage"
)

func TestBloomSkipsBlocks(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 500_000
	cfg.RareTokenInjections = 25
	ds := gen.Generate(cfg)

	s := newStorage(t, &storage.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	fullRange := func(f storage.Filter) *storage.Query {
		return &storage.Query{
			MinTimestamp: math.MinInt64,
			MaxTimestamp: math.MaxInt64,
			Filters:      []storage.Filter{f},
		}
	}

	qRare := fullRange(storage.Filter{Column: "_msg", Token: gen.RareToken})
	gotRare, ssRare := s.Search(qRare)

	wantRare := bruteForce(ds.Rows, qRare)
	if ok, msg := rowsEqual(gotRare, wantRare); !ok {
		t.Fatalf("rare-token query disagrees with brute-force scan: %s", msg)
	}
	if len(gotRare) != ds.RareTokenRows {
		t.Fatalf("rare-token query returned %d rows, generator injected %d", len(gotRare), ds.RareTokenRows)
	}

	t.Logf("rare token  %q -> %d rows", gen.RareToken, len(gotRare))
	t.Logf("  %s", ssRare)
	t.Logf("  skip ratio: %.2f%%", ssRare.SkipRatio()*100)

	qCommon := fullRange(storage.Filter{Column: "_msg", Token: gen.CommonToken})
	gotCommon, ssCommon := s.Search(qCommon)

	if len(gotCommon) != len(ds.Rows) {
		t.Fatalf("common-token query returned %d rows, want all %d (gen.CommonToken is injected into every message)", len(gotCommon), len(ds.Rows))
	}

	t.Logf("common token %q -> %d rows", gen.CommonToken, len(gotCommon))
	t.Logf("  %s", ssCommon)
	t.Logf("  skip ratio: %.2f%%", ssCommon.SkipRatio()*100)

	if ssRare.BlocksTotal == 0 {
		t.Fatalf("BlocksTotal is 0, want every considered block counted")
	}
	if ssRare.BlocksSkippedByBloom == 0 {
		t.Fatalf("BlocksSkippedByBloom is 0 on a token present in ~%d of %d rows.", ds.RareTokenRows, len(ds.Rows))
	}

	if ssRare.SkipRatio() < 0.85 {
		t.Errorf("rare-token skip ratio is only %.2f%%, expected >85%%.\nWith ~%d matching rows spread over %d blocks, almost every block", ssRare.SkipRatio()*100, ds.RareTokenRows, ssRare.BlocksTotal)
	}

	if ssCommon.SkipRatio() > 0.10 {
		t.Errorf("common-token skip ratio is %.2f%%, expected near 0%%.", ssCommon.SkipRatio()*100)
	}

	speedup := float64(ssCommon.BlocksScanned) / float64(max(ssRare.BlocksScanned, 1))
	t.Logf("bloom filtering avoided %.0fx the block decodes on the selective query", speedup)
}

func TestBloomFalsePositivesOnDisk(t *testing.T) {
	cfg := gen.DefaultConfig()
	cfg.Rows = 200_000
	ds := gen.Generate(cfg)

	s := newStorage(t, &storage.Config{ShardsCount: 1})
	ingest(t, s, ds, 10_000)

	k := float64(storage.BloomHashesCount)
	theory := math.Pow(1-math.Exp(-k/float64(storage.BloomBitsPerItem)), k)

	var total storage.SearchStats
	absent := gen.AbsentTokens(300, 99)
	for _, tok := range absent {
		q := &storage.Query{
			MinTimestamp: math.MinInt64,
			MaxTimestamp: math.MaxInt64,
			Filters:      []storage.Filter{{Column: "_msg", Token: tok}},
		}
		rows, ss := s.Search(q)
		if len(rows) != 0 {
			t.Fatalf("query for guaranteed-absent token %q returned %d rows", tok, len(rows))
		}
		total.Merge(ss)
	}

	if total.BlocksTotal == 0 {
		t.Fatalf("BlocksTotal is 0 across %d queries -- counters are not being bumped", len(absent))
	}

	measured := float64(total.BloomMaybe) / float64(total.BlocksTotal)
	t.Logf("in-situ FP over %d absent-token queries:", len(absent))
	t.Logf("  blocks considered: %d", total.BlocksTotal)
	t.Logf("  bloom said maybe:  %d", total.BloomMaybe)
	t.Logf("  measured FP rate:  %.4f%%   (unit-level theory: %.4f%%)", measured*100, theory*100)

	if total.BloomMaybe == total.BlocksTotal {
		t.Errorf("every block said maybe for every absent token -- bloom filters are")
	}
	if measured > 0.05 {
		t.Errorf("in-situ FP rate %.2f%% is implausibly high (>5%%).", measured*100)
	}
}
