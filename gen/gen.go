// Package gen produces deterministic synthetic log data shaped like real
// Kubernetes application logs.
//
// The harness measurements depend on its exact distributions (cardinalities,
// rare-token injection rate), so changing it changes what the numbers mean.
package gen

import (
	"fmt"
	"math/rand"

	"github.com/niladrix719/minilog/minilog"
)

// Config controls dataset generation.
type Config struct {
	// Rows is the total number of log entries to generate.
	Rows int

	// Streams is the number of distinct log streams.
	//
	// This is the cardinality knob and it is the most important one in the
	// whole exercise. Try 100, then 10_000, then 1_000_000 with the same row
	// count and watch what happens to part count, index size, and query
	// latency. High stream cardinality is the #1 production failure mode of
	// VictoriaLogs and this is where you get to feel it.
	Streams int

	// StartTime is the unix-nanosecond timestamp of the first row.
	StartTime int64

	// SpanNanos is the time range the rows are spread across.
	SpanNanos int64

	// Seed makes generation deterministic. Same seed, same bytes.
	Seed int64

	// RareTokenInjections is the EXACT number of rows that will contain
	// RareToken. Defaults to max(Rows/20000, 20).
	//
	// This is an exact count rather than a probability on purpose: at a
	// realistic rarity, a probabilistic rate lands on zero often enough to
	// make the stage 2 skip measurement randomly degenerate. The harness needs
	// a guaranteed non-zero ground truth to compare against.
	//
	// Keep it small relative to the block count. The stage 2 harness asserts
	// you skip >95% of blocks, which requires the matching rows to be
	// concentrated in a handful of blocks.
	RareTokenInjections int
}

// Tokens the harnesses query for. They are injected at known rates so the
// measurements have a ground truth to compare against.
const (
	// RareToken appears in exactly Config.RareTokenInjections rows.
	// A query for it should skip nearly every block.
	RareToken = "zqxjkvbwpf"

	// CommonToken appears in every row. Should skip ~no blocks.
	CommonToken = "request"

	// AbsentTokenPrefix is used to build tokens guaranteed not to exist
	// anywhere in the dataset. The false-positive harness queries thousands
	// of these; every "maybe" answer is by definition a false positive.
	AbsentTokenPrefix = "nonexistent"
)

// DefaultConfig returns a small, fast dataset suitable for iterating.
func DefaultConfig() *Config {
	return &Config{
		Rows:      200_000,
		Streams:   200,
		StartTime: 1750000000 * 1e9,
		SpanNanos: 6 * 3600 * 1e9,
		Seed:      42,
	}
}

// defaultRareInjections is the fallback when Config.RareTokenInjections is 0.
func defaultRareInjections(rows int) int {
	n := rows / 20000
	if n < 20 {
		n = 20
	}
	if n > rows {
		n = rows
	}
	return n
}

var (
	levels   = []string{"debug", "info", "warn", "error"}
	levelsW  = []int{50, 400, 40, 10} // weights; info dominates, as in real logs
	services = []string{
		"api-gateway", "auth-service", "billing", "cart", "checkout",
		"inventory", "notifications", "payments", "search", "shipping",
	}
	methods  = []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	statuses = []string{"200", "201", "204", "301", "400", "401", "403", "404", "429", "500", "502", "503"}
	statusW  = []int{600, 80, 40, 10, 60, 30, 20, 80, 15, 40, 15, 10}
	paths    = []string{
		"/api/v1/users", "/api/v1/orders", "/api/v1/products", "/api/v1/cart",
		"/api/v1/checkout", "/healthz", "/metrics", "/api/v2/search",
	}
	regions = []string{"us-east-1", "us-west-2", "eu-central-1", "ap-south-1"}
)

// Stream is one log stream's identifying labels.
type Stream struct {
	ID     uint64
	Labels []minilog.Field
}

// Dataset is a generated corpus plus the ground truth about it.
type Dataset struct {
	Rows    []minilog.Row
	Streams []Stream

	// RareTokenRows is the exact number of rows containing gen.RareToken.
	// The stage 2 harness asserts the query returns exactly this many.
	RareTokenRows int

	// LogicalBytes is the sum of Row.LogicalSizeBytes().
	LogicalBytes int64

	cfg *Config
}

// Generate builds a dataset. Deterministic for a given Config.
func Generate(cfg *Config) *Dataset {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	nRare := cfg.RareTokenInjections
	if nRare == 0 {
		nRare = defaultRareInjections(cfg.Rows)
	}
	rng := rand.New(rand.NewSource(cfg.Seed))

	// Pick the exact set of row indices that will carry RareToken.
	rareIdx := make(map[int]struct{}, nRare)
	for len(rareIdx) < nRare && len(rareIdx) < cfg.Rows {
		rareIdx[rng.Intn(cfg.Rows)] = struct{}{}
	}

	streams := make([]Stream, cfg.Streams)
	for i := range streams {
		labels := []minilog.Field{
			{Name: "service", Value: services[i%len(services)]},
			{Name: "pod", Value: fmt.Sprintf("%s-%d-%04x", services[i%len(services)], i/len(services), rng.Intn(1<<16))},
			{Name: "namespace", Value: []string{"prod", "staging"}[i%2]},
			{Name: "region", Value: regions[i%len(regions)]},
		}
		streams[i] = Stream{ID: minilog.StreamIDForLabels(labels), Labels: labels}
	}

	ds := &Dataset{
		Rows:    make([]minilog.Row, 0, cfg.Rows),
		Streams: streams,
		cfg:     cfg,
	}

	step := cfg.SpanNanos / int64(max(cfg.Rows, 1))
	for i := 0; i < cfg.Rows; i++ {
		st := &streams[rng.Intn(len(streams))]
		ts := cfg.StartTime + int64(i)*step + rng.Int63n(max64(step, 1))

		level := levels[weighted(rng, levelsW)]
		status := statuses[weighted(rng, statusW)]
		method := methods[rng.Intn(len(methods))]
		path := paths[rng.Intn(len(paths))]
		durMs := rng.Intn(3000)
		traceID := fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())

		msg := fmt.Sprintf("%s %s %s request completed status=%s duration_ms=%d",
			level, method, path, status, durMs)
		if _, ok := rareIdx[i]; ok {
			msg += " " + RareToken
			ds.RareTokenRows++
		}

		// Fields must be sorted by Name -- it is a precondition of the block
		// writer, and the round-trip harness compares field slices directly.
		fields := []minilog.Field{
			{Name: "_msg", Value: msg},
			{Name: "duration_ms", Value: fmt.Sprintf("%d", durMs)},
			{Name: "level", Value: level},
			{Name: "method", Value: method},
			{Name: "namespace", Value: st.Labels[2].Value},
			{Name: "path", Value: path},
			{Name: "pod", Value: st.Labels[1].Value},
			{Name: "region", Value: st.Labels[3].Value},
			{Name: "service", Value: st.Labels[0].Value},
			{Name: "status", Value: status},
			{Name: "trace_id", Value: traceID},
		}

		r := minilog.Row{Timestamp: ts, StreamID: st.ID, Fields: fields}
		ds.LogicalBytes += int64(r.LogicalSizeBytes())
		ds.Rows = append(ds.Rows, r)
	}
	return ds
}

// AbsentTokens returns n tokens guaranteed not to appear in any dataset.
//
// Used by the false-positive harness: every block whose bloom filter says
// "maybe" for one of these is, by construction, a false positive.
func AbsentTokens(n int, seed int64) []string {
	rng := rand.New(rand.NewSource(seed))
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%016x%016x", AbsentTokenPrefix, rng.Uint64(), rng.Uint64())
	}
	return out
}

func weighted(rng *rand.Rand, weights []int) int {
	total := 0
	for _, w := range weights {
		total += w
	}
	x := rng.Intn(total)
	for i, w := range weights {
		if x < w {
			return i
		}
		x -= w
	}
	return len(weights) - 1
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
