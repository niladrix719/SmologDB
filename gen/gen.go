// Package gen generates deterministic synthetic logs for tests.
package gen

import (
	"fmt"
	"math/rand"

	"github.com/niladrix719/smologdb/storage"
)

type Config struct {
	Rows int

	Streams int

	StartTime int64

	SpanNanos int64

	Seed int64

	RareTokenInjections int
}

const (
	RareToken = "zqxjkvbwpf"

	CommonToken = "request"

	AbsentTokenPrefix = "nonexistent"
)

func DefaultConfig() *Config {
	return &Config{
		Rows:      200_000,
		Streams:   200,
		StartTime: 1750000000 * 1e9,
		SpanNanos: 6 * 3600 * 1e9,
		Seed:      42,
	}
}

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
	levelsW  = []int{50, 400, 40, 10}
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

type Stream struct {
	ID     uint64
	Labels []storage.Field
}

type Dataset struct {
	Rows    []storage.Row
	Streams []Stream

	RareTokenRows int

	LogicalBytes int64

	cfg *Config
}

func Generate(cfg *Config) *Dataset {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	nRare := cfg.RareTokenInjections
	if nRare == 0 {
		nRare = defaultRareInjections(cfg.Rows)
	}
	rng := rand.New(rand.NewSource(cfg.Seed))

	rareIdx := make(map[int]struct{}, nRare)
	for len(rareIdx) < nRare && len(rareIdx) < cfg.Rows {
		rareIdx[rng.Intn(cfg.Rows)] = struct{}{}
	}

	streams := make([]Stream, cfg.Streams)
	for i := range streams {
		labels := []storage.Field{
			{Name: "service", Value: services[i%len(services)]},
			{Name: "pod", Value: fmt.Sprintf("%s-%d-%04x", services[i%len(services)], i/len(services), rng.Intn(1<<16))},
			{Name: "namespace", Value: []string{"prod", "staging"}[i%2]},
			{Name: "region", Value: regions[i%len(regions)]},
		}
		streams[i] = Stream{ID: storage.StreamIDForLabels(labels), Labels: labels}
	}

	ds := &Dataset{
		Rows:    make([]storage.Row, 0, cfg.Rows),
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

		fields := []storage.Field{
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

		r := storage.Row{Timestamp: ts, StreamID: st.ID, Fields: fields}
		ds.LogicalBytes += int64(r.LogicalSizeBytes())
		ds.Rows = append(ds.Rows, r)
	}
	return ds
}

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
