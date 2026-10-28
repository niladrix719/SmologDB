package agent

// ---------------------------------------------------------------------------
// STAGE 12 -- YOUR IMPLEMENTATION GOES HERE.
//
// Verified by:  go test ./agent -run TestStage12 -v     (parsing, handler)
//               go test ./harness -run TestStage12 -v   (end to end)
//
// Reference: app/vlinsert/jsonline/jsonline.go (the handler) and
// app/vlinsert/insertutil/{common_params.go,timestamp.go} (the query args and
// the timestamp rules). The real handler is the same shape as this one; what
// differs is that it streams and has a write-concurrency limiter.
// ---------------------------------------------------------------------------

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/niladrix719/minilog/minilog"
)

// The agent is the ingestion component, and until now nothing could feed it
// except a Go call. This file is the first front door: an HTTP endpoint that
// takes newline-delimited JSON, turns each line into a minilog.Row, and hands
// the rows to a sink.
//
//	curl -X POST --data-binary @app.jsonl \
//	     'http://127.0.0.1:9000/insert/jsonline?_stream_fields=app'
//
// A raw log line is not a Row. A Row needs a StreamID, a nanosecond timestamp
// and fields sorted by name, and a log line has none of those. Producing them
// is the whole job, and it is where the sources differ from each other.

// RowSink is where parsed rows go. *Agent satisfies it.
//
// It is an interface for a concrete reason, not a speculative one: the tests
// hand the receiver a recorder, so parsing and HTTP behaviour are checked
// without a cluster behind them.
type RowSink interface {
	MustAddRows(rows []minilog.Row)
}

// FlushSink is a RowSink that can also be told "put everything on disk now".
// The file tailer needs it for checkpoint ordering; see tailer.go. *Agent
// satisfies it.
type FlushSink interface {
	RowSink
	MustFlush()
}

const (
	// MaxBodyBytes bounds one request. Larger is refused with 413.
	MaxBodyBytes = 64 << 20

	// MaxLineBytes bounds one JSON line. A longer line fails the request.
	//
	// This is here because of a trap, not because 1MB is special:
	// bufio.Scanner's default token limit is 64KB, and when a line exceeds it
	// the scan simply STOPS and Err() reports it. Code that never checks Err()
	// silently ingests the lines before the long one and drops everything
	// after, and answers 200.
	MaxLineBytes = 1 << 20
)

// ParseParams says how to turn one JSON object into a Row.
type ParseParams struct {
	// StreamFields are the names whose values form the stream labels. A
	// stream is the unit of locality: rows with the same labels share a
	// StreamID and end up in the same blocks, and routing (stage 7) keys on
	// it. Choose labels that are low-cardinality and describe the SOURCE --
	// app, host, namespace -- never a request id.
	//
	// Empty means every row is in one stream. That is legal and sometimes
	// right; it is also the Zipf problem from stage 7 in its purest form.
	StreamFields []string

	// MsgFields: the first one present (and non-empty) in the object becomes
	// the field "_msg". Empty means ["_msg"].
	MsgFields []string

	// TimeFields: the first one present is the row's timestamp, and is
	// REMOVED from the fields -- it has been consumed. Empty means ["_time"].
	TimeFields []string
}

// withDefaults returns p with the two empty defaults filled in. COMPLETE.
func (p ParseParams) withDefaults() ParseParams {
	if len(p.MsgFields) == 0 {
		p.MsgFields = []string{"_msg"}
	}
	if len(p.TimeFields) == 0 {
		p.TimeFields = []string{"_time"}
	}
	return p
}

// ParseJSONLine turns one JSON object into a Row. now is the current time in
// unix nanoseconds, passed in so the result is deterministic.
//
// The rules, in the order they will bite you:
//
//  1. The line must be a JSON OBJECT. An array, a string, a number, or
//     invalid JSON is an error.
//
//  2. Decode numbers WITHOUT going through float64. A float64 has 53 bits of
//     mantissa; a unix-nanosecond timestamp needs 61. Decoding
//     {"_time":1714557600123456789} the obvious way gives you
//     1714557600123456768 -- off by 21 nanoseconds, silently, and every
//     "sorted by timestamp" guarantee downstream is now about the wrong
//     numbers. json.Decoder.UseNumber() keeps the literal text.
//
//  3. Flatten nested objects into dotted names: {"a":{"b":1}} -> "a.b"="1".
//     Arrays are kept whole as compact JSON text: [1,2] -> "[1,2]". true and
//     false become "true"/"false". Numbers keep their literal text. null is
//     omitted.
//
//  4. A field with an EMPTY value is the same as an absent field. Drop it.
//
//  5. Time: the first name in p.TimeFields that is present is parsed with
//     parseTimestamp and then REMOVED from the fields. If none is present, or
//     it is empty, the timestamp is now.
//
//  6. Message: the first name in p.MsgFields that is present and non-empty is
//     renamed to "_msg". Any other message-like field stays as it is.
//
//  7. Stream: labels are the (name, value) pairs for the names in
//     p.StreamFields that are present in THIS object. StreamID is
//     minilog.StreamIDForLabels(labels) -- which sorts for you, so the order
//     of keys in the JSON never changes the stream. The stream fields also
//     stay in Fields.
//
//  8. Fields come out SORTED BY NAME. Row's own doc comment says the block
//     writer depends on it; a Row with unsorted fields is not an error here,
//     it is a corrupted block three stages later.
func ParseJSONLine(line []byte, p *ParseParams, now int64) (minilog.Row, error) {
	var row minilog.Row
	var err error
	pp := p.withDefaults()
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var obj map[string]any
	err = dec.Decode(&obj)
	if err != nil || obj == nil {
		return minilog.Row{}, fmt.Errorf("invalid json object: %v", err)
	}
	out := map[string]string{}
	flatenObject("", obj, out)
	row.Timestamp = now
	for _, t := range pp.TimeFields {
		v, ok := out[t]
		if ok {
			row.Timestamp, err = parseTimestamp(v, now)
			if err != nil {
				return minilog.Row{}, err
			}
			delete(out, t)
			break
		}
	}
	for _, m := range pp.MsgFields {
		v, ok := out[m]
		if ok {
			out["_msg"] = v
			if m != "_msg" {
				delete(out, m)
			}
			break
		}
	}
	var labels []minilog.Field
	for _, s := range pp.StreamFields {
		v, ok := out[s]
		if ok {
			labels = append(labels, minilog.Field{Name: s, Value: v})
		}
	}
	row.StreamID = minilog.StreamIDForLabels(labels)

	row.Fields = make([]minilog.Field, len(out))
	i := 0
	for k, v := range out {
		row.Fields[i] = minilog.Field{Name: k, Value: v}
		i++
	}
	slices.SortFunc(row.Fields, func(a, b minilog.Field) int {
		return strings.Compare(a.Name, b.Name)
	})
	return row, nil
}

func flatenObject(prefix string, obj any, out map[string]string) {
	switch obj := obj.(type) {
	case map[string]any:
		for k, v := range obj {
			name := k
			if prefix != "" {
				name = prefix + "." + k
			}
			flatenObject(name, v, out)
		}
	case string:
		if obj != "" {
			out[prefix] = obj
		}
	case json.Number:
		out[prefix] = obj.String()
	case bool:
		out[prefix] = strconv.FormatBool(obj)
	case []any:
		b, _ := json.Marshal(obj)
		out[prefix] = string(b)
	}
}

// parseTimestamp turns a timestamp string into unix nanoseconds.
//
//	""  "0"  "-"      -> now           ("-" is syslog's "no timestamp")
//	"2024-05-01T10:00:00.5Z"  -> RFC3339 with optional fractional seconds
//	"1714557600"      -> unix seconds
//	"1714557600123"   -> unix milliseconds
//	"1714557600123456"-> unix microseconds
//	"1714557600123456789" -> unix nanoseconds
//	anything else     -> an error
//
// How to tell RFC3339 from a number: the real one checks whether the 5th
// character is '-' (len > 4 and s[4] == '-'). A year is four digits, so a
// date has a '-' there and a unix number never does.
//
// How to tell the unit of a number: by magnitude. Use these boundaries:
//
//	n < 1e11          seconds        (1e11 seconds is the year 5138)
//	n < 1e14          milliseconds
//	n < 1e17          microseconds
//	otherwise         nanoseconds
//
// Today's values land at about 1.7e9, 1.7e12, 1.7e15 and 1.7e18 -- one in each
// band, which is the reason the bands are 1e3 apart and not something clever.
func parseTimestamp(s string, now int64) (int64, error) {
	var t time.Time
	if s == "" || s == "-" || s == "0" {
		return now, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		mult := int64(1)
		switch {
		case n < 1e11:
			mult = 1e9
		case n < 1e14:
			mult = 1e6
		case n < 1e17:
			mult = 1e3
		}
		if n > math.MaxInt64/mult {
			return 0, fmt.Errorf("timestamp %q out of range", s)
		}
		return n * mult, nil
	}
	t, err = time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("unrecognized timestamp %q", s)
	}
	return t.UnixNano(), nil
}

// receiver is the HTTP handler. Build it with NewReceiver.
type receiver struct {
	sink     RowSink
	defaults ParseParams
}

// NewReceiver returns a handler serving POST /insert/jsonline. COMPLETE.
//
// defaults may be nil. Every request can override them with query args; see
// ServeHTTP.
func NewReceiver(sink RowSink, defaults *ParseParams) http.Handler {
	rc := &receiver{sink: sink}
	if defaults != nil {
		rc.defaults = *defaults
	}
	return rc
}

// ServeHTTP handles one request.
//
// Routing: only POST /insert/jsonline. Another path is 404, so this handler
// can be mounted at "/" next to others; another method on that path is 405.
//
// Query args, which override the defaults for this request only, all
// comma-separated lists (queryParams below does the splitting):
//
//	_stream_fields   -> ParseParams.StreamFields
//	_msg_field       -> ParseParams.MsgFields
//	_time_field      -> ParseParams.TimeFields
//
// The body is one JSON object per line. Blank lines are skipped.
//
// ATOMICITY, which is the decision to think about: parse EVERY line first,
// and only then call sink.MustAddRows ONCE with all of them. If line 7 of 10
// is bad, answer 400 naming "line 7" and hand the sink NOTHING. The
// alternative -- ingest lines 1 to 6, fail on 7 -- leaves the client
// with a request that "failed" and a log store that has half of it, and the
// client's retry then stores 1 to 6 twice. Rejecting the whole batch keeps a
// retry safe. (The real one streams and so cannot do this; that is the cost
// of streaming, and it is why a collector's retry policy matters there.)
//
// Limits: wrap the body in http.MaxBytesReader(w, r.Body, MaxBodyBytes) and
// answer 413 if it trips. Use bufio.Scanner with scanner.Buffer(...,
// MaxLineBytes) and CHECK scanner.Err() -- see MaxLineBytes for why.
//
// Success is 200 with an empty body.
//
// Extension, not tested: Content-Encoding: gzip, which every real shipper
// sends. compress/gzip is in the standard library.
func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var rows []minilog.Row
	var row minilog.Row
	var err error
	if r.URL.Path != "/insert/jsonline" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pp := queryParams(r, rc.defaults)
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(nil, MaxLineBytes)
	n := 0
	for sc.Scan() {
		n++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		row, err = ParseJSONLine(line, &pp, time.Now().UnixNano())
		if err != nil {
			http.Error(w, fmt.Sprintf("line %d: %v", n, err), http.StatusBadRequest)
			return
		}
		rows = append(rows, row)
	}
	err = sc.Err()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc.sink.MustAddRows(rows)
}

// queryParams returns defaults with any of the three query args applied.
// COMPLETE.
func queryParams(r *http.Request, defaults ParseParams) ParseParams {
	q := r.URL.Query()
	if v := q.Get("_stream_fields"); v != "" {
		defaults.StreamFields = splitList(v)
	}
	if v := q.Get("_msg_field"); v != "" {
		defaults.MsgFields = splitList(v)
	}
	if v := q.Get("_time_field"); v != "" {
		defaults.TimeFields = splitList(v)
	}
	return defaults.withDefaults()
}

// splitList splits a comma-separated list, dropping empty items. COMPLETE.
func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
