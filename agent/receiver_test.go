package agent

// STAGE 12 -- COMPLETE. Parsing rules and handler behaviour, no cluster needed.

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/niladrix719/minilog/minilog"
)

// recSink records what the receiver hands it.
type recSink struct {
	mu    sync.Mutex
	calls int
	rows  []minilog.Row
}

func (s *recSink) MustAddRows(rows []minilog.Row) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.rows = append(s.rows, rows...)
}

func fieldOf(r minilog.Row, name string) string {
	v, _ := r.GetField(name)
	return v
}

func names(r minilog.Row) []string {
	out := make([]string, len(r.Fields))
	for i, f := range r.Fields {
		out[i] = f.Name
	}
	return out
}

func TestStage12ParseJSONLine(t *testing.T) {
	const now = int64(42)
	may1 := time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC).UnixNano()

	t.Run("fields, stream, time", func(t *testing.T) {
		line := `{"level":"info","_msg":"hello","app":"web","_time":"2024-05-01T10:00:00Z"}`
		row, err := ParseJSONLine([]byte(line), &ParseParams{StreamFields: []string{"app"}}, now)
		if err != nil {
			t.Fatal(err)
		}
		if row.Timestamp != may1 {
			t.Fatalf("timestamp %d, want %d", row.Timestamp, may1)
		}
		if want := []string{"_msg", "app", "level"}; !slices.Equal(names(row), want) {
			t.Fatalf("fields %v, want %v: sorted by name, and _time removed once consumed", names(row), want)
		}
		if want := minilog.StreamIDForLabels([]minilog.Field{{Name: "app", Value: "web"}}); row.StreamID != want {
			t.Fatalf("StreamID %d, want the id of {app=web} = %d", row.StreamID, want)
		}
	})

	t.Run("nanosecond timestamp survives as a JSON number", func(t *testing.T) {
		// Above 2^53, so a float64 round trip would change the low digits.
		row, err := ParseJSONLine([]byte(`{"_time":1714557600123456789,"_msg":"x"}`), &ParseParams{}, now)
		if err != nil {
			t.Fatal(err)
		}
		if row.Timestamp != 1714557600123456789 {
			t.Fatalf("timestamp %d, want 1714557600123456789.\n"+
				"The number went through float64. Use json.Decoder.UseNumber().", row.Timestamp)
		}
	})

	t.Run("nested objects, arrays, bools, nulls, empties", func(t *testing.T) {
		line := `{"a":{"b":{"c":1}},"d":[1,2],"e":true,"f":null,"g":""}`
		row, err := ParseJSONLine([]byte(line), &ParseParams{}, now)
		if err != nil {
			t.Fatal(err)
		}
		want := []minilog.Field{{Name: "a.b.c", Value: "1"}, {Name: "d", Value: "[1,2]"}, {Name: "e", Value: "true"}}
		if !slices.Equal(row.Fields, want) {
			t.Fatalf("fields %v, want %v (null and empty are dropped)", row.Fields, want)
		}
		if row.Timestamp != now {
			t.Fatalf("no time field: timestamp %d, want now=%d", row.Timestamp, now)
		}
	})

	t.Run("message field mapping", func(t *testing.T) {
		row, err := ParseJSONLine([]byte(`{"msg":"B","message":"A"}`), &ParseParams{MsgFields: []string{"message", "msg"}}, now)
		if err != nil {
			t.Fatal(err)
		}
		if fieldOf(row, "_msg") != "A" || fieldOf(row, "msg") != "B" || fieldOf(row, "message") != "" {
			t.Fatalf("fields %v: the FIRST listed message field becomes _msg; the other stays", row.Fields)
		}
	})

	t.Run("no stream fields, or absent ones, means one stream", func(t *testing.T) {
		none := minilog.StreamIDForLabels(nil)
		a, _ := ParseJSONLine([]byte(`{"_msg":"x"}`), &ParseParams{}, now)
		b, _ := ParseJSONLine([]byte(`{"_msg":"x"}`), &ParseParams{StreamFields: []string{"app"}}, now)
		if a.StreamID != none || b.StreamID != none {
			t.Fatalf("StreamIDs %d, %d; both should be the empty-label stream %d", a.StreamID, b.StreamID, none)
		}
	})

	t.Run("key order never changes the stream or the field order", func(t *testing.T) {
		p := &ParseParams{StreamFields: []string{"b", "a"}}
		x, _ := ParseJSONLine([]byte(`{"z":"1","b":"2","a":"3"}`), p, now)
		y, _ := ParseJSONLine([]byte(`{"a":"3","z":"1","b":"2"}`), p, now)
		if x.StreamID != y.StreamID || !slices.Equal(x.Fields, y.Fields) || !slices.Equal(names(x), []string{"a", "b", "z"}) {
			t.Fatalf("x=%v y=%v", x, y)
		}
	})

	t.Run("errors", func(t *testing.T) {
		for _, bad := range []string{`[1,2]`, `"str"`, `12`, `{"a":`, ``, `not json`} {
			if _, err := ParseJSONLine([]byte(bad), &ParseParams{}, now); err == nil {
				t.Fatalf("%q parsed without error", bad)
			}
		}
		if _, err := ParseJSONLine([]byte(`{"_time":"garbage"}`), &ParseParams{}, now); err == nil {
			t.Fatalf("a time field that does not parse must be an error, not silently 'now'")
		}
	})
}

func TestStage12ParseTimestamp(t *testing.T) {
	const now = int64(777)
	for _, c := range []struct {
		in   string
		want int64
	}{
		{"", now},
		{"0", now},
		{"-", now},
		{"1714557600", 1714557600 * 1e9},
		{"1714557600123", 1714557600123 * 1e6},
		{"1714557600123456", 1714557600123456 * 1e3},
		{"1714557600123456789", 1714557600123456789},
		{"2024-05-01T10:00:00Z", time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC).UnixNano()},
		{"2024-05-01T10:00:00.5Z", time.Date(2024, 5, 1, 10, 0, 0, 5e8, time.UTC).UnixNano()},
		{"2024-05-01T12:00:00+02:00", time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC).UnixNano()},
	} {
		got, err := parseTimestamp(c.in, now)
		if err != nil || got != c.want {
			t.Fatalf("parseTimestamp(%q) = %d, %v; want %d", c.in, got, err, c.want)
		}
	}
	for _, bad := range []string{"garbage", "2024-13-45T00:00:00Z", "12ab"} {
		if _, err := parseTimestamp(bad, now); err == nil {
			t.Fatalf("parseTimestamp(%q) succeeded, want an error", bad)
		}
	}
}

func post(h http.Handler, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, strings.NewReader(body)))
	return rec
}

func TestStage12ReceiverHandler(t *testing.T) {
	const path = "/insert/jsonline"

	t.Run("good body is one batch, blank lines skipped", func(t *testing.T) {
		s := &recSink{}
		rec := post(NewReceiver(s, nil), path, `{"_msg":"a"}`+"\n\n"+`{"_msg":"b"}`+"\n"+`{"_msg":"c"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body)
		}
		if s.calls != 1 || len(s.rows) != 3 {
			t.Fatalf("sink got %d calls and %d rows; want ONE call with 3 rows", s.calls, len(s.rows))
		}
	})

	t.Run("a bad line rejects the whole request", func(t *testing.T) {
		s := &recSink{}
		rec := post(NewReceiver(s, nil), path, `{"_msg":"a"}`+"\n"+`{broken`+"\n"+`{"_msg":"c"}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status %d, want 400", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "line 2") {
			t.Fatalf("body %q should name the bad line", rec.Body)
		}
		if s.calls != 0 || len(s.rows) != 0 {
			t.Fatalf("sink got %d rows from a rejected request; a retry would now store them twice", len(s.rows))
		}
	})

	t.Run("method and path", func(t *testing.T) {
		s := &recSink{}
		h := NewReceiver(s, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET: status %d, want 405", rec.Code)
		}
		if rec := post(h, "/somewhere/else", `{"_msg":"a"}`); rec.Code != http.StatusNotFound {
			t.Fatalf("other path: status %d, want 404", rec.Code)
		}
		if s.calls != 0 {
			t.Fatalf("sink touched by a refused request")
		}
	})

	t.Run("query args override the defaults", func(t *testing.T) {
		s := &recSink{}
		rec := post(NewReceiver(s, nil),
			path+"?_stream_fields=app&_msg_field=message&_time_field=ts",
			`{"message":"m","app":"a","ts":"2024-05-01T10:00:00Z"}`)
		if rec.Code != http.StatusOK || len(s.rows) != 1 {
			t.Fatalf("status %d rows %d: %s", rec.Code, len(s.rows), rec.Body)
		}
		r := s.rows[0]
		if fieldOf(r, "_msg") != "m" || fieldOf(r, "ts") != "" {
			t.Fatalf("fields %v: message renamed, ts consumed", r.Fields)
		}
		if r.Timestamp != time.Date(2024, 5, 1, 10, 0, 0, 0, time.UTC).UnixNano() {
			t.Fatalf("timestamp %d not taken from ts", r.Timestamp)
		}
		if want := minilog.StreamIDForLabels([]minilog.Field{{Name: "app", Value: "a"}}); r.StreamID != want {
			t.Fatalf("StreamID %d, want %d", r.StreamID, want)
		}
	})

	t.Run("a line longer than 64KB works", func(t *testing.T) {
		s := &recSink{}
		long := strings.Repeat("x", 200_000)
		rec := post(NewReceiver(s, nil), path, `{"_msg":"`+long+`"}`+"\n"+`{"_msg":"after"}`)
		if rec.Code != http.StatusOK || len(s.rows) != 2 || len(fieldOf(s.rows[0], "_msg")) != 200_000 {
			t.Fatalf("status %d, %d rows. bufio.Scanner stops at 64KB unless you give it a bigger buffer,\n"+
				"and it stops SILENTLY unless you check Err().", rec.Code, len(s.rows))
		}
	})

	t.Run("a line over MaxLineBytes is refused, not truncated", func(t *testing.T) {
		s := &recSink{}
		rec := post(NewReceiver(s, nil), path, `{"_msg":"`+strings.Repeat("y", MaxLineBytes+10)+`"}`)
		if rec.Code/100 != 4 || s.calls != 0 {
			t.Fatalf("status %d, sink calls %d; want a 4xx and nothing ingested", rec.Code, s.calls)
		}
	})

	t.Run("no time field means a recent timestamp", func(t *testing.T) {
		s := &recSink{}
		post(NewReceiver(s, nil), path, `{"_msg":"a"}`)
		if len(s.rows) != 1 || time.Since(time.Unix(0, s.rows[0].Timestamp)) > time.Minute {
			t.Fatalf("rows %v", s.rows)
		}
	})
}
