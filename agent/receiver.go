package agent

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

	"github.com/niladrix719/smologdb/storage"
)

type RowSink interface {
	MustAddRows(rows []storage.Row)
}

type FlushSink interface {
	RowSink
	MustFlush()
}

const (
	MaxBodyBytes = 64 << 20

	MaxLineBytes = 1 << 20
)

type ParseParams struct {
	StreamFields []string

	MsgFields []string

	TimeFields []string
}

func (p ParseParams) withDefaults() ParseParams {
	if len(p.MsgFields) == 0 {
		p.MsgFields = []string{"_msg"}
	}
	if len(p.TimeFields) == 0 {
		p.TimeFields = []string{"_time"}
	}
	return p
}

func ParseJSONLine(line []byte, p *ParseParams, now int64) (storage.Row, error) {
	var row storage.Row
	var err error
	pp := p.withDefaults()
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber() // float64 would round nanosecond timestamps
	var obj map[string]any
	err = dec.Decode(&obj)
	if err != nil || obj == nil {
		return storage.Row{}, fmt.Errorf("invalid json object: %v", err)
	}
	out := map[string]string{}
	flatenObject("", obj, out)
	row.Timestamp = now
	for _, t := range pp.TimeFields {
		v, ok := out[t]
		if ok {
			row.Timestamp, err = parseTimestamp(v, now)
			if err != nil {
				return storage.Row{}, err
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
	var labels []storage.Field
	for _, s := range pp.StreamFields {
		v, ok := out[s]
		if ok {
			labels = append(labels, storage.Field{Name: s, Value: v})
		}
	}
	row.StreamID = storage.StreamIDForLabels(labels)

	row.Fields = make([]storage.Field, len(out))
	i := 0
	for k, v := range out {
		row.Fields[i] = storage.Field{Name: k, Value: v}
		i++
	}
	slices.SortFunc(row.Fields, func(a, b storage.Field) int {
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

type receiver struct {
	sink     RowSink
	defaults ParseParams
}

func NewReceiver(sink RowSink, defaults *ParseParams) http.Handler {
	rc := &receiver{sink: sink}
	if defaults != nil {
		rc.defaults = *defaults
	}
	return rc
}

func (rc *receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var rows []storage.Row
	var row storage.Row
	var err error
	if r.URL.Path != "/ingest" {
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
	// Rows go to the sink only after every line parsed, so a bad line rejects the whole request
	// and a client retry can't store the good half twice.
	rc.sink.MustAddRows(rows)
}

func queryParams(r *http.Request, defaults ParseParams) ParseParams {
	q := r.URL.Query()
	if v := q.Get("stream"); v != "" {
		defaults.StreamFields = splitList(v)
	}
	if v := q.Get("msg_field"); v != "" {
		defaults.MsgFields = splitList(v)
	}
	if v := q.Get("time_field"); v != "" {
		defaults.TimeFields = splitList(v)
	}
	return defaults.withDefaults()
}

func splitList(s string) []string {
	var out []string
	for _, item := range strings.Split(s, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
