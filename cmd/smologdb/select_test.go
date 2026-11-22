package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/niladrix719/smologdb/storage"
)

func row(ts int64, msg string) storage.Row {
	return storage.Row{Timestamp: ts, StreamID: 1, Fields: []storage.Field{{Name: "_msg", Value: msg}}}
}

func get(t *testing.T, srv *httptest.Server, path string) (int, http.Header, []map[string]string) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var lines []map[string]string
	if resp.StatusCode == http.StatusOK {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			var m map[string]string
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				t.Fatalf("GET %s: line %q is not a JSON object of strings: %v", path, sc.Text(), err)
			}
			lines = append(lines, m)
		}
	} else {
		io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode, resp.Header, lines
}

func TestSelectEndpoint(t *testing.T) {
	s := storage.MustOpenStorage(t.TempDir(), &storage.Config{ShardsCount: 1})
	defer s.MustClose()
	now := time.Now().UnixNano()
	sec := int64(time.Second)
	s.MustAddRows([]storage.Row{
		row(now-2*int64(time.Hour), "ERROR ancient"),
		row(now-5*sec, "boot ok"),
		row(now-4*sec, "ERROR disk full"),
		row(now-3*sec, "request served"),
		row(now-2*sec, "ERROR timeout"),
		row(now-1*sec, "ERROR disk full again"),
	})
	s.MustForceFlush()
	srv := httptest.NewServer(newSelectHandler(s))
	defer srv.Close()

	code, header, lines := get(t, srv, "/select?q=ERROR&limit=2&since=1h")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if ct := header.Get("Content-Type"); !strings.HasPrefix(ct, "application/x-ndjson") {
		t.Errorf("Content-Type %q, want application/x-ndjson", ct)
	}
	if len(lines) != 2 || lines[0]["_msg"] != "ERROR disk full again" || lines[1]["_msg"] != "ERROR timeout" {
		t.Fatalf("got %v, want the 2 newest ERROR rows, newest first", lines)
	}
	if _, err := time.Parse(time.RFC3339Nano, lines[0]["_time"]); err != nil {
		t.Errorf("_time %q is not RFC 3339: %v", lines[0]["_time"], err)
	}

	if _, _, lines := get(t, srv, "/select?q=ancient&since=1h"); len(lines) != 0 {
		t.Errorf("since=1h returned %v, want nothing older than an hour", lines)
	}
	if _, _, lines := get(t, srv, "/select?q=ancient"); len(lines) != 1 {
		t.Errorf("no since returned %d rows for 'ancient', want 1 (no since means all time)", len(lines))
	}
	if _, _, lines := get(t, srv, "/select?q="+url.QueryEscape(`"disk full" -again`)); len(lines) != 1 || lines[0]["_msg"] != "ERROR disk full" {
		t.Errorf(`q="disk full" -again returned %v, want just "ERROR disk full"`, lines)
	}

	for _, bad := range []string{"/select?limit=abc", "/select?since=yesterday", "/select?q=" + url.QueryEscape(`"unclosed`)} {
		if code, _, _ := get(t, srv, bad); code != http.StatusBadRequest {
			t.Errorf("GET %s: status %d, want 400", bad, code)
		}
	}
}
