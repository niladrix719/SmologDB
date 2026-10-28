package agent

// STAGE 12 -- COMPLETE. The tailer's pure pieces: reading lines, and the
// checkpoint file.

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func strs(lines [][]byte) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = string(l)
	}
	return out
}

func TestStage12ReadNewLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	write := func(s string) {
		if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	read := func(offset int64) ([]string, int64) {
		lines, off, err := readNewLines(path, offset)
		if err != nil {
			t.Fatalf("readNewLines(%d): %v", offset, err)
		}
		return strs(lines), off
	}

	// An unfinished last line is left alone.
	write("a\nbb\nccc")
	lines, off := read(0)
	if !slices.Equal(lines, []string{"a", "bb"}) || off != 5 {
		t.Fatalf("got %v, offset %d; want [a bb], 5. The partial line 'ccc' has no newline yet\n"+
			"and must not be consumed, or its other half arrives later as a second fragment.", lines, off)
	}

	// The writer finishes it. Resume from the returned offset.
	write("a\nbb\ncccc\nd\n")
	lines, off = read(5)
	if !slices.Equal(lines, []string{"cccc", "d"}) || off != 12 {
		t.Fatalf("got %v, offset %d; want [cccc d], 12", lines, off)
	}

	// Nothing new.
	lines, off = read(12)
	if len(lines) != 0 || off != 12 {
		t.Fatalf("idle read returned %v, offset %d", lines, off)
	}

	// Truncated below the offset: start over.
	write("x\n")
	lines, off = read(12)
	if !slices.Equal(lines, []string{"x"}) || off != 2 {
		t.Fatalf("after truncation got %v, offset %d; want [x], 2.\n"+
			"A file smaller than the offset was truncated or replaced: restart from 0, or the\n"+
			"tailer waits forever at an offset the file will never reach again.", lines, off)
	}

	// CRLF and blank lines.
	write("a\r\n\n\nb\n")
	lines, off = read(0)
	if !slices.Equal(lines, []string{"a", "b"}) || off != 7 {
		t.Fatalf("got %v, offset %d; want [a b], 7 (CR stripped, blanks skipped but consumed)", lines, off)
	}

	// A file that does not exist yet.
	missing, moff, err := readNewLines(filepath.Join(dir, "later.log"), 7)
	if err != nil || len(missing) != 0 || moff != 7 {
		t.Fatalf("missing file: %v, %d, %v; want no lines, offset unchanged, no error", strs(missing), moff, err)
	}
}

func TestStage12ReadNewLinesBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.log")
	line := "0123456789abcdef\n" // 17 bytes
	n := (maxReadPerPoll/len(line))*2 + 10
	buf := make([]byte, 0, n*len(line))
	for i := 0; i < n; i++ {
		buf = append(buf, line...)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	var off int64
	var total, calls int
	for off < int64(len(buf)) {
		lines, next, err := readNewLines(path, off)
		if err != nil || next <= off {
			t.Fatalf("no progress at offset %d: next=%d err=%v", off, next, err)
		}
		if next-off > maxReadPerPoll {
			t.Fatalf("one call consumed %d bytes, over maxReadPerPoll=%d", next-off, maxReadPerPoll)
		}
		total += len(lines)
		off = next
		calls++
	}
	if total != n || calls < 2 {
		t.Fatalf("read %d lines in %d calls, want %d lines in at least 2", total, calls, n)
	}
}

func TestStage12MustWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.json")
	mustWriteFileAtomic(path, []byte("one"))
	mustWriteFileAtomic(path, []byte("two"))
	if got, _ := os.ReadFile(path); string(got) != "two" {
		t.Fatalf("contents %q, want %q", got, "two")
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		t.Fatalf("%d files in the directory after two writes; the temp file must be renamed away", len(ents))
	}
}

func TestStage12Checkpoints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.json")

	// Missing: empty, non-nil, assignable.
	cps := loadCheckpoints(path)
	if cps == nil || len(cps) != 0 {
		t.Fatalf("missing file: got %v, want an empty non-nil map", cps)
	}
	cps["/var/log/a.log"] = 1234

	mustSaveCheckpoints(path, map[string]int64{"/var/log/a.log": 1234, "/var/log/b.log": 7})
	got := loadCheckpoints(path)
	if got["/var/log/a.log"] != 1234 || got["/var/log/b.log"] != 7 || len(got) != 2 {
		t.Fatalf("round trip gave %v", got)
	}

	// Overwrite, not append.
	mustSaveCheckpoints(path, map[string]int64{"/var/log/a.log": 99})
	if got := loadCheckpoints(path); len(got) != 1 || got["/var/log/a.log"] != 99 {
		t.Fatalf("second save gave %v", got)
	}

	// Corrupt: logged, empty, no panic.
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := loadCheckpoints(path); got == nil || len(got) != 0 {
		t.Fatalf("corrupt file: got %v, want an empty non-nil map", got)
	}
}
