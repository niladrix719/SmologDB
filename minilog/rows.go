package minilog

import (
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// ---------------------------------------------------------------------------
// These are the logical types that cross the public API boundary. Everything
// below the API (blocks, columns, parts) is internal to the package.
// ---------------------------------------------------------------------------

// Field is a single name/value pair in a log entry.
//
// Note that Value is always a string. Typing happens at encode time, per
// column block -- see encoding.go. This is the "schema-free but typed"
// trick that VictoriaLogs uses.
type Field struct {
	Name  string
	Value string
}

// Row is a single log entry.
type Row struct {
	// Timestamp is unix nanoseconds.
	Timestamp int64

	// StreamID identifies the log stream this row belongs to.
	//
	// In VictoriaLogs this is (tenantID, u128 hash of sorted stream labels).
	// We simplify to a single uint64 hash -- the structural point (rows are
	// grouped and sorted by stream so that a stream's rows land in the same
	// blocks) is preserved.
	StreamID uint64

	// Fields are the log entry's fields, sorted by Name.
	//
	// Sorted order is a precondition for the block writer: it lets the writer
	// build columns with a merge rather than a map lookup per field.
	Fields []Field
}

// StreamIDForLabels computes a StreamID from a set of stream labels.
//
// Labels are canonically sorted before hashing so that label order in the
// input does not affect the resulting id.
func StreamIDForLabels(labels []Field) uint64 {
	sorted := make([]Field, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	var sb strings.Builder
	for _, l := range sorted {
		sb.WriteString(l.Name)
		sb.WriteByte('=')
		sb.WriteString(l.Value)
		sb.WriteByte(0)
	}
	return xxhash.Sum64String(sb.String())
}

// SortRows sorts rows by (StreamID, Timestamp), which is the order blocks
// must be built in.
func SortRows(rows []Row) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].StreamID != rows[j].StreamID {
			return rows[i].StreamID < rows[j].StreamID
		}
		return rows[i].Timestamp < rows[j].Timestamp
	})
}

// GetField returns the value of the named field, and whether it was present.
func (r *Row) GetField(name string) (string, bool) {
	for i := range r.Fields {
		if r.Fields[i].Name == name {
			return r.Fields[i].Value, true
		}
	}
	return "", false
}

// LogicalSizeBytes returns the approximate size of the row as raw JSON.
//
// The write-amplification and compression-ratio harnesses use this as the
// "logical bytes" denominator, so it must stay stable across your changes.
func (r *Row) LogicalSizeBytes() int {
	// {"_time":<20>,...}
	n := 2 + 30
	for i := range r.Fields {
		// "name":"value",
		n += len(r.Fields[i].Name) + len(r.Fields[i].Value) + 6
	}
	return n
}
