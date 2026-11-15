package storage

import (
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
)

type Field struct {
	Name  string
	Value string
}

type Row struct {
	Timestamp int64

	StreamID uint64

	Fields []Field
}

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

func SortRows(rows []Row) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].StreamID != rows[j].StreamID {
			return rows[i].StreamID < rows[j].StreamID
		}
		return rows[i].Timestamp < rows[j].Timestamp
	})
}

func (r *Row) GetField(name string) (string, bool) {
	for i := range r.Fields {
		if r.Fields[i].Name == name {
			return r.Fields[i].Value, true
		}
	}
	return "", false
}

func (r *Row) LogicalSizeBytes() int {

	n := 2 + 30
	for i := range r.Fields {

		n += len(r.Fields[i].Name) + len(r.Fields[i].Value) + 6
	}
	return n
}
