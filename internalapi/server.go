package internalapi

import (
	"io"
	"log"
	"net/http"

	"github.com/niladrix719/smologdb/storage"
)

type StorageNodeHandler struct {
	s storage.LogStorage
}

func NewStorageNodeHandler(s storage.LogStorage) *StorageNodeHandler {
	return &StorageNodeHandler{s: s}
}

func (h *StorageNodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !CheckVersion(w, r) {
		return
	}
	switch r.URL.Path {
	case InsertPath:
		h.handleInsert(w, r)
	case SelectPath:
		h.handleSelect(w, r)
	case ForceFlushPath:
		h.handleForceFlush(w, r)
	default:
		http.NotFound(w, r)
		return
	}
}

func (h *StorageNodeHandler) handleInsert(w http.ResponseWriter, r *http.Request) {
	var rows []storage.Row
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		failedText := "failed to read request"
		http.Error(w, failedText, http.StatusBadRequest)
		return
	}
	rows, _, err = UnmarshalRowBatch(rows, bytes)
	if err != nil {
		failedText := "failed to insert rows"
		http.Error(w, failedText, http.StatusBadRequest)
		return
	}
	h.s.MustAddRows(rows)

	w.WriteHeader(http.StatusNoContent)
}

func (h *StorageNodeHandler) handleSelect(w http.ResponseWriter, r *http.Request) {
	var q storage.Query
	var rows []storage.Row
	var ss *storage.SearchStats
	var resBytes []byte
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		failedText := "failed to read request"
		http.Error(w, failedText, http.StatusBadRequest)
		return
	}
	_, err = UnmarshalQuery(&q, bytes)
	if err != nil {
		failedText := "failed to unmarshal query"
		http.Error(w, failedText, http.StatusBadRequest)
		return
	}
	rows, ss = h.s.Search(&q)
	resBytes = MarshalQueryResponse(resBytes, rows, ss)
	_, err = w.Write(resBytes)
	if err != nil {
		log.Printf("failed to send response: %v", err)
	}
}

func (h *StorageNodeHandler) handleForceFlush(w http.ResponseWriter, r *http.Request) {
	h.s.MustForceFlush()
	w.WriteHeader(http.StatusNoContent)
}

// CheckVersion rejects requests from a different protocol version, including ones that send none.
func CheckVersion(w http.ResponseWriter, r *http.Request) bool {
	got := r.URL.Query().Get(VersionArg)
	if got == ProtocolVersion {
		return true
	}
	http.Error(w, "unsupported protocol version="+got+"; this node speaks "+ProtocolVersion+
		"; a rolling upgrade is probably half-finished -- finish it", http.StatusBadRequest)
	return false
}
