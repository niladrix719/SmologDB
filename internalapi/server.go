package internalapi

// ---------------------------------------------------------------------------
// Verified by:  go test ./harness -run TestStage6 -v
//
// Reference: app/vlinsert/internalinsert/internalinsert.go (the /internal/insert
// handler), app/vlselect/internalselect/internalselect.go (the
// /internal/select/* handlers), app/vlstorage/main.go (the routing table).
// ---------------------------------------------------------------------------

import (
	"io"
	"log"
	"net/http"

	"github.com/niladrix719/minilog/minilog"
)

// StorageNodeHandler serves the internal API on top of a local storage.
//
// This is the "vlstorage" role: it owns a shard of the data and answers
// questions about that shard only. It knows nothing about other nodes, has no
// address list, and cannot itself fan anything out. That ignorance is the
// design -- the moment a storage node needs to know about its peers you have
// acquired a membership problem, and with it every distributed-systems
// difficulty that CLUSTER.md explains VictoriaLogs avoids.
type StorageNodeHandler struct {
	s minilog.LogStorage
}

// NewStorageNodeHandler returns a handler serving s.
//
// It is used in two places and must behave identically in both: cmd/minilogd
// serves it over a real listener, and harness/cluster_test.go serves it over
// an httptest listener. If you find yourself wanting a "test mode" here, the
// test has stopped testing the thing that ships.
func NewStorageNodeHandler(s minilog.LogStorage) *StorageNodeHandler {
	return &StorageNodeHandler{s: s}
}

// ServeHTTP routes an internal request.
//
// Route table:
//
//	POST InsertPath      body = MarshalRowBatch     -> 204
//	POST SelectPath      body = MarshalQuery        -> 200, MarshalQueryResponse
//	GET  ForceFlushPath  no body                    -> 204
//
// Do the version check FIRST, before reading the body, and reject a mismatch
// with 400 and a message naming both versions. A storage node that reads a
// body it cannot interpret has already lost -- and the error message is what
// somebody stares at during a half-finished rolling upgrade at 3am, so make it
// say what it means.
//
// Status codes matter more than usual here, because stage 9 asks netselect to
// distinguish "this node is down" from "this node answered with an error", and
// the only signal it has is what comes back. Be deliberate:
//
//	400  the request was wrong (bad version, undecodable body). Never retry.
//	500  this node broke while doing valid work.
//
// Neither of those is "unavailable" -- unavailability is a transport failure,
// with no HTTP response at all. Stage 9 leans on that distinction.
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

// handleInsert reads a row batch and hands it to the local storage.
//
// Note what this does NOT do: it does not re-route, re-shard, or validate that
// the rows "belong" here. Whatever arrives is stored. That is what makes
// netinsert's re-routing in stage 9 work at all -- a rerouted block lands on a
// node the router would never have chosen, and the node must not care.
func (h *StorageNodeHandler) handleInsert(w http.ResponseWriter, r *http.Request) {
	var rows []minilog.Row
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

// handleSelect runs a query against the local storage and writes the response.
//
// The rows written must be sorted by (StreamID, Timestamp) -- Storage.Search
// already guarantees that, and netselect's merge depends on it. If a node ever
// returns unsorted rows the merge produces a plausible-looking wrong answer
// rather than an error, which is the worst failure shape available.
//
// Apply q.Limit here too (stage 10). Until then it is fine to ignore it; the
// stage 10 harness will tell you when it stops being fine.
func (h *StorageNodeHandler) handleSelect(w http.ResponseWriter, r *http.Request) {
	var q minilog.Query
	var rows []minilog.Row
	var ss *minilog.SearchStats
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

// handleForceFlush makes previously inserted rows queryable.
func (h *StorageNodeHandler) handleForceFlush(w http.ResponseWriter, r *http.Request) {
	h.s.MustForceFlush()
	w.WriteHeader(http.StatusNoContent)
}

// CheckVersion reports whether r carries a compatible protocol version, and
// writes the rejection itself if not.
//
// Getting this subtly wrong (defaulting an absent version to "compatible",
// say) defeats the entire point of having a version.
func CheckVersion(w http.ResponseWriter, r *http.Request) bool {
	got := r.URL.Query().Get(VersionArg)
	if got == ProtocolVersion {
		return true
	}
	http.Error(w, "unsupported protocol version="+got+"; this node speaks "+ProtocolVersion+
		"; a rolling upgrade is probably half-finished -- finish it", http.StatusBadRequest)
	return false
}
