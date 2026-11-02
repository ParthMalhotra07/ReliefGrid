//go:build seams

package storagenode

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// seamsBuild reports whether this binary was built with the seams tag.
const seamsBuild = true

// seamArmer is the node side of the seam routes (*raftstore.Node in the
// seams build).
type seamArmer interface {
	ArmSeam(spec raftstore.SeamSpec) (*raftstore.Seam, error)
}

var _ seamArmer = (*raftstore.Node)(nil)

// seamRegistry is this process's armed boundaries by id. It lives in
// memory: a restarted pod has none.
type seamRegistry struct {
	mu    sync.Mutex
	next  int
	seams map[string]*raftstore.Seam
}

func (r *seamRegistry) add(s *raftstore.Seam) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seams == nil {
		r.seams = map[string]*raftstore.Seam{}
	}
	r.next++
	id := strconv.Itoa(r.next)
	r.seams[id] = s
	return id
}

func (r *seamRegistry) get(id string) *raftstore.Seam {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seams[id]
}

type armSeamRequest struct {
	Kind         testseam.Kind `json:"kind"`
	OperationIDs []string      `json:"operationIds,omitempty"`
	TimeoutMs    int64         `json:"timeoutMs"`
}

type seamResponse struct {
	ID    string          `json:"id"`
	Kind  testseam.Kind   `json:"kind"`
	State testseam.State  `json:"state"`
	Point *testseam.Point `json:"point,omitempty"`
	// Appended is a log barrier's operations in the log so far, so the
	// runner can propose B only once A is appended.
	Appended map[string]uint64 `json:"appended,omitempty"`
}

func seamStatus(id string, s *raftstore.Seam) seamResponse {
	st := s.Hold.Status()
	out := seamResponse{ID: id, Kind: st.Kind, State: st.State, Point: st.Point}
	if s.Barrier != nil {
		out.Appended = maps.Clone(s.Barrier.Indexes())
	}
	return out
}

// registerSeamRoutes adds the test-seam routes (S4.6b, #88) to the mgmt
// mux. Operator token required:
//
//	POST /mgmt/test/seams                 {kind, operationIds, timeoutMs} → 201 {id, kind, state}
//	GET  /mgmt/test/seams/{id}            {id, kind, state, point, appended}
//	POST /mgmt/test/seams/{id}/release    → the seam's status
//	POST /mgmt/test/seams/{id}/abort      → the seam's status
func registerSeamRoutes(mux *http.ServeMux, s *server) {
	mux.HandleFunc("POST /mgmt/test/seams", s.armSeam)
	mux.HandleFunc("GET /mgmt/test/seams/{id}", s.seamOp(nil))
	mux.HandleFunc("POST /mgmt/test/seams/{id}/release", s.seamOp((*testseam.Hold).Release))
	mux.HandleFunc("POST /mgmt/test/seams/{id}/abort", s.seamOp((*testseam.Hold).Abort))
}

func (s *server) armSeam(w http.ResponseWriter, r *http.Request) {
	if !s.operatorOnly(w, r) {
		return
	}
	var body armSeamRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeMgmtError(w, http.StatusBadRequest, "body: "+err.Error())
		return
	}
	if body.TimeoutMs <= 0 {
		writeMgmtError(w, http.StatusBadRequest, "timeoutMs is required and must be positive")
		return
	}
	n, ok := s.node.(seamArmer)
	if !ok {
		writeMgmtError(w, http.StatusNotImplemented, fmt.Sprintf("node %T cannot arm seams", s.node))
		return
	}
	seam, err := n.ArmSeam(raftstore.SeamSpec{Kind: body.Kind, OperationIDs: body.OperationIDs, Timeout: time.Duration(body.TimeoutMs) * time.Millisecond})
	if err != nil {
		writeMgmtError(w, http.StatusBadRequest, err.Error())
		return
	}
	id := s.seams.add(seam)
	writeJSON(w, http.StatusCreated, seamStatus(id, seam))
}

// seamOp answers with a seam's status after applying op (if any) to it.
func (s *server) seamOp(op func(*testseam.Hold)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.operatorOnly(w, r) {
			return
		}
		id := r.PathValue("id")
		seam := s.seams.get(id)
		if seam == nil {
			writeMgmtError(w, http.StatusNotFound, "no seam "+id)
			return
		}
		if op != nil {
			op(seam.Hold)
		}
		writeJSON(w, http.StatusOK, seamStatus(id, seam))
	}
}
