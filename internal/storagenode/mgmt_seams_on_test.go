//go:build seams

package storagenode

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/testseam"
)

// seamNode is a fakeNode that arms real testseam boundaries.
type seamNode struct {
	*fakeNode
	specs []raftstore.SeamSpec
	seams []*raftstore.Seam
}

func (n *seamNode) ArmSeam(spec raftstore.SeamSpec) (*raftstore.Seam, error) {
	if spec.Kind == "nope" {
		return nil, errors.New("raftstore: unknown seam kind")
	}
	n.specs = append(n.specs, spec)
	s := &raftstore.Seam{Hold: testseam.NewHold(spec.Kind, spec.Timeout)}
	if spec.Kind == testseam.LogBarrier {
		s.Barrier = testseam.NewBarrier(spec.Timeout, func() uint64 { return 0 }, func() uint64 { return 0 }, spec.OperationIDs...)
		s.Hold = s.Barrier.Hold()
	}
	n.seams = append(n.seams, s)
	return s, nil
}

type seamJSON struct {
	ID       string            `json:"id"`
	Kind     testseam.Kind     `json:"kind"`
	State    testseam.State    `json:"state"`
	Point    *testseam.Point   `json:"point"`
	Appended map[string]uint64 `json:"appended"`
}

func arm(t *testing.T, h http.Handler, body any) seamJSON {
	t.Helper()
	rec := do(t, h, call{method: http.MethodPost, path: "/mgmt/test/seams", header: operatorAuth, body: body})
	if rec.Code != http.StatusCreated {
		t.Fatalf("arm %v: %d %s", body, rec.Code, rec.Body.String())
	}
	return decodeJSON[seamJSON](t, rec)
}

func get(t *testing.T, h http.Handler, id string) seamJSON {
	t.Helper()
	rec := do(t, h, call{method: http.MethodGet, path: "/mgmt/test/seams/" + id, header: operatorAuth})
	if rec.Code != http.StatusOK {
		t.Fatalf("get %s: %d %s", id, rec.Code, rec.Body.String())
	}
	return decodeJSON[seamJSON](t, rec)
}

func TestSeamsBuildReportsItself(t *testing.T) {
	h := newTestServer(t, &seamNode{fakeNode: newFakeNode(t)}).mgmtRoutes()
	st := decodeJSON[map[string]any](t, do(t, h, call{method: http.MethodGet, path: "/mgmt/status"}))
	if st["seams"] != true {
		t.Fatalf("status %v: a seams build must say so", st)
	}
}

func TestSeamRoutesRequireOperatorToken(t *testing.T) {
	n := &seamNode{fakeNode: newFakeNode(t)}
	s := newTestServer(t, n)
	body := map[string]any{"kind": "snapshot-persist", "timeoutMs": 1000}
	if rec := do(t, s.mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/test/seams", body: body}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d, want 401", rec.Code)
	}
	s.operatorToken = ""
	if rec := do(t, s.mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/test/seams", header: operatorAuth, body: body}); rec.Code != http.StatusForbidden {
		t.Fatalf("no token configured: %d, want 403", rec.Code)
	}
	if len(n.specs) != 0 {
		t.Fatal("armed without a valid operator token")
	}
}

func TestSeamRoutesRejectBadRequests(t *testing.T) {
	n := &seamNode{fakeNode: newFakeNode(t)}
	h := newTestServer(t, n).mgmtRoutes()
	for name, body := range map[string]any{
		"not JSON":      "{",
		"unknown field": map[string]any{"kind": "snapshot-persist", "timeoutMs": 1000, "extra": 1},
		"no timeout":    map[string]any{"kind": "snapshot-persist"},
		"node refuses":  map[string]any{"kind": "nope", "timeoutMs": 1000},
	} {
		if rec := do(t, h, call{method: http.MethodPost, path: "/mgmt/test/seams", header: operatorAuth, body: body}); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s, want 400", name, rec.Code, rec.Body.String())
		}
	}
	if rec := do(t, h, call{method: http.MethodGet, path: "/mgmt/test/seams/99", header: operatorAuth}); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown seam: %d, want 404", rec.Code)
	}
}

// Arm, observe reached with its point, release; and abort another.
func TestSeamRoutesArmReachRelease(t *testing.T) {
	n := &seamNode{fakeNode: newFakeNode(t)}
	h := newTestServer(t, n).mgmtRoutes()
	s := arm(t, h, map[string]any{"kind": "snapshot-persist", "timeoutMs": 5000})
	if s.ID == "" || s.Kind != testseam.SnapshotPersist || s.State != testseam.Armed {
		t.Fatalf("armed %+v", s)
	}
	if len(n.specs) != 1 || n.specs[0].Timeout != 5*time.Second {
		t.Fatalf("node got %+v, want one 5s snapshot-persist spec", n.specs)
	}
	done := make(chan error, 1)
	go func() { done <- n.seams[0].Hold.Reach(testseam.Point{ImageIndex: 9, Bytes: 1, Size: 2}) }()
	deadline := time.Now().Add(5 * time.Second)
	for get(t, h, s.ID).State != testseam.Reached {
		if time.Now().After(deadline) {
			t.Fatal("never reported reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if p := get(t, h, s.ID).Point; p == nil || p.ImageIndex != 9 || p.Bytes != 1 || p.Size != 2 {
		t.Fatalf("point %+v", p)
	}
	rec := do(t, h, call{method: http.MethodPost, path: "/mgmt/test/seams/" + s.ID + "/release", header: operatorAuth})
	if rec.Code != http.StatusOK || decodeJSON[seamJSON](t, rec).State != testseam.Released {
		t.Fatalf("release: %d %s", rec.Code, rec.Body.String())
	}
	if err := <-done; err != nil {
		t.Fatalf("released Reach: %v", err)
	}

	s2 := arm(t, h, map[string]any{"kind": "restore-stage", "timeoutMs": 5000})
	if s2.ID == s.ID {
		t.Fatal("two seams share an id")
	}
	go func() { done <- n.seams[1].Hold.Reach(testseam.Point{}) }()
	if _, err := n.seams[1].Hold.WaitReached(t.Context()); err != nil {
		t.Fatal(err)
	}
	rec = do(t, h, call{method: http.MethodPost, path: "/mgmt/test/seams/" + s2.ID + "/abort", header: operatorAuth})
	if rec.Code != http.StatusOK || decodeJSON[seamJSON](t, rec).State != testseam.Aborted {
		t.Fatalf("abort: %d %s", rec.Code, rec.Body.String())
	}
	if err := <-done; !errors.Is(err, testseam.ErrAborted) {
		t.Fatalf("aborted Reach: %v", err)
	}
}

// The log barrier's appended operations are visible before it is reached,
// so the runner can send B only once A is in the log.
func TestSeamRoutesLogBarrierReportsAppended(t *testing.T) {
	n := &seamNode{fakeNode: newFakeNode(t)}
	h := newTestServer(t, n).mgmtRoutes()
	s := arm(t, h, map[string]any{"kind": "log-barrier", "operationIds": []string{"op-a", "op-b"}, "timeoutMs": 5000})
	if got := n.specs[0].OperationIDs; len(got) != 2 || got[0] != "op-a" {
		t.Fatalf("node got operationIds %v", got)
	}
	n.seams[0].Barrier.Appended(12, "op-a")
	if got := get(t, h, s.ID); got.State != testseam.Armed || got.Appended["op-a"] != 12 {
		t.Fatalf("barrier %+v, want armed with op-a appended at 12", got)
	}
}
