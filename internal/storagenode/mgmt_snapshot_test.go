package storagenode

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Forcing a snapshot and dumping every row are operator-only, like
// bootstrap: a dump exposes all of the shard's data.
func TestMgmtSnapshotAndDumpRequireOperatorToken(t *testing.T) {
	for _, ep := range []struct{ method, path string }{
		{http.MethodPost, "/mgmt/snapshot"},
		{http.MethodGet, "/mgmt/dump"},
	} {
		for _, tc := range []struct {
			name       string
			configured string
			header     map[string]string
			status     int
		}{
			{"no token configured", "", operatorAuth, http.StatusForbidden},
			{"no Authorization header", testToken, nil, http.StatusUnauthorized},
			{"wrong token", testToken, map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized},
			{"operator token", testToken, operatorAuth, http.StatusOK},
		} {
			t.Run(ep.path+"/"+tc.name, func(t *testing.T) {
				n := newFakeNode(t)
				s := newTestServer(t, n)
				s.operatorToken = tc.configured
				rec := do(t, s.mgmtRoutes(), call{method: ep.method, path: ep.path, header: tc.header})
				if rec.Code != tc.status {
					t.Fatalf("status %d (%s), want %d", rec.Code, rec.Body.String(), tc.status)
				}
				// The 403 names no single endpoint: every operator route shares it.
				if tc.configured == "" && strings.Contains(rec.Body.String(), "bootstrap") {
					t.Fatalf("403 body %s names bootstrap", rec.Body.String())
				}
				if tc.status != http.StatusOK && (n.snaps != 0 || len(n.modes) != 0) {
					t.Fatal("ran without a valid operator token")
				}
			})
		}
	}
}

func TestMgmtSnapshot(t *testing.T) {
	t.Run("taken", func(t *testing.T) {
		n := newFakeNode(t)
		n.snapInfo = raftstore.SnapshotInfo{ID: "2-1100-1792", RaftIndex: 1100, AppliedIndex: 1098}
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/snapshot", header: operatorAuth})
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		got := decodeJSON[snapshotResponse](t, rec)
		want := snapshotResponse{PodName: testPod, SnapshotID: "2-1100-1792", RaftIndex: 1100, AppliedIndex: 1098}
		if got != want || n.snaps != 1 {
			t.Fatalf("snapshot = %+v after %d calls, want %+v after 1", got, n.snaps, want)
		}
	})
	t.Run("failure is 500", func(t *testing.T) {
		n := newFakeNode(t)
		n.snapErr = errors.New("raftstore: snapshot: disk full")
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodPost, path: "/mgmt/snapshot", header: operatorAuth})
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status %d, want 500", rec.Code)
		}
	})
}

// A dump is the local FSM's rows (a stale read: every replica dumps its own
// state), summarised as the applied index it holds, a SHA-256 over the
// canonical rows and a row count per table. Dumps are compared only at
// equal applied indexes: there, equal rows give equal hashes and different
// rows different ones.
func TestMgmtDump(t *testing.T) {
	seed := func(n *fakeNode, keys ...string) {
		for _, k := range keys {
			n.seed(t, store.CompareAndSet{Meta: meta("op-" + k), Key: k, Value: json.RawMessage(`1`)})
		}
	}
	dump := func(n *fakeNode) dumpResponse {
		t.Helper()
		rec := do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodGet, path: "/mgmt/dump", header: operatorAuth})
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		return decodeJSON[dumpResponse](t, rec)
	}
	a, b, c := newFakeNode(t), newFakeNode(t), newFakeNode(t)
	seed(a, "kv:1", "kv:2")
	seed(b, "kv:1", "kv:2")
	seed(c, "kv:1", "kv:3") // same applied index, different rows
	da, db, dc := dump(a), dump(b), dump(c)
	if da.PodName != testPod || da.AppliedIndex != 2 || len(da.SHA256) != 64 {
		t.Fatalf("dump = %+v", da)
	}
	if want := map[string]int{"records": 2, "operation_results": 2, "attempt_closures": 0, "outbox": 0, "meta": 2}; !reflect.DeepEqual(da.Rows, want) {
		t.Fatalf("rows = %v, want %v", da.Rows, want)
	}
	if da.SHA256 != db.SHA256 {
		t.Fatal("equal state, different hashes")
	}
	if dc.AppliedIndex != da.AppliedIndex || dc.SHA256 == da.SHA256 {
		t.Fatalf("different rows at the same applied index: %+v vs %+v", dc, da)
	}
	if !reflect.DeepEqual(a.modes, []raftstore.ReadMode{raftstore.ReadStale}) {
		t.Fatalf("read modes %v, want one stale read of the local FSM", a.modes)
	}
}
