package storagenode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
	"github.com/ParthMalhotra07/ReliefGrid/internal/raftstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func seedKVs(t *testing.T, n *fakeNode, keys ...string) {
	t.Helper()
	for _, k := range keys {
		n.seed(t, store.CompareAndSet{Meta: meta("op-" + k), Key: k, Value: json.RawMessage(`1`)})
	}
}

func merkleCall(t *testing.T, n *fakeNode, query string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, newTestServer(t, n).mgmtRoutes(), call{method: http.MethodGet, path: "/mgmt/merkle" + query, header: operatorAuth})
}

// The digest exposes every row of the shard, like a dump: operator only.
func TestMgmtMerkleRequiresOperatorToken(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			s := newTestServer(t, n)
			s.operatorToken = tc.configured
			rec := do(t, s.mgmtRoutes(), call{method: http.MethodGet, path: "/mgmt/merkle", header: tc.header})
			if rec.Code != tc.status {
				t.Fatalf("status %d (%s), want %d", rec.Code, rec.Body.String(), tc.status)
			}
			if tc.status != http.StatusOK && len(n.modes) != 0 {
				t.Fatal("read the FSM without a valid operator token")
			}
		})
	}
}

// GET /mgmt/merkle is this replica's digest (a stale read of its own FSM),
// with its pod name and ring version. Equal rows at an equal index give
// equal roots; different rows at the same index do not.
func TestMgmtMerkleDigest(t *testing.T) {
	a, b, c := newFakeNode(t), newFakeNode(t), newFakeNode(t)
	seedKVs(t, a, "kv:1", "kv:2")
	seedKVs(t, b, "kv:1", "kv:2")
	seedKVs(t, c, "kv:1", "kv:3")
	get := func(n *fakeNode, q string) merkleResponse {
		t.Helper()
		rec := merkleCall(t, n, q)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /mgmt/merkle%s: %d %s", q, rec.Code, rec.Body.String())
		}
		return decodeJSON[merkleResponse](t, rec)
	}
	ma, mb, mc := get(a, "?at=2"), get(b, "?at=2"), get(c, "?at=2")
	if ma.PodName != testPod || ma.RingVersion != loadRing(t).Version() || ma.Summary.AppliedIndex != 2 || ma.Summary.SchemaVersion != "1" {
		t.Fatalf("merkle = %+v", ma)
	}
	if ma.Digest.Root != mb.Digest.Root || !reflect.DeepEqual(ma.Digest, mb.Digest) {
		t.Fatal("equal state, different digests")
	}
	if mc.Digest.Root == ma.Digest.Root {
		t.Fatal("different rows at the same index, same root")
	}
	if len(ma.Digest.Tables) != 4 || ma.Digest.Tables[0].Name != "records" || ma.Digest.Tables[0].Rows != 2 {
		t.Fatalf("tables = %+v", ma.Digest.Tables)
	}
	if !reflect.DeepEqual(a.modes, []raftstore.ReadMode{raftstore.ReadStale}) {
		t.Fatalf("read modes %v, want one stale read of the local FSM", a.modes)
	}
	// Without at, the digest is at whatever index the read sees: for
	// looking at one replica, never for comparing.
	if cur := get(a, ""); cur.Summary.AppliedIndex != 2 || cur.Digest.Root != ma.Digest.Root {
		t.Fatalf("digest without at = %+v", cur.Summary)
	}
}

// Any index but the one the read sees is 409 INDEX_MISMATCH carrying the
// replica's applied index, so the caller can choose a new target, and no
// digest. AnyIndex cannot be asked for explicitly, and a malformed index is
// 400: only leaving at out means "any".
func TestMgmtMerkleExactIndex(t *testing.T) {
	n := newFakeNode(t)
	seedKVs(t, n, "kv:1", "kv:2", "kv:3")
	for _, at := range []uint64{0, 2, 4} {
		rec := merkleCall(t, n, fmt.Sprintf("?at=%d", at))
		if rec.Code != http.StatusConflict {
			t.Fatalf("at=%d on a replica at 3: %d %s, want 409", at, rec.Code, rec.Body.String())
		}
		e := decodeJSON[indexMismatchResponse](t, rec)
		if e.Code != "INDEX_MISMATCH" || e.AppliedIndex != 3 || !strings.Contains(e.Error, strconv.FormatUint(at, 10)) {
			t.Fatalf("at=%d: %+v", at, e)
		}
		if strings.Contains(rec.Body.String(), `"digest"`) || strings.Contains(rec.Body.String(), `"root"`) {
			t.Fatalf("a mismatch returned a digest: %s", rec.Body.String())
		}
	}
	for _, q := range []string{
		"?at=" + strconv.FormatUint(fsm.AnyIndex, 10),
		"?at=",
		"?at=-1",
		"?at=abc",
		"?at=3&table=records",
		"?at=3&bucket=0",
		"?at=3&table=records&bucket=256",
		"?at=3&table=records&bucket=-1",
		"?at=3&table=records&bucket=x",
		"?at=3&table=meta&bucket=0",
		"?at=3&table=nope&bucket=0",
	} {
		if rec := merkleCall(t, n, q); rec.Code != http.StatusBadRequest {
			t.Errorf("GET /mgmt/merkle%s: %d %s, want 400", q, rec.Code, rec.Body.String())
		}
	}
}

// With table and bucket, the answer is that bucket's leaves at the index,
// which rebuild the bucket hash the full digest reported.
func TestMgmtMerkleBucket(t *testing.T) {
	n := newFakeNode(t)
	seedKVs(t, n, "kv:1", "kv:2", "kv:3", "kv:4")
	full := decodeJSON[merkleResponse](t, merkleCall(t, n, "?at=4"))
	key, err := merkle.EncodeRow([]any{"kv:3"})
	if err != nil {
		t.Fatal(err)
	}
	b := merkle.BucketOf(key)
	rec := merkleCall(t, n, fmt.Sprintf("?at=4&table=records&bucket=%d", b))
	if rec.Code != http.StatusOK {
		t.Fatalf("bucket: %d %s", rec.Code, rec.Body.String())
	}
	got := decodeJSON[merkleBucketResponse](t, rec)
	if got.PodName != testPod || got.AppliedIndex != 4 || got.Table != "records" || got.Bucket != b {
		t.Fatalf("bucket = %+v", got)
	}
	if got.Hash != full.Digest.Tables[0].Buckets[b] || merkle.BucketHash(got.Leaves) != got.Hash {
		t.Fatal("bucket leaves do not match the digest")
	}
	found := false
	for _, l := range got.Leaves {
		found = found || l.PK == "kv:3"
	}
	if !found {
		t.Fatalf("kv:3 not in its bucket: %+v", got.Leaves)
	}
	if rec := merkleCall(t, n, fmt.Sprintf("?at=3&table=records&bucket=%d", b)); rec.Code != http.StatusConflict {
		t.Fatalf("bucket at a stale index: %d, want 409", rec.Code)
	}
	empty := decodeJSON[merkleBucketResponse](t, merkleCall(t, n, "?at=4&table=attempt_closures&bucket=0"))
	if empty.Leaves == nil || len(empty.Leaves) != 0 {
		t.Fatalf("empty bucket leaves = %#v, want []", empty.Leaves)
	}
}

func TestMgmtMerkleReadFailureIs500(t *testing.T) {
	n := newFakeNode(t)
	n.readErr = fmt.Errorf("sqlite: disk I/O error")
	if rec := merkleCall(t, n, "?at=1"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rec.Code)
	}
}

// An unadmitted disk has no FSM to digest.
func TestGatedRefusesMerkle(t *testing.T) {
	g := NewGated(raftstore.Identity{PodName: testPod, GroupID: "shard-1", Shard: testShard}, false, GatedConfig{OperatorToken: testToken})
	rec := do(t, g.MgmtRoutes(), call{method: http.MethodGet, path: "/mgmt/merkle?at=1", header: operatorAuth})
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "UNADMITTED") {
		t.Fatalf("gated merkle: %d %s, want 409 UNADMITTED", rec.Code, rec.Body.String())
	}
}
