package fsm_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// auditLog is a log that leaves rows in every hashed table: records of
// every kind including a tombstone, operation results (with a rejected
// command's result), attempt closures, published and unpublished outbox
// rows.
func auditLog(t *testing.T) [][]byte {
	t.Helper()
	created := store.OutboxEvent{EventID: "ev-q1-created", EventType: "request.created", SchemaVersion: 1,
		Exchange: "relief.requests", RoutingKey: "request.created.water", Payload: json.RawMessage(`{"id":"q1"}`)}
	matched := store.OutboxEvent{EventID: "ev-q1-matched", EventType: "match.completed", SchemaVersion: 1,
		Exchange: "relief.events", RoutingKey: "match.completed", Payload: json.RawMessage(`{"id":"q1"}`)}
	a1, a2, a3 := attempt("a1", "r1"), attempt("a2", "r1"), attempt("a3", "r2")
	cmds := []any{
		store.CreateEntity{Meta: meta("c-r1"), Key: store.ResourceKey("r1"), Resource: resource("r1", store.TypeWater, store.ResourceAvailable)},
		store.CreateEntity{Meta: meta("c-r2"), Key: store.ResourceKey("r2"), Resource: resource("r2", store.TypeWater, store.ResourceAvailable)},
		store.CreateEntity{Meta: meta("c-q1"), Key: store.RequestKey("q1"), Request: request("q1", store.TypeWater), Outbox: []store.OutboxEvent{created}},
		store.CreateEntity{Meta: meta("c-q2"), Key: store.RequestKey("q2"), Request: request("q2", store.TypeWater)},
		store.CompareAndSet{Meta: meta("kv-1"), Key: "kv:demo", Value: json.RawMessage(`1`)},
		store.CompareAndSet{Meta: meta("kv-2"), Key: "kv:demo", ExpectedVersion: 1, Value: json.RawMessage(`2`)},
		store.CompareAndSet{Meta: meta("kv-3"), Key: "kv:demo", ExpectedVersion: 1, Value: json.RawMessage(`3`)}, // conflict
		store.CompareAndSet{Meta: meta("kv-gone-1"), Key: "kv:gone", Value: json.RawMessage(`1`)},
		store.CompareAndSet{Meta: meta("kv-gone-2"), Key: "kv:gone", ExpectedVersion: 1, Delete: true}, // tombstone
		store.TransitionRequest{Meta: meta("t-q1-matching"), Key: store.RequestKey("q1"), ExpectedVersion: 1,
			ExpectedStatus: store.RequestPending, To: store.RequestMatching, Attempt: &a1},
		tryClaim("claim-a1", "r1", "q1", a1, store.TypeWater),
		store.TransitionRequest{Meta: meta("t-q1-matched"), Key: store.RequestKey("q1"), ExpectedVersion: 2,
			ExpectedStatus: store.RequestMatching, ExpectedAttemptID: "a1", To: store.RequestMatched, Outbox: []store.OutboxEvent{matched}},
		tryClaim("claim-a2", "r1", "q2", a2, store.TypeWater), // CONFLICT_CLOSED
		closeAttempt("close-a3", "r2", "q2", a3),
		store.MarkOutboxPublished{Meta: meta("mark-1"), EventIDs: []string{"ev-q1-created"}},
	}
	log := make([][]byte, len(cmds))
	for i, c := range cmds {
		log[i] = mustEncode(t, c)
	}
	return log
}

// replicaOf applies log to a fresh in-memory FSM, as a follower would.
func replicaOf(t *testing.T, log [][]byte) *fsm.FSM {
	t.Helper()
	h := newHarness(t)
	for _, e := range log {
		h.applyEntry(e)
	}
	return h.f
}

// fileReplicaOf applies log to a fresh FSM on a database file, as on a pod.
func fileReplicaOf(t *testing.T, log [][]byte) *fsm.FSM {
	t.Helper()
	f, err := fsm.Open(context.Background(), "file:"+filepath.Join(t.TempDir(), "fsm-0.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	for i, e := range log {
		if _, err := f.Apply(uint64(i+1), e); err != nil {
			t.Fatalf("Apply(%d): %v", i+1, err)
		}
	}
	return f
}

func merkleOf(t *testing.T, f *fsm.FSM) fsm.MerkleReport {
	t.Helper()
	r, err := f.Merkle(context.Background(), f.AppliedIndex())
	if err != nil {
		t.Fatalf("Merkle(%d): %v", f.AppliedIndex(), err)
	}
	return r
}

// Replicas that applied the same log have the same root and summary,
// whether in memory, on a file, or rebuilt from a VACUUM INTO image: the
// digest is over logical content, not file bytes (v3.1 §16).
func TestMerkleEqualAcrossReplicasAndImage(t *testing.T) {
	ctx := context.Background()
	log := auditLog(t)
	leader := replicaOf(t, log)
	want := merkleOf(t, leader)

	if want.Summary.AppliedIndex != uint64(len(log)) || want.Summary.SchemaVersion != "1" {
		t.Fatalf("summary = %+v, want applied index %d, schema 1", want.Summary, len(log))
	}
	if want.Summary.Tombstones != 1 {
		t.Fatalf("tombstones = %d, want 1 (kv:gone)", want.Summary.Tombstones)
	}
	var names []string
	for _, tb := range want.Digest.Tables {
		names = append(names, tb.Name)
		if tb.Rows == 0 {
			t.Errorf("table %s is empty: the audit would compare nothing there", tb.Name)
		}
	}
	if !slices.Equal(names, []string{"records", "operation_results", "attempt_closures", "outbox"}) {
		t.Fatalf("tables = %v", names)
	}

	path := filepath.Join(t.TempDir(), "image.db")
	if err := leader.VacuumInto(ctx, path); err != nil {
		t.Fatal(err)
	}
	img, err := fsm.OpenImage(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = img.Close() })

	for name, f := range map[string]*fsm.FSM{
		"memory replica": replicaOf(t, log),
		"file replica":   fileReplicaOf(t, log),
		"image":          img,
	} {
		if got := merkleOf(t, f); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: root %s, want %s (summary %+v, want %+v)", name, got.Digest.Root, want.Digest.Root, got.Summary, want.Summary)
		}
	}

	shorter := replicaOf(t, log[:len(log)-1])
	if merkleOf(t, shorter).Digest.Root == want.Digest.Root {
		t.Fatal("one command fewer gives the same root")
	}
}

// pinpoint audits three replicas the way storagectl audit does: bucket
// diff on the digests, then a row diff on each differing bucket.
func pinpoint(t *testing.T, reps map[string]*fsm.FSM) []merkle.RowDiff {
	t.Helper()
	ctx := context.Background()
	digests := map[string]merkle.Digest{}
	for pod, f := range reps {
		digests[pod] = merkleOf(t, f).Digest
	}
	tables, err := merkle.DiffBuckets(digests)
	if err != nil {
		t.Fatal(err)
	}
	var out []merkle.RowDiff
	for _, td := range tables {
		for _, b := range td.Buckets {
			leaves := map[string][]merkle.Leaf{}
			for pod, f := range reps {
				br, err := f.MerkleBucket(ctx, f.AppliedIndex(), td.Table, b)
				if err != nil {
					t.Fatalf("MerkleBucket(%s, %d) on %s: %v", td.Table, b, pod, err)
				}
				if br.Hash != digests[pod].Tables[tableIndex(t, digests[pod], td.Table)].Buckets[b] {
					t.Fatalf("%s: bucket %s/%d hash %s does not match its digest", pod, td.Table, b, br.Hash)
				}
				if merkle.BucketHash(br.Leaves) != br.Hash {
					t.Fatalf("%s: bucket %s/%d leaves do not rebuild its hash", pod, td.Table, b)
				}
				leaves[pod] = br.Leaves
			}
			out = append(out, merkle.DiffRows(td.Table, leaves)...)
		}
	}
	return out
}

func tableIndex(t *testing.T, d merkle.Digest, name string) int {
	t.Helper()
	for i, tb := range d.Tables {
		if tb.Name == name {
			return i
		}
	}
	t.Fatalf("no table %s", name)
	return -1
}

// The negative control: one row changed, removed or added on one replica,
// outside Raft, is found and named exactly (table, key, kind, pod), and
// nothing else is reported.
func TestMerkleCorruptionIsPinpointed(t *testing.T) {
	cases := []struct {
		name string
		stmt string
		args []any
		want merkle.RowDiff
	}{
		{"record body", `UPDATE records SET body = replace(body, '"PENDING"', '"CANCELLED"') WHERE key = ?`, []any{"req:q2"},
			merkle.RowDiff{Table: "records", PK: "req:q2", Kind: merkle.DiffContent}},
		{"record version", `UPDATE records SET version = version + 1 WHERE key = ?`, []any{"kv:demo"},
			merkle.RowDiff{Table: "records", PK: "kv:demo", Kind: merkle.DiffContent}},
		{"fake tombstone", `UPDATE records SET deleted = 1 WHERE key = ?`, []any{"res:r2"},
			merkle.RowDiff{Table: "records", PK: "res:r2", Kind: merkle.DiffContent}},
		{"lost tombstone", `DELETE FROM records WHERE key = ?`, []any{"kv:gone"},
			merkle.RowDiff{Table: "records", PK: "kv:gone", Kind: merkle.DiffMissing}},
		{"lost operation result", `DELETE FROM operation_results WHERE operation_id = ?`, []any{"claim-a1"},
			merkle.RowDiff{Table: "operation_results", PK: "claim-a1", Kind: merkle.DiffMissing}},
		{"operation payload hash", `UPDATE operation_results SET payload_hash = 'x' WHERE operation_id = ?`, []any{"kv-1"},
			merkle.RowDiff{Table: "operation_results", PK: "kv-1", Kind: merkle.DiffContent}},
		{"forged closure", `INSERT INTO attempt_closures (resource_key, attempt_id, request_id, fingerprint, reason, disposition, closed_at) VALUES (?, 'a9', 'q9', 'fp', 'forged', 'AVAILABLE', 1)`, []any{"res:r1"},
			merkle.RowDiff{Table: "attempt_closures", PK: "res:r1/a9", Kind: merkle.DiffExtra}},
		{"outbox published flag", `UPDATE outbox SET published = 1 WHERE event_id = ?`, []any{"ev-q1-matched"},
			merkle.RowDiff{Table: "outbox", PK: "2", Kind: merkle.DiffContent}},
	}
	log := auditLog(t)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reps := map[string]*fsm.FSM{"pod-0": replicaOf(t, log), "pod-1": replicaOf(t, log), "pod-2": replicaOf(t, log)}
			before := merkleOf(t, reps["pod-2"]).Digest.Root
			if err := reps["pod-2"].CorruptRow(context.Background(), c.stmt, c.args...); err != nil {
				t.Fatalf("corrupt: %v", err)
			}
			if merkleOf(t, reps["pod-2"]).Digest.Root == before {
				t.Fatal("the corruption did not change the root")
			}
			c.want.Pods, c.want.Majority = []string{"pod-2"}, []string{"pod-0", "pod-1"}
			if got := pinpoint(t, reps); !reflect.DeepEqual(got, []merkle.RowDiff{c.want}) {
				t.Fatalf("audit found\n %+v\nwant\n %+v", got, []merkle.RowDiff{c.want})
			}
		})
	}
	clean := map[string]*fsm.FSM{"pod-0": replicaOf(t, log), "pod-1": replicaOf(t, log), "pod-2": replicaOf(t, log)}
	if got := pinpoint(t, clean); got != nil {
		t.Fatalf("clean replicas reported %+v", got)
	}
}

// The applied index is a precondition, not content: Merkle refuses any
// index but the one its read transaction sees, and the root does not
// depend on meta, so equal content at a forged index still hashes equal
// while the summary shows the index.
func TestMerkleRequiresExactIndex(t *testing.T) {
	ctx := context.Background()
	log := auditLog(t)
	f := replicaOf(t, log)
	n := f.AppliedIndex()
	for _, at := range []uint64{n - 1, n + 1, 0} {
		_, err := f.Merkle(ctx, at)
		var mis *fsm.IndexMismatchError
		if !errors.As(err, &mis) || !errors.Is(err, fsm.ErrIndexMismatch) || mis.Want != at || mis.Got != n {
			t.Fatalf("Merkle(%d) at %d: err = %v, want index mismatch {want %d, got %d}", at, n, err, at, n)
		}
		if _, err := f.MerkleBucket(ctx, at, "records", 0); !errors.Is(err, fsm.ErrIndexMismatch) {
			t.Fatalf("MerkleBucket(%d) at %d: err = %v, want index mismatch", at, n, err)
		}
	}
	cur, err := f.Merkle(ctx, fsm.AnyIndex)
	if err != nil || cur.Summary.AppliedIndex != n {
		t.Fatalf("Merkle(AnyIndex) = %+v, %v; want applied index %d", cur.Summary, err, n)
	}

	forged := replicaOf(t, log)
	if err := forged.CorruptRow(ctx, `UPDATE meta SET v = '999' WHERE k = 'applied_index'`); err != nil {
		t.Fatal(err)
	}
	if _, err := forged.Merkle(ctx, n); !errors.Is(err, fsm.ErrIndexMismatch) {
		t.Fatalf("Merkle(%d) with meta at 999: err = %v, want index mismatch", n, err)
	}
	got, err := forged.Merkle(ctx, 999)
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest.Root != cur.Digest.Root || got.Summary.AppliedIndex != 999 {
		t.Fatalf("forged index: root %s (want %s), summary index %d", got.Digest.Root, cur.Digest.Root, got.Summary.AppliedIndex)
	}
}

// Merkle reads every table in one read transaction pinned by its first
// read of meta: a command applied while it runs is not in the digest, which
// equals that of a replica stopped at the requested index. The apply lands
// right after meta is read (so meta and the tables must share one
// transaction) and after the first table (so the tables must share it too).
func TestMerkleIsOneCommittedState(t *testing.T) {
	log := auditLog(t)
	n := len(log) - 3 // log[n] is a rejected claim: it writes operation_results only
	for _, point := range []string{"meta", "records"} {
		t.Run("apply after "+point, func(t *testing.T) {
			f := fileReplicaOf(t, log[:n])
			var once sync.Once
			fsm.SetAfterMerkleTable(t, func(table string) {
				if table == point {
					once.Do(func() { // a concurrent Apply, on the write connection
						if _, err := f.Apply(uint64(n+1), log[n]); err != nil {
							t.Errorf("Apply during Merkle: %v", err)
						}
					})
				}
			})
			got, err := f.Merkle(context.Background(), uint64(n))
			if err != nil {
				t.Fatalf("Merkle(%d): %v", n, err)
			}
			fsm.SetAfterMerkleTable(t, nil)
			if want := merkleOf(t, fileReplicaOf(t, log[:n])); !reflect.DeepEqual(got, want) {
				t.Fatalf("digest during an apply: root %s, want the state at %d: %s", got.Digest.Root, n, want.Digest.Root)
			}
			if f.AppliedIndex() != uint64(n+1) {
				t.Fatalf("the concurrent apply did not happen: applied %d", f.AppliedIndex())
			}
			if after := merkleOf(t, f); !reflect.DeepEqual(after, merkleOf(t, fileReplicaOf(t, log[:n+1]))) {
				t.Fatal("the next digest does not include the concurrent apply")
			}
		})
	}
}

// Every column of every table is hashed, in declared order; meta is the
// only table left out, and holds only the two keys the summary reports. A
// column or table added to the schema fails here until the audit covers it.
func TestMerkleCoversEverySchemaColumn(t *testing.T) {
	ctx := context.Background()
	f := replicaOf(t, auditLog(t))
	schema, err := f.SchemaColumns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(schema["meta"], []string{"k", "v"}) {
		t.Fatalf("meta columns = %v", schema["meta"])
	}
	delete(schema, "meta")
	if got := fsm.MerkleColumns(); !reflect.DeepEqual(got, schema) {
		t.Fatalf("hashed columns\n %v\nschema\n %v", got, schema)
	}
	// The audit, which does not link the FSM, checks digests against
	// merkle.Tables: the FSM must produce exactly those, in that order.
	rep, err := f.Merkle(ctx, f.AppliedIndex())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tb := range rep.Digest.Tables {
		names = append(names, tb.Name)
	}
	if !reflect.DeepEqual(names, merkle.Tables) {
		t.Fatalf("digest tables %v, merkle.Tables %v", names, merkle.Tables)
	}
	if err := rep.Digest.Check(); err != nil {
		t.Fatalf("the FSM's digest fails Check: %v", err)
	}
	if err := f.CorruptRow(ctx, `INSERT INTO meta (k, v) VALUES ('ring_version', '1')`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Merkle(ctx, f.AppliedIndex()); err == nil {
		t.Fatal("Merkle ignored a meta key it neither hashes nor reports")
	}
}

func TestMerkleBucketRejectsBadArguments(t *testing.T) {
	ctx := context.Background()
	f := replicaOf(t, auditLog(t))
	n := f.AppliedIndex()
	for _, c := range []struct {
		table  string
		bucket int
	}{{"meta", 0}, {"nope", 0}, {"records", -1}, {"records", merkle.Buckets}} {
		if _, err := f.MerkleBucket(ctx, n, c.table, c.bucket); !errors.Is(err, fsm.ErrMerkleArgument) || errors.Is(err, fsm.ErrIndexMismatch) {
			t.Errorf("MerkleBucket(%s, %d): err = %v, want ErrMerkleArgument", c.table, c.bucket, err)
		}
	}
	br, err := f.MerkleBucket(ctx, n, "attempt_closures", 0)
	if err != nil || br.AppliedIndex != n || br.Table != "attempt_closures" || br.Bucket != 0 {
		t.Fatalf("MerkleBucket = %+v, %v", br, err)
	}
}
