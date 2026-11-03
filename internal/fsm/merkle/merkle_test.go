package merkle_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
)

func mustEncode(t *testing.T, vals ...any) []byte {
	t.Helper()
	b, err := merkle.EncodeRow(vals)
	if err != nil {
		t.Fatalf("EncodeRow(%v): %v", vals, err)
	}
	return b
}

// Two different rows never encode to the same bytes: the separator inside a
// value, NULL against "", an integer against its text, and a column split
// differently are all distinct.
func TestEncodeRowIsInjective(t *testing.T) {
	pairs := [][2][]any{
		{{"a|b", "c"}, {"a", "b|c"}},
		// A value holding the text tag byte: without length prefixes the
		// two rows would be the same bytes.
		{{"a\x02b", "c"}, {"a", "b\x02c"}},
		{{[]byte("a\x03b"), "c"}, {[]byte("a"), "b\x03c"}},
		{{"a | b"}, {"a", "b"}},
		{{nil}, {""}},
		{{int64(0)}, {nil}},
		{{int64(1)}, {"1"}},
		{{"ab"}, {"a", "b"}},
		{{"ab", ""}, {"a", "b"}},
		{{[]byte("x")}, {"x"}},
		{{"x"}, {"x", nil}},
		{{int64(-1)}, {int64(1)}},
	}
	for _, p := range pairs {
		a, b := mustEncode(t, p[0]...), mustEncode(t, p[1]...)
		if bytes.Equal(a, b) {
			t.Errorf("%#v and %#v encode the same: %x", p[0], p[1], a)
		}
	}
	if !bytes.Equal(mustEncode(t, "k", int64(7), nil), mustEncode(t, "k", int64(7), nil)) {
		t.Fatal("the same row encodes differently twice")
	}
}

// The FSM schema has only INTEGER and TEXT columns; anything else scanned
// from a row is a bug, not something to hash by its Go formatting.
func TestEncodeRowRejectsUnsupportedTypes(t *testing.T) {
	for _, v := range []any{1.5, 7, true, struct{}{}} {
		if _, err := merkle.EncodeRow([]any{v}); err == nil {
			t.Errorf("EncodeRow accepted %T", v)
		}
	}
}

// A leaf can never be mistaken for an inner node, even over the same bytes.
func TestLeafAndNodeAreDomainSeparated(t *testing.T) {
	l, r := merkle.LeafHash("t", []byte("l")), merkle.LeafHash("t", []byte("r"))
	if merkle.NodeHash(l, r) == merkle.LeafHash("", append(l[:], r[:]...)) {
		t.Fatal("an inner node equals a leaf over the same bytes")
	}
	if merkle.LeafHash("ab", []byte("c")) == merkle.LeafHash("a", []byte("bc")) {
		t.Fatal("the table name and the row are not separated")
	}
	if merkle.NodeHash(l, r) == merkle.NodeHash(r, l) {
		t.Fatal("node hash ignores child order")
	}
}

// An odd node is carried up, never paired with a copy of itself, so a list
// and the same list with its last element repeated have different roots.
func TestTreeRootCarriesOddNodeUp(t *testing.T) {
	h := func(s string) merkle.Hash { return merkle.LeafHash("t", []byte(s)) }
	a, b, c := h("a"), h("b"), h("c")
	if got, want := merkle.TreeRoot([]merkle.Hash{a, b, c}), merkle.NodeHash(merkle.NodeHash(a, b), c); got != want {
		t.Fatalf("root of 3 = %s, want node(node(a,b),c) %s", got, want)
	}
	if merkle.TreeRoot([]merkle.Hash{a, b, c}) == merkle.TreeRoot([]merkle.Hash{a, b, c, c}) {
		t.Fatal("[a b c] and [a b c c] have the same root")
	}
	if merkle.TreeRoot([]merkle.Hash{a}) != a {
		t.Fatal("the root of one hash is not that hash")
	}
	if merkle.TreeRoot(nil) == merkle.TreeRoot([]merkle.Hash{{}}) {
		t.Fatal("the empty tree equals a tree of one zero hash")
	}
}

// Rows must arrive in strictly ascending primary-key order, compared as
// SQLite orders them: integers numerically, text byte by byte, composite
// keys column by column. Anything else is refused rather than hashed.
func TestTableRequiresAscendingPrimaryKey(t *testing.T) {
	cases := []struct {
		name  string
		pk    int
		rows  [][]any
		valid bool
	}{
		{"text ascending", 1, [][]any{{"a", "x"}, {"b", "x"}}, true},
		{"text descending", 1, [][]any{{"b", "x"}, {"a", "x"}}, false},
		{"duplicate", 1, [][]any{{"a", "x"}, {"a", "y"}}, false},
		{"bytewise, not case-folded", 1, [][]any{{"B"}, {"a"}}, true},
		{"integers numerically", 1, [][]any{{int64(2)}, {int64(10)}}, true},
		{"integers descending", 1, [][]any{{int64(10)}, {int64(2)}}, false},
		{"composite ascending", 2, [][]any{{"a", "z"}, {"b", "a"}}, true},
		{"composite second column", 2, [][]any{{"a", "b"}, {"a", "a"}}, false},
		{"mixed key types", 1, [][]any{{"a"}, {int64(1)}}, false},
		{"null key", 1, [][]any{{nil, "x"}}, false},
		{"row shorter than key", 2, [][]any{{"a"}}, false},
	}
	for _, c := range cases {
		tb := merkle.NewTable("t", c.pk)
		var err error
		for _, r := range c.rows {
			if err = tb.Add(r); err != nil {
				break
			}
		}
		if (err == nil) != c.valid {
			t.Errorf("%s: Add err = %v, want valid=%v", c.name, err, c.valid)
		}
	}
}

func buildTable(t *testing.T, rows [][]any) merkle.TableDigest {
	t.Helper()
	tb := merkle.NewTable("records", 1)
	for _, r := range rows {
		if err := tb.Add(r); err != nil {
			t.Fatalf("Add(%v): %v", r, err)
		}
	}
	return tb.Digest()
}

// sampleRows are n rows key:00000.. in key order.
func sampleRows(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{fmt.Sprintf("key:%05d", i), int64(1), fmt.Sprintf(`{"n":%d}`, i)}
	}
	return rows
}

func differingBuckets(a, b merkle.TableDigest) []int {
	var out []int
	for i := range a.Buckets {
		if a.Buckets[i] != b.Buckets[i] {
			out = append(out, i)
		}
	}
	return out
}

func bucketOfKey(t *testing.T, key string) int {
	t.Helper()
	return merkle.BucketOf(mustEncode(t, key))
}

// A changed, missing or extra row changes its own bucket and no other, so a
// mismatch narrows to one bucket instead of every leaf after it.
func TestOneRowDifferenceTouchesOneBucket(t *testing.T) {
	base := sampleRows(1000)
	want := buildTable(t, base)
	if again := buildTable(t, sampleRows(1000)); !reflect.DeepEqual(again, want) {
		t.Fatal("the same rows give different digests")
	}
	if want.Rows != 1000 {
		t.Fatalf("Rows = %d, want 1000", want.Rows)
	}

	changed := sampleRows(1000)
	changed[500][2] = `{"n":-1}`
	missing := append(sampleRows(1000)[:500:500], sampleRows(1000)[501:]...)
	extra := append(sampleRows(1000)[:501:501], append([][]any{{"key:00500x", int64(1), `{}`}}, sampleRows(1000)[501:]...)...)

	for name, c := range map[string]struct {
		rows [][]any
		key  string
	}{
		"changed": {changed, "key:00500"},
		"missing": {missing, "key:00500"},
		"extra":   {extra, "key:00500x"},
	} {
		got := buildTable(t, c.rows)
		if got.Root == want.Root {
			t.Errorf("%s: root unchanged", name)
		}
		if d := differingBuckets(want, got); !reflect.DeepEqual(d, []int{bucketOfKey(t, c.key)}) {
			t.Errorf("%s: differing buckets %v, want only %d", name, d, bucketOfKey(t, c.key))
		}
	}
}

func TestEmptyTableDigest(t *testing.T) {
	a, b := merkle.NewTable("t", 1).Digest(), merkle.NewTable("t", 1).Digest()
	if a.Rows != 0 || a != b {
		t.Fatalf("empty digests differ or count rows: %+v", a)
	}
	empty := sha256.Sum256([]byte{0x02})
	for i, h := range a.Buckets {
		if h != empty {
			t.Fatalf("empty bucket %d = %s", i, h)
		}
	}
	if len(a.Buckets) != merkle.Buckets || merkle.Buckets != 256 {
		t.Fatalf("%d buckets, want 256", len(a.Buckets))
	}
}

// Collecting one bucket returns exactly its leaves in key order, and they
// rebuild the bucket hash the digest reported.
func TestCollectRebuildsBucketHash(t *testing.T) {
	rows := sampleRows(500)
	b := bucketOfKey(t, "key:00042")
	tb := merkle.NewTable("records", 1)
	tb.Collect(b)
	for _, r := range rows {
		if err := tb.Add(r); err != nil {
			t.Fatal(err)
		}
	}
	d, leaves := tb.Digest(), tb.Leaves()
	if len(leaves) == 0 {
		t.Fatal("no leaves collected")
	}
	found := false
	for i, l := range leaves {
		if merkle.BucketOf(l.Key) != b {
			t.Fatalf("leaf %s is in bucket %d, not %d", l.PK, merkle.BucketOf(l.Key), b)
		}
		if i > 0 && leaves[i-1].PK >= l.PK {
			t.Fatalf("leaves out of key order: %s then %s", leaves[i-1].PK, l.PK)
		}
		found = found || l.PK == "key:00042"
	}
	if !found {
		t.Fatal("key:00042 not among its bucket's leaves")
	}
	if got := merkle.BucketHash(leaves); got != d.Buckets[b] {
		t.Fatalf("rebuilt bucket hash %s, digest says %s", got, d.Buckets[b])
	}
	if merkle.NewTable("records", 1).Leaves() != nil {
		t.Fatal("a table not collecting returns leaves")
	}
}

func digestOf(t *testing.T, records, ops [][]any) merkle.Digest {
	t.Helper()
	r, o := merkle.NewTable("records", 1), merkle.NewTable("operation_results", 1)
	for _, row := range records {
		if err := r.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range ops {
		if err := o.Add(row); err != nil {
			t.Fatal(err)
		}
	}
	return merkle.NewDigest(r.Digest(), o.Digest())
}

// The shard root covers every table in order: a change in any table changes
// it, and the bucket diff names that table and bucket only.
func TestDiffBucketsAcrossReplicas(t *testing.T) {
	ops := [][]any{{"op-1", "h"}, {"op-2", "h"}}
	clean := digestOf(t, sampleRows(200), ops)
	bad := sampleRows(200)
	bad[7][2] = `{"n":"x"}`
	reps := map[string]merkle.Digest{
		"pod-0": clean,
		"pod-1": digestOf(t, sampleRows(200), ops),
		"pod-2": digestOf(t, bad, ops),
	}
	if clean.Root != reps["pod-1"].Root || clean.Root == reps["pod-2"].Root {
		t.Fatal("roots do not reflect the one corrupted replica")
	}
	diffs, err := merkle.DiffBuckets(reps)
	if err != nil {
		t.Fatal(err)
	}
	want := []merkle.TableDiff{{Table: "records", Buckets: []int{bucketOfKey(t, "key:00007")}}}
	if !reflect.DeepEqual(diffs, want) {
		t.Fatalf("DiffBuckets = %+v, want %+v", diffs, want)
	}

	reps["pod-2"] = clean
	if diffs, err := merkle.DiffBuckets(reps); err != nil || diffs != nil {
		t.Fatalf("equal replicas: %+v, %v", diffs, err)
	}

	tableSwap := merkle.NewDigest(merkle.NewTable("operation_results", 1).Digest(), merkle.NewTable("records", 1).Digest())
	if _, err := merkle.DiffBuckets(map[string]merkle.Digest{"a": clean, "b": tableSwap}); err == nil {
		t.Fatal("digests over different tables were compared")
	}
	swapped := merkle.NewDigest(clean.Tables[1], clean.Tables[0])
	if swapped.Root == clean.Root {
		t.Fatal("the shard root ignores table order")
	}
}

func leaf(pk, content string) merkle.Leaf {
	key, _ := merkle.EncodeRow([]any{pk})
	return merkle.Leaf{Key: key, PK: pk, Hash: merkle.LeafHash("t", []byte(content))}
}

// Row diffs name each differing key with how it differs from the majority,
// and which pods disagree.
func TestDiffRowsClassifiesAgainstMajority(t *testing.T) {
	reps := map[string][]merkle.Leaf{
		"pod-0": {leaf("a", "1"), leaf("b", "1"), leaf("c", "1")},
		"pod-1": {leaf("a", "1"), leaf("b", "1"), leaf("c", "1")},
		"pod-2": {leaf("a", "2"), leaf("c", "1"), leaf("d", "1")},
	}
	got := merkle.DiffRows("records", reps)
	want := []merkle.RowDiff{
		{Table: "records", PK: "a", Kind: merkle.DiffContent, Pods: []string{"pod-2"}, Majority: []string{"pod-0", "pod-1"}},
		{Table: "records", PK: "b", Kind: merkle.DiffMissing, Pods: []string{"pod-2"}, Majority: []string{"pod-0", "pod-1"}},
		{Table: "records", PK: "d", Kind: merkle.DiffExtra, Pods: []string{"pod-2"}, Majority: []string{"pod-0", "pod-1"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DiffRows =\n %+v\nwant\n %+v", got, want)
	}

	// Two replicas, or three that all disagree, have no majority: a conflict
	// naming every pod, not a guess at which one is right.
	two := merkle.DiffRows("records", map[string][]merkle.Leaf{"pod-0": {leaf("a", "1")}, "pod-1": {leaf("a", "2")}})
	if len(two) != 1 || two[0].Kind != merkle.DiffConflict || !reflect.DeepEqual(two[0].Pods, []string{"pod-0", "pod-1"}) || two[0].Majority != nil {
		t.Fatalf("two-replica diff = %+v", two)
	}
	if d := merkle.DiffRows("records", map[string][]merkle.Leaf{"x": {leaf("a", "1")}, "y": {leaf("a", "1")}}); d != nil {
		t.Fatalf("equal leaves differ: %+v", d)
	}
}

func TestHashJSONRoundTrip(t *testing.T) {
	h := merkle.LeafHash("t", []byte("x"))
	b, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"`+h.String()+`"` || len(h.String()) != 64 {
		t.Fatalf("Hash JSON = %s", b)
	}
	var back merkle.Hash
	if err := json.Unmarshal(b, &back); err != nil || back != h {
		t.Fatalf("round trip = %s, %v", back, err)
	}
	if err := json.Unmarshal([]byte(`"abc"`), &back); err == nil {
		t.Fatal("a short hash decoded")
	}
	lb, err := json.Marshal(leaf("a", "1"))
	if err != nil {
		t.Fatal(err)
	}
	var l merkle.Leaf
	if err := json.Unmarshal(lb, &l); err != nil || !reflect.DeepEqual(l, leaf("a", "1")) {
		t.Fatalf("leaf round trip = %+v, %v (%s)", l, err, lb)
	}
}
