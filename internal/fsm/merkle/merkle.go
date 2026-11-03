// Package merkle builds the content digest the S4.5 audit compares across
// the replicas of a shard: a Merkle tree over canonically encoded FSM rows.
// It does no I/O; internal/fsm streams rows into it from one read
// transaction.
//
// Encoding. A row is each column in declared order: 0x00 for NULL; 0x01
// and 8 big-endian bytes for an integer; 0x02, uvarint(length) and the
// bytes for text; 0x03, length and bytes for a blob. Every value is
// self-delimiting (its tag fixes how many bytes follow), so a row decodes
// in exactly one way and distinct rows never share an encoding, unlike a
// formatted dump where a separator may also occur inside a value.
//
// Hashes (SHA-256, each with its own first byte so no hash of one kind can
// stand for another):
//
//	leaf   = H(0x00 ‖ uvarint(len(table)) ‖ table ‖ row)
//	node   = H(0x01 ‖ left ‖ right)
//	bucket = H(0x02 ‖ leaf ‖ leaf ‖ ...)   leaves in primary-key order
//	empty  = H(0x03)                        the root of no hashes
//
// Shape. Each row falls in one of 256 fixed buckets, the first byte of
// SHA-256 over its encoded primary key. A table's root is the tree over its
// 256 bucket hashes, and a shard's root the tree over its table roots in a
// fixed order. A changed, missing or extra row changes exactly one bucket,
// so a mismatch narrows from root to table to bucket to row; one tree over
// all rows in key order would shift every leaf after a missing row. An odd
// node is carried up a level, never paired with a copy of itself.
package merkle

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"slices"
	"strings"
)

// Buckets is the number of fixed buckets per table.
const Buckets = 256

const (
	tagLeaf   = 0x00
	tagNode   = 0x01
	tagBucket = 0x02
	tagEmpty  = 0x03

	colNull = 0x00
	colInt  = 0x01
	colText = 0x02
	colBlob = 0x03
)

// Hash is a SHA-256 digest; it is hex in JSON and in String.
type Hash [sha256.Size]byte

func (h Hash) String() string { return hex.EncodeToString(h[:]) }

func (h Hash) MarshalText() ([]byte, error) { return []byte(h.String()), nil }

func (h *Hash) UnmarshalText(b []byte) error {
	if hex.DecodedLen(len(b)) != sha256.Size {
		return fmt.Errorf("merkle: hash %q: want %d hex digits", b, 2*sha256.Size)
	}
	_, err := hex.Decode(h[:], b)
	return err
}

var (
	emptyRoot   = Hash(sha256.Sum256([]byte{tagEmpty}))
	emptyBucket = Hash(sha256.Sum256([]byte{tagBucket}))
)

// EncodeRow encodes one row's column values: int64, string, []byte or nil,
// the types the SQLite driver scans INTEGER, TEXT, BLOB and NULL into. Any
// other type is an error, never formatted.
func EncodeRow(vals []any) ([]byte, error) {
	var b []byte
	for i, v := range vals {
		switch v := v.(type) {
		case nil:
			b = append(b, colNull)
		case int64:
			b = append(b, colInt)
			b = binary.BigEndian.AppendUint64(b, uint64(v))
		case string:
			b = append(b, colText)
			b = binary.AppendUvarint(b, uint64(len(v)))
			b = append(b, v...)
		case []byte:
			b = append(b, colBlob)
			b = binary.AppendUvarint(b, uint64(len(v)))
			b = append(b, v...)
		default:
			return nil, fmt.Errorf("merkle: column %d: unsupported type %T", i, v)
		}
	}
	return b, nil
}

// LeafHash is the hash of one encoded row of table.
func LeafHash(table string, row []byte) Hash {
	h := sha256.New()
	h.Write([]byte{tagLeaf})
	h.Write(binary.AppendUvarint(nil, uint64(len(table))))
	h.Write([]byte(table))
	h.Write(row)
	return Hash(h.Sum(nil))
}

// NodeHash is the hash of an inner node.
func NodeHash(left, right Hash) Hash {
	var b [1 + 2*sha256.Size]byte
	b[0] = tagNode
	copy(b[1:], left[:])
	copy(b[1+sha256.Size:], right[:])
	return sha256.Sum256(b[:])
}

// TreeRoot is the root of the binary tree over hs in order; an odd node is
// carried up unchanged. The root of no hashes is a fixed empty value.
func TreeRoot(hs []Hash) Hash {
	if len(hs) == 0 {
		return emptyRoot
	}
	level := slices.Clone(hs)
	for len(level) > 1 {
		next := level[:0]
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
			} else {
				next = append(next, NodeHash(level[i], level[i+1]))
			}
		}
		level = next
	}
	return level[0]
}

// BucketOf is the bucket of a row with the encoded primary key pk.
func BucketOf(pk []byte) int {
	sum := sha256.Sum256(pk)
	return int(sum[0])
}

// BucketHash rebuilds a bucket hash from its leaves in primary-key order.
func BucketHash(leaves []Leaf) Hash {
	h := sha256.New()
	h.Write([]byte{tagBucket})
	for _, l := range leaves {
		h.Write(l.Hash[:])
	}
	return Hash(h.Sum(nil))
}

// Leaf is one row's leaf: its encoded primary key, the key for display and
// the leaf hash.
type Leaf struct {
	Key  []byte `json:"key"`
	PK   string `json:"pk"`
	Hash Hash   `json:"hash"`
}

// TableDigest is one table's root, row count and bucket hashes.
type TableDigest struct {
	Name    string        `json:"name"`
	Root    Hash          `json:"root"`
	Rows    int           `json:"rows"`
	Buckets [Buckets]Hash `json:"buckets"`
}

// Table accumulates one table's digest from its rows, which must arrive in
// strictly ascending primary-key order (SQLite's ORDER BY with BINARY text:
// integers numerically, text byte by byte, column by column). Rows out of
// order, duplicate keys and NULL keys are errors, so the canonical order is
// checked rather than trusted.
type Table struct {
	name    string
	pkCols  int
	rows    int
	prev    []any
	buckets [Buckets]hash.Hash

	collect int // bucket whose leaves are kept; -1 for none
	leaves  []Leaf
}

// NewTable starts the digest of table, whose primary key is the first
// pkCols columns of each row.
func NewTable(name string, pkCols int) *Table {
	return &Table{name: name, pkCols: pkCols, collect: -1}
}

// Collect makes the table keep the leaves of bucket b, for Leaves.
func (t *Table) Collect(b int) { t.collect = b }

// Add adds the next row.
func (t *Table) Add(row []any) error {
	if t.pkCols < 1 || len(row) < t.pkCols {
		return fmt.Errorf("merkle: %s: row of %d columns, primary key of %d", t.name, len(row), t.pkCols)
	}
	pk := row[:t.pkCols]
	if t.prev != nil {
		c, err := comparePK(t.prev, pk)
		if err != nil {
			return fmt.Errorf("merkle: %s: %w", t.name, err)
		}
		if c >= 0 {
			return fmt.Errorf("merkle: %s: key %s after %s: rows not in ascending primary-key order", t.name, display(pk), display(t.prev))
		}
	} else if _, err := comparePK(pk, pk); err != nil {
		return fmt.Errorf("merkle: %s: %w", t.name, err)
	}
	key, err := EncodeRow(pk)
	if err != nil {
		return fmt.Errorf("merkle: %s key: %w", t.name, err)
	}
	enc, err := EncodeRow(row)
	if err != nil {
		return fmt.Errorf("merkle: %s key %s: %w", t.name, display(pk), err)
	}
	t.prev = slices.Clone(pk)
	leaf := LeafHash(t.name, enc)
	b := BucketOf(key)
	if t.buckets[b] == nil {
		t.buckets[b] = sha256.New()
		t.buckets[b].Write([]byte{tagBucket})
	}
	t.buckets[b].Write(leaf[:])
	if b == t.collect {
		t.leaves = append(t.leaves, Leaf{Key: key, PK: display(pk), Hash: leaf})
	}
	t.rows++
	return nil
}

// Digest is the table's digest over the rows added so far.
func (t *Table) Digest() TableDigest {
	d := TableDigest{Name: t.name, Rows: t.rows}
	for i, h := range t.buckets {
		if h == nil {
			d.Buckets[i] = emptyBucket
		} else {
			d.Buckets[i] = Hash(h.Sum(nil))
		}
	}
	d.Root = TreeRoot(d.Buckets[:])
	return d
}

// Leaves are the collected bucket's leaves in key order; nil if Collect was
// not called.
func (t *Table) Leaves() []Leaf { return t.leaves }

// comparePK orders two primary keys as SQLite does; NULL and other types
// are errors.
func comparePK(a, b []any) (int, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("keys of %d and %d columns", len(a), len(b))
	}
	for i := range a {
		var c int
		switch x := a[i].(type) {
		case int64:
			y, ok := b[i].(int64)
			if !ok {
				return 0, fmt.Errorf("key column %d: %T against %T", i, a[i], b[i])
			}
			c = cmp.Compare(x, y)
		case string:
			y, ok := b[i].(string)
			if !ok {
				return 0, fmt.Errorf("key column %d: %T against %T", i, a[i], b[i])
			}
			c = strings.Compare(x, y)
		default:
			return 0, fmt.Errorf("key column %d: unsupported key type %T", i, a[i])
		}
		if c != 0 {
			return c, nil
		}
	}
	return 0, nil
}

func display(pk []any) string {
	parts := make([]string, len(pk))
	for i, v := range pk {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, "/")
}

// Digest is a shard's content digest: its table digests in a fixed order
// and the root over their roots.
type Digest struct {
	Root   Hash          `json:"root"`
	Tables []TableDigest `json:"tables"`
}

// NewDigest combines table digests, in the order given, into a shard digest.
func NewDigest(tables ...TableDigest) Digest {
	roots := make([]Hash, len(tables))
	for i, t := range tables {
		roots[i] = t.Root
	}
	return Digest{Root: TreeRoot(roots), Tables: tables}
}

// TableDiff names a table and the buckets in which replicas disagree.
type TableDiff struct {
	Table   string `json:"table"`
	Buckets []int  `json:"buckets"`
}

// DiffBuckets compares replica digests (keyed by pod) and returns, per
// table, the buckets on which they do not all agree; nil if they agree.
// Digests over different tables or table orders are an error, not a
// difference.
func DiffBuckets(reps map[string]Digest) ([]TableDiff, error) {
	pods := sortedKeys(reps)
	if len(pods) < 2 {
		return nil, errors.New("merkle: need at least two replicas to compare")
	}
	ref := reps[pods[0]]
	for _, p := range pods[1:] {
		d := reps[p]
		if len(d.Tables) != len(ref.Tables) {
			return nil, fmt.Errorf("merkle: %s has %d tables, %s has %d", p, len(d.Tables), pods[0], len(ref.Tables))
		}
		for i := range d.Tables {
			if d.Tables[i].Name != ref.Tables[i].Name {
				return nil, fmt.Errorf("merkle: table %d is %s on %s, %s on %s", i, d.Tables[i].Name, p, ref.Tables[i].Name, pods[0])
			}
		}
	}
	var out []TableDiff
	for i, rt := range ref.Tables {
		var buckets []int
		for b := range rt.Buckets {
			for _, p := range pods[1:] {
				if reps[p].Tables[i].Buckets[b] != rt.Buckets[b] {
					buckets = append(buckets, b)
					break
				}
			}
		}
		if buckets != nil {
			out = append(out, TableDiff{Table: rt.Name, Buckets: buckets})
		}
	}
	return out, nil
}

// DiffKind is how a row on some replicas differs from the majority.
type DiffKind string

const (
	DiffContent  DiffKind = "content"  // present on both, different content
	DiffMissing  DiffKind = "missing"  // the majority has it, these pods do not
	DiffExtra    DiffKind = "extra"    // these pods have it, the majority does not
	DiffConflict DiffKind = "conflict" // no strict majority agrees: no side is presumed right
)

// RowDiff is one primary key on which replicas disagree. Pods disagree with
// Majority; for a conflict Pods is every pod and Majority is nil.
type RowDiff struct {
	Table    string   `json:"table"`
	PK       string   `json:"pk"`
	Kind     DiffKind `json:"kind"`
	Pods     []string `json:"pods"`
	Majority []string `json:"majority,omitempty"`
}

// DiffRows compares one bucket's leaves from each replica (keyed by pod)
// and returns every key on which they disagree, in key order. A pod lacking
// a key counts as one more distinct value.
func DiffRows(table string, reps map[string][]Leaf) []RowDiff {
	pods := sortedKeys(reps)
	type entry struct {
		pk  string
		key []byte
		by  map[string]Hash
	}
	keys := map[string]*entry{}
	for _, p := range pods {
		for _, l := range reps[p] {
			e := keys[string(l.Key)]
			if e == nil {
				e = &entry{pk: l.PK, key: l.Key, by: map[string]Hash{}}
				keys[string(l.Key)] = e
			}
			e.by[p] = l.Hash
		}
	}
	entries := make([]*entry, 0, len(keys))
	for _, e := range keys {
		entries = append(entries, e)
	}
	slices.SortFunc(entries, func(a, b *entry) int {
		return cmp.Or(strings.Compare(a.pk, b.pk), bytes.Compare(a.key, b.key))
	})

	var out []RowDiff
	for _, e := range entries {
		// Group pods by value; "" is absent.
		groups := map[string][]string{}
		for _, p := range pods {
			v := ""
			if h, ok := e.by[p]; ok {
				v = h.String()
			}
			groups[v] = append(groups[v], p)
		}
		if len(groups) == 1 {
			continue
		}
		major := ""
		found := false
		for v, ps := range groups {
			if 2*len(ps) > len(pods) {
				major, found = v, true
			}
		}
		if !found {
			out = append(out, RowDiff{Table: table, PK: e.pk, Kind: DiffConflict, Pods: pods})
			continue
		}
		var minority [][]string
		kinds := map[string]DiffKind{}
		for v, ps := range groups {
			if v == major {
				continue
			}
			minority = append(minority, ps)
			switch {
			case major == "":
				kinds[ps[0]] = DiffExtra
			case v == "":
				kinds[ps[0]] = DiffMissing
			default:
				kinds[ps[0]] = DiffContent
			}
		}
		slices.SortFunc(minority, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
		for _, ps := range minority {
			out = append(out, RowDiff{Table: table, PK: e.pk, Kind: kinds[ps[0]], Pods: ps, Majority: groups[major]})
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}
