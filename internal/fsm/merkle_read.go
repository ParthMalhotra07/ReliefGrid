package fsm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/merkle"
)

// merkleTables is what the audit hashes, in shard-root order: every table
// but meta, every column in declared order, the first pk columns being the
// primary key the rows are ordered by. A test checks this against the
// schema, so a new column cannot be left out silently.
var merkleTables = []struct {
	name string
	pk   int
	cols []string
}{
	{"records", 1, []string{"key", "kind", "version", "deleted", "type", "status", "geohash", "body", "updated_at"}},
	{"operation_results", 1, []string{"operation_id", "payload_hash", "key", "result", "applied_index"}},
	{"attempt_closures", 2, []string{"resource_key", "attempt_id", "request_id", "fingerprint", "reason", "disposition", "closed_at"}},
	{"outbox", 1, []string{"seq", "event_id", "entity_key", "entity_version", "event", "published", "created_at"}},
}

// recordsDeletedCol is the position of records.deleted, for the tombstone count.
const recordsDeletedCol = 3

// AnyIndex asks Merkle for the digest at whatever index it reads, instead
// of requiring one. Only for looking at a single replica; comparisons need
// an exact index.
const AnyIndex uint64 = math.MaxUint64

// ErrIndexMismatch: the FSM is not at the index the audit asked for.
var ErrIndexMismatch = errors.New("fsm: applied index mismatch")

// IndexMismatchError is the index asked for and the index the read saw.
type IndexMismatchError struct{ Want, Got uint64 }

func (e *IndexMismatchError) Error() string {
	return fmt.Sprintf("fsm: audit at applied index %d, FSM is at %d", e.Want, e.Got)
}

func (e *IndexMismatchError) Is(target error) bool { return target == ErrIndexMismatch }

// MerkleSummary is the metadata compared beside the content root rather
// than hashed into it: the applied index is the comparison's precondition,
// and two replicas with equal content have equal roots.
type MerkleSummary struct {
	SchemaVersion string `json:"schemaVersion"`
	AppliedIndex  uint64 `json:"appliedIndex"`
	Tombstones    int    `json:"tombstones"`
}

// MerkleReport is one replica's digest at one applied index.
type MerkleReport struct {
	Summary MerkleSummary `json:"summary"`
	Digest  merkle.Digest `json:"digest"`
}

// MerkleBucketReport is one bucket's leaves at one applied index, with the
// bucket hash they rebuild.
type MerkleBucketReport struct {
	AppliedIndex uint64        `json:"appliedIndex"`
	Table        string        `json:"table"`
	Bucket       int           `json:"bucket"`
	Hash         merkle.Hash   `json:"hash"`
	Leaves       []merkle.Leaf `json:"leaves"`
}

// afterMerkleTable, tests only, runs after Merkle reads meta and after it
// reads each hashed table.
var afterMerkleTable func(table string)

// Merkle is the content digest of every hashed table at applied index at
// (AnyIndex: at whatever index it reads). It is a read on the read pool, in
// one read transaction whose first statement reads meta, which pins the WAL
// snapshot every table is then read from: Apply keeps committing on the
// write connection meanwhile and none of it is seen. It never writes, and
// is never called from Apply. If the transaction is not at at, it returns
// an *IndexMismatchError and no digest.
//
// On an in-memory database reads share the write connection, so there it
// holds Apply off until it returns; pods use a file.
func (f *FSM) Merkle(ctx context.Context, at uint64) (MerkleReport, error) {
	var rep MerkleReport
	err := f.merkleRead(ctx, at, func(tx *sql.Tx, sum MerkleSummary) error {
		tables := make([]merkle.TableDigest, 0, len(merkleTables))
		for _, mt := range merkleTables {
			tb := merkle.NewTable(mt.name, mt.pk)
			err := streamTable(ctx, tx, mt.name, mt.cols, mt.pk, func(row []any) error {
				if mt.name == "records" && row[recordsDeletedCol] == int64(1) {
					sum.Tombstones++
				}
				return tb.Add(row)
			})
			if err != nil {
				return err
			}
			tables = append(tables, tb.Digest())
			if afterMerkleTable != nil {
				afterMerkleTable(mt.name)
			}
		}
		rep = MerkleReport{Summary: sum, Digest: merkle.NewDigest(tables...)}
		return nil
	})
	return rep, err
}

// MerkleBucket is one bucket of one hashed table at applied index at: its
// leaves in key order and its hash, read like Merkle.
func (f *FSM) MerkleBucket(ctx context.Context, at uint64, table string, bucket int) (MerkleBucketReport, error) {
	idx := -1
	for i, mt := range merkleTables {
		if mt.name == table {
			idx = i
		}
	}
	if idx < 0 {
		return MerkleBucketReport{}, fmt.Errorf("fsm: merkle: table %q is not audited", table)
	}
	if bucket < 0 || bucket >= merkle.Buckets {
		return MerkleBucketReport{}, fmt.Errorf("fsm: merkle: bucket %d: must be 0 to %d", bucket, merkle.Buckets-1)
	}
	mt := merkleTables[idx]
	var rep MerkleBucketReport
	err := f.merkleRead(ctx, at, func(tx *sql.Tx, sum MerkleSummary) error {
		tb := merkle.NewTable(mt.name, mt.pk)
		tb.Collect(bucket)
		if err := streamTable(ctx, tx, mt.name, mt.cols, mt.pk, tb.Add); err != nil {
			return err
		}
		leaves := tb.Leaves()
		if leaves == nil {
			leaves = []merkle.Leaf{}
		}
		rep = MerkleBucketReport{AppliedIndex: sum.AppliedIndex, Table: table, Bucket: bucket,
			Hash: tb.Digest().Buckets[bucket], Leaves: leaves}
		return nil
	})
	return rep, err
}

// merkleRead runs fn in one read transaction after checking, with the
// transaction's first read, that it is at applied index at.
func (f *FSM) merkleRead(ctx context.Context, at uint64, fn func(*sql.Tx, MerkleSummary) error) error {
	tx, err := f.rdb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("fsm: merkle: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // read only: nothing to commit
	sum, err := readMerkleMeta(ctx, tx)
	if err != nil {
		return err
	}
	if at != AnyIndex && sum.AppliedIndex != at {
		return &IndexMismatchError{Want: at, Got: sum.AppliedIndex}
	}
	if afterMerkleTable != nil {
		afterMerkleTable("meta")
	}
	return fn(tx, sum)
}

// readMerkleMeta reads meta, which holds exactly schema_version and
// applied_index; any other key would be state the audit neither hashes nor
// reports, so it is an error.
func readMerkleMeta(ctx context.Context, tx *sql.Tx) (MerkleSummary, error) {
	rows, err := tx.QueryContext(ctx, `SELECT k, v FROM meta ORDER BY k`)
	if err != nil {
		return MerkleSummary{}, fmt.Errorf("fsm: merkle: meta: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var sum MerkleSummary
	seen := 0
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return MerkleSummary{}, fmt.Errorf("fsm: merkle: meta: %w", err)
		}
		switch k {
		case "schema_version":
			sum.SchemaVersion = v
		case "applied_index":
			if sum.AppliedIndex, err = strconv.ParseUint(v, 10, 64); err != nil {
				return MerkleSummary{}, fmt.Errorf("fsm: merkle: applied index %q: %w", v, err)
			}
		default:
			return MerkleSummary{}, fmt.Errorf("fsm: merkle: meta key %q is neither hashed nor summarised", k)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return MerkleSummary{}, fmt.Errorf("fsm: merkle: meta: %w", err)
	}
	if seen != 2 {
		return MerkleSummary{}, fmt.Errorf("fsm: merkle: meta has %d of its 2 keys", seen)
	}
	return sum, nil
}

// streamTable passes every row of table, in primary-key order, to fn. The
// slice is reused between rows.
func streamTable(ctx context.Context, tx *sql.Tx, table string, cols []string, pk int, fn func([]any) error) error {
	query := `SELECT ` + strings.Join(cols, ", ") + ` FROM ` + table + ` ORDER BY ` + strings.Join(cols[:pk], ", ")
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("fsm: merkle %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("fsm: merkle %s: %w", table, err)
		}
		if err := fn(vals); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("fsm: merkle %s: %w", table, err)
	}
	return nil
}
