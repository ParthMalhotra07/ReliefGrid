package fsm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

const schemaVersion = "1"

var schema = []string{
	`CREATE TABLE records (
		key        TEXT PRIMARY KEY,
		kind       TEXT NOT NULL,
		version    INTEGER NOT NULL,
		deleted    INTEGER NOT NULL,
		type       TEXT NOT NULL,
		status     TEXT NOT NULL,
		geohash    TEXT NOT NULL,
		body       TEXT NOT NULL,
		updated_at INTEGER NOT NULL
	)`,
	`CREATE INDEX records_scan ON records (kind, type, status, geohash)`,
	`CREATE TABLE operation_results (
		operation_id  TEXT PRIMARY KEY,
		payload_hash  TEXT NOT NULL,
		key           TEXT NOT NULL,
		result        TEXT NOT NULL,
		applied_index INTEGER NOT NULL
	)`,
	`CREATE TABLE attempt_closures (
		resource_key TEXT NOT NULL,
		attempt_id   TEXT NOT NULL,
		request_id   TEXT NOT NULL,
		fingerprint  TEXT NOT NULL,
		reason       TEXT NOT NULL,
		disposition  TEXT NOT NULL,
		closed_at    INTEGER NOT NULL,
		PRIMARY KEY (resource_key, attempt_id)
	)`,
	`CREATE TABLE outbox (
		seq            INTEGER PRIMARY KEY,
		event_id       TEXT NOT NULL UNIQUE,
		entity_key     TEXT NOT NULL,
		entity_version INTEGER NOT NULL,
		event          TEXT NOT NULL,
		published      INTEGER NOT NULL,
		created_at     INTEGER NOT NULL
	)`,
	`CREATE INDEX outbox_pending ON outbox (published, seq)`,
	`CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)`,
}

// dumpOrder lists every table with the key it is dumped in.
var dumpOrder = []struct{ table, orderBy string }{
	{"records", "key"},
	{"operation_results", "operation_id"},
	{"attempt_closures", "resource_key, attempt_id"},
	{"outbox", "seq"},
	{"meta", "k"},
}

// FSM is one shard's deterministic state machine on SQLite. Apply must be
// called serially in log order; reads may run concurrently with it.
type FSM struct {
	db      *sql.DB
	applied atomic.Uint64
}

// Open creates the schema in a fresh database at dsn (for example
// "file:shard0?mode=memory"). The FSM file is derived state (M3): Open
// refuses a database that already has a schema, so callers always rebuild
// it from a snapshot and the log instead of trusting an old file.
func Open(ctx context.Context, dsn string) (*FSM, error) {
	db, err := openDB(dsn)
	if err != nil {
		return nil, err
	}
	f := &FSM{db: db}
	if err := f.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return f, nil
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("fsm: open %s: %w", dsn, err)
	}
	// One connection: an in-memory database exists per connection, and
	// Apply is serial anyway. The connection is never recycled.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	return db, nil
}

func setPragmas(ctx context.Context, db *sql.DB) error {
	for _, p := range []string{`PRAGMA journal_mode = WAL`, `PRAGMA synchronous = OFF`} {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("fsm: %s: %w", p, err)
		}
	}
	return nil
}

func (f *FSM) init(ctx context.Context) error {
	var n int
	if err := f.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type = 'table'`).Scan(&n); err != nil {
		return fmt.Errorf("fsm: inspect database: %w", err)
	}
	if n != 0 {
		return errors.New("fsm: database is not empty; FSM state is derived and must be rebuilt from snapshot and log")
	}
	if err := setPragmas(ctx, f.db); err != nil {
		return err
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("fsm: begin schema: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, stmt := range schema {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("fsm: create schema: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO meta (k, v) VALUES ('schema_version', ?), ('applied_index', '0')`, schemaVersion); err != nil {
		return fmt.Errorf("fsm: init meta: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fsm: commit schema: %w", err)
	}
	return nil
}

// OpenImage opens a database file written by VacuumInto (a snapshot image),
// checks its schema version and takes its applied index. It sets WAL and
// synchronous=OFF like Open.
func OpenImage(ctx context.Context, path string) (*FSM, error) {
	// SQLite would create a missing file; an image must already exist.
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("fsm: image %s: %w", path, err)
	}
	db, err := openDB("file:" + path)
	if err != nil {
		return nil, err
	}
	f := &FSM{db: db}
	if err := f.loadImage(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("fsm: image %s: %w", path, err)
	}
	return f, nil
}

func (f *FSM) loadImage(ctx context.Context) error {
	if err := setPragmas(ctx, f.db); err != nil {
		return err
	}
	var version, applied string
	if err := f.db.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = 'schema_version'`).Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version != schemaVersion {
		return fmt.Errorf("schema version %q, want %q", version, schemaVersion)
	}
	if err := f.db.QueryRowContext(ctx, `SELECT v FROM meta WHERE k = 'applied_index'`).Scan(&applied); err != nil {
		return fmt.Errorf("read applied index: %w", err)
	}
	n, err := strconv.ParseUint(applied, 10, 64)
	if err != nil {
		return fmt.Errorf("applied index %q: %w", applied, err)
	}
	f.applied.Store(n)
	return nil
}

// VacuumInto writes a consistent copy of the database to path, which must
// not exist. Raft serialises FSM.Snapshot with Apply, so the copy is exactly
// the state at AppliedIndex.
func (f *FSM) VacuumInto(ctx context.Context, path string) error {
	if _, err := f.db.ExecContext(ctx, `VACUUM INTO ?`, path); err != nil {
		return fmt.Errorf("fsm: vacuum into %s: %w", path, err)
	}
	return nil
}

// JournalMode reports SQLite's journal_mode ("wal" for every FSM database).
func (f *FSM) JournalMode(ctx context.Context) (string, error) {
	var mode string
	if err := f.db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
		return "", fmt.Errorf("fsm: journal mode: %w", err)
	}
	return mode, nil
}

func (f *FSM) Close() error { return f.db.Close() }

// AppliedIndex is the index of the last entry whose transaction committed.
func (f *FSM) AppliedIndex() uint64 { return f.applied.Load() }

// Apply applies one committed log entry at index in a single SQLite
// transaction. The returned error is an infrastructure failure, after which
// the replica must stop; every domain outcome is in Result.
func (f *FSM) Apply(index uint64, entry []byte) (store.Result, error) {
	if applied := f.applied.Load(); index <= applied {
		return store.Result{}, fmt.Errorf("fsm: entry %d is not after applied index %d", index, applied)
	}
	ctx := context.Background() // Apply is local and must run to completion
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return store.Result{}, fmt.Errorf("fsm: begin apply %d: %w", index, err)
	}
	defer func() { _ = tx.Rollback() }()

	t := &txn{ctx: ctx, tx: tx, index: index}
	res, err := t.apply(entry)
	if err != nil {
		return store.Result{}, fmt.Errorf("fsm: apply %d: %w", index, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE meta SET v = ? WHERE k = 'applied_index'`, strconv.FormatUint(index, 10)); err != nil {
		return store.Result{}, fmt.Errorf("fsm: apply %d: record index: %w", index, err)
	}
	if err := tx.Commit(); err != nil {
		return store.Result{}, fmt.Errorf("fsm: commit %d: %w", index, err)
	}
	f.applied.Store(index)
	return res, nil
}

func (f *FSM) ReadRecord(ctx context.Context, key string) (store.Record, error) {
	rec, ok, err := readRecord(ctx, f.db, key)
	if err != nil {
		return store.Record{}, err
	}
	if !ok {
		return store.Record{}, fmt.Errorf("record %q: %w", key, store.ErrNotFound)
	}
	return rec, nil
}

func (f *FSM) ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error) {
	c, ok, err := readClosure(ctx, f.db, resourceKey, attemptID)
	if err != nil {
		return store.Closure{}, err
	}
	if !ok {
		return store.Closure{}, fmt.Errorf("closure %s/%s: %w", resourceKey, attemptID, store.ErrNotFound)
	}
	return c, nil
}

// Scan returns live records matching q in key order.
func (f *FSM) Scan(ctx context.Context, q store.ScanQuery) ([]store.Record, error) {
	where := []string{"kind = ?", "deleted = 0"}
	args := []any{string(q.Kind)}
	if q.Type != "" {
		where = append(where, "type = ?")
		args = append(args, string(q.Type))
	}
	if len(q.Statuses) > 0 {
		where = append(where, "status IN ("+placeholders(len(q.Statuses))+")")
		for _, s := range q.Statuses {
			args = append(args, s)
		}
	}
	if len(q.GeohashPrefixes) > 0 {
		var ranges []string
		for _, p := range q.GeohashPrefixes {
			ranges = append(ranges, "(geohash >= ? AND geohash < ?)")
			args = append(args, p, p+"\xff")
		}
		where = append(where, "("+strings.Join(ranges, " OR ")+")")
	}
	query := `SELECT body FROM records WHERE ` + strings.Join(where, " AND ") + ` ORDER BY key`
	if q.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, q.Limit)
	}
	rows, err := f.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("fsm: scan: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []store.Record
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, fmt.Errorf("fsm: scan row: %w", err)
		}
		var rec store.Record
		if err := json.Unmarshal([]byte(body), &rec); err != nil {
			return nil, fmt.Errorf("fsm: scan decode: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fsm: scan: %w", err)
	}
	return out, nil
}

// PendingOutbox returns unpublished events in commit order.
func (f *FSM) PendingOutbox(ctx context.Context, limit int) ([]store.OutboxEvent, error) {
	query, args := `SELECT event FROM outbox WHERE published = 0 ORDER BY seq`, []any{}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := f.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("fsm: outbox: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []store.OutboxEvent
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			return nil, fmt.Errorf("fsm: outbox row: %w", err)
		}
		var ev store.OutboxEvent
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			return nil, fmt.Errorf("fsm: outbox decode: %w", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fsm: outbox: %w", err)
	}
	return out, nil
}

// Operation returns the stored result of an operation, marked Replayed: it
// is history, never proof of current ownership.
func (f *FSM) Operation(ctx context.Context, operationID string) (store.Result, error) {
	res, _, ok, err := readOperation(ctx, f.db, operationID)
	if err != nil {
		return store.Result{}, err
	}
	if !ok {
		return store.Result{}, fmt.Errorf("operation %q: %w", operationID, store.ErrNotFound)
	}
	res.Replayed = true
	return res, nil
}

// Dump returns every row of every FSM table in a canonical, sorted text form,
// so two replicas (or a replica and its replay) can be compared logically
// rather than byte-for-byte.
func (f *FSM) Dump(ctx context.Context) (map[string][]string, error) {
	out := make(map[string][]string, len(dumpOrder))
	for _, d := range dumpOrder {
		rows, err := f.db.QueryContext(ctx, `SELECT * FROM `+d.table+` ORDER BY `+d.orderBy)
		if err != nil {
			return nil, fmt.Errorf("fsm: dump %s: %w", d.table, err)
		}
		lines, err := dumpRows(rows)
		_ = rows.Close()
		if err != nil {
			return nil, fmt.Errorf("fsm: dump %s: %w", d.table, err)
		}
		out[d.table] = lines
	}
	return out, nil
}

func dumpRows(rows *sql.Rows) ([]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var lines []string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		fields := make([]string, len(cols))
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			fields[i] = fmt.Sprintf("%s=%v", cols[i], v)
		}
		lines = append(lines, strings.Join(fields, " | "))
	}
	return lines, rows.Err()
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// querier is satisfied by *sql.DB and *sql.Tx.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func readRecord(ctx context.Context, q querier, key string) (store.Record, bool, error) {
	var body string
	err := q.QueryRowContext(ctx, `SELECT body FROM records WHERE key = ?`, key).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Record{}, false, nil
	}
	if err != nil {
		return store.Record{}, false, fmt.Errorf("fsm: read %q: %w", key, err)
	}
	var rec store.Record
	if err := json.Unmarshal([]byte(body), &rec); err != nil {
		return store.Record{}, false, fmt.Errorf("fsm: decode %q: %w", key, err)
	}
	return rec, true, nil
}

func readClosure(ctx context.Context, q querier, resourceKey, attemptID string) (store.Closure, bool, error) {
	c := store.Closure{ResourceKey: resourceKey, AttemptID: attemptID}
	var disposition string
	err := q.QueryRowContext(ctx,
		`SELECT request_id, fingerprint, reason, disposition, closed_at FROM attempt_closures WHERE resource_key = ? AND attempt_id = ?`,
		resourceKey, attemptID).Scan(&c.RequestID, &c.Fingerprint, &c.Reason, &disposition, &c.ClosedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Closure{}, false, nil
	}
	if err != nil {
		return store.Closure{}, false, fmt.Errorf("fsm: read closure %s/%s: %w", resourceKey, attemptID, err)
	}
	c.Disposition = store.ResourceStatus(disposition)
	return c, true, nil
}

func readOperation(ctx context.Context, q querier, operationID string) (store.Result, string, bool, error) {
	var body, hash string
	err := q.QueryRowContext(ctx, `SELECT result, payload_hash FROM operation_results WHERE operation_id = ?`, operationID).Scan(&body, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return store.Result{}, "", false, nil
	}
	if err != nil {
		return store.Result{}, "", false, fmt.Errorf("fsm: read operation %q: %w", operationID, err)
	}
	var res store.Result
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		return store.Result{}, "", false, fmt.Errorf("fsm: decode operation %q: %w", operationID, err)
	}
	return res, hash, true, nil
}
