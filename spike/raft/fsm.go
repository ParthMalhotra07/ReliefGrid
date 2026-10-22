package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/hashicorp/raft"
	_ "modernc.org/sqlite"
)

// casCommand is the only command: set Key to Value if its version is ExpectedVersion.
// A missing key has version 0.
type casCommand struct {
	Key             string `json:"key"`
	ExpectedVersion int64  `json:"expectedVersion"`
	Value           string `json:"value"`
}

// casResult is what Apply returns. On a conflict, Value and Version are the current ones.
// Err is a string so the result never carries a non-deterministic error value.
type casResult struct {
	Value    string
	Version  int64
	Conflict bool
	Err      string
}

// fsm is a single-table SQLite state machine. The file is derived (M3): openFSM
// deletes it, and Raft rebuilds it from the snapshot plus the log.
//
// applied is the index of the last command whose effects reads can see. Raft's own
// AppliedIndex() is not that: it advances when committed entries are queued for the
// FSM goroutine, before Apply runs (processLogs in raft.go, v1.8.0).
type fsm struct {
	db      *sql.DB
	applied atomic.Uint64
}

func openFSM(path string) (*fsm, error) {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove derived fsm file: %w", err)
		}
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)")
	if err != nil {
		return nil, fmt.Errorf("open fsm: %w", err)
	}
	db.SetMaxOpenConns(1) // Apply and the HTTP reads share one connection
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL)`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create fsm schema: %w", err)
	}
	return &fsm{db: db}, nil
}

func (f *fsm) Close() error { return f.db.Close() }

// Apply checks the version and writes in one transaction. It uses only the
// command bytes: no clock, randomness or map iteration.
func (f *fsm) Apply(l *raft.Log) any {
	defer f.applied.Store(l.Index)
	var cmd casCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return casResult{Err: fmt.Sprintf("decode command at index %d: %v", l.Index, err)}
	}
	res, err := f.cas(cmd)
	if err != nil {
		// A storage error here would make replicas diverge; the real FSM must stop
		// the node instead. The spike reports it.
		return casResult{Err: err.Error()}
	}
	return res
}

// Applied is the index of the last command applied to SQLite.
func (f *fsm) Applied() uint64 { return f.applied.Load() }

func (f *fsm) cas(cmd casCommand) (casResult, error) {
	ctx := context.Background() // Raft callbacks carry no context; this is local SQLite
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return casResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var cur casResult
	err = tx.QueryRowContext(ctx, `SELECT value, version FROM kv WHERE key = ?`, cmd.Key).Scan(&cur.Value, &cur.Version)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return casResult{}, err
	}
	if cur.Version != cmd.ExpectedVersion {
		cur.Conflict = true
		return cur, nil
	}
	next := casResult{Value: cmd.Value, Version: cur.Version + 1}
	if _, err := tx.ExecContext(ctx, `INSERT INTO kv (key, value, version) VALUES (?, ?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value, version = excluded.version`,
		cmd.Key, next.Value, next.Version); err != nil {
		return casResult{}, err
	}
	return next, tx.Commit()
}

// Get reads the local copy; on a follower it may lag the leader.
func (f *fsm) Get(key string) (value string, version int64, err error) {
	ctx := context.Background()
	err = f.db.QueryRowContext(ctx, `SELECT value, version FROM kv WHERE key = ?`, key).Scan(&value, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, nil
	}
	return value, version, err
}

type kvRow struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version int64  `json:"version"`
}

// Snapshot runs on the FSM goroutine, so no Apply runs concurrently; it copies the
// (tiny) table into memory and Persist writes it out later.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	ctx := context.Background()
	rows, err := f.db.QueryContext(ctx, `SELECT key, value, version FROM kv ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	snap := kvSnapshot{Applied: f.Applied()}
	for rows.Next() {
		var r kvRow
		if err := rows.Scan(&r.Key, &r.Value, &r.Version); err != nil {
			return nil, err
		}
		snap.Rows = append(snap.Rows, r)
	}
	return snap, rows.Err()
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	ctx := context.Background()
	defer func() { _ = rc.Close() }()
	var snap kvSnapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM kv`); err != nil {
		return err
	}
	for _, r := range snap.Rows {
		if _, err := tx.ExecContext(ctx, `INSERT INTO kv (key, value, version) VALUES (?, ?, ?)`, r.Key, r.Value, r.Version); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	f.applied.Store(snap.Applied)
	return nil
}

type kvSnapshot struct {
	Applied uint64  `json:"applied"`
	Rows    []kvRow `json:"rows"`
}

func (s kvSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("write snapshot: %w", err)
	}
	return sink.Close()
}

func (kvSnapshot) Release() {}
