package fsm

import "context"

// HoldRead opens a transaction on the pool reads use, reads from it so it
// holds a WAL read snapshot, and keeps it until release is called.
func (f *FSM) HoldRead(ctx context.Context) (release func(), err error) {
	tx, err := f.reads().BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM records`).Scan(&n); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return func() { _ = tx.Rollback() }, nil
}

// ExecOnReadPool runs a statement on the pool reads use.
func (f *FSM) ExecOnReadPool(ctx context.Context, stmt string) error {
	_, err := f.reads().ExecContext(ctx, stmt)
	return err
}

// SetAfterDumpTable makes Dump call fn after it reads each table, until the
// test ends.
func SetAfterDumpTable(t interface{ Cleanup(func()) }, fn func(table string)) {
	afterDumpTable = fn
	t.Cleanup(func() { afterDumpTable = nil })
}
