package fsm

import "context"

// CorruptRow runs a statement on the write connection, outside any command:
// the negative control's way of making one replica diverge, which no
// production path can do.
func (f *FSM) CorruptRow(ctx context.Context, stmt string, args ...any) error {
	_, err := f.db.ExecContext(ctx, stmt, args...)
	return err
}

// SchemaColumns lists every table in the schema with its columns in
// declared order, from SQLite itself.
func (f *FSM) SchemaColumns(ctx context.Context) (map[string][]string, error) {
	rows, err := f.reads().QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			_ = rows.Close()
			return nil, err
		}
		tables = append(tables, n)
	}
	_ = rows.Close()
	out := map[string][]string{}
	for _, tb := range tables {
		cols, err := f.reads().QueryContext(ctx, `SELECT name FROM pragma_table_info(?) ORDER BY cid`, tb)
		if err != nil {
			return nil, err
		}
		for cols.Next() {
			var c string
			if err := cols.Scan(&c); err != nil {
				_ = cols.Close()
				return nil, err
			}
			out[tb] = append(out[tb], c)
		}
		_ = cols.Close()
	}
	return out, nil
}

// MerkleColumns is what the audit hashes: table, then columns in order.
func MerkleColumns() map[string][]string {
	out := map[string][]string{}
	for _, t := range merkleTables {
		out[t.name] = t.cols
	}
	return out
}

// SetAfterMerkleTable makes Merkle call fn after it reads each table, until
// the test ends.
func SetAfterMerkleTable(t interface{ Cleanup(func()) }, fn func(table string)) {
	afterMerkleTable = fn
	t.Cleanup(func() { afterMerkleTable = nil })
}
