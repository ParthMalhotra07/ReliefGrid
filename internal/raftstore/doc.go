// Package raftstore wires HashiCorp Raft, its durable log and stable stores,
// and file snapshots to the FSM for one shard replica.
//
// On the PVC (Config.DataDir) it keeps the node identity, the raft-boltdb log
// and stable store, and the file snapshot store, in separate entries. The
// SQLite FSM lives in Config.FSMDir (an emptyDir): it is derived state, wiped
// on Open and rebuilt from the latest snapshot plus the log (M3).
//
// A node never bootstraps itself. A fresh disk waits with an empty
// configuration until Bootstrap is called explicitly, and Bootstrap refuses
// if any Raft state exists.
//
// Everything that waits for "applied up to N" (strong reads, readiness) uses
// the FSM's own completed index, never raft.AppliedIndex(), which advances
// when entries are queued for the FSM goroutine rather than when SQLite has
// committed them.
package raftstore
