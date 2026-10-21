// Package fsm is the deterministic SQLite state machine that each shard's
// Raft group applies. Every precondition is checked inside the committed
// apply, in one SQLite transaction.
package fsm
