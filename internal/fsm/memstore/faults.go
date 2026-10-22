package memstore

import (
	"context"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Call describes one Store call seen by a fault Hook.
type Call struct {
	Method      string // "TryClaim", "ReadRecord", "Scan", ...
	Key         string // routed key; "" for Scan, PendingOutbox and MarkOutboxPublished
	OperationID string // "" for reads
}

// Fault is what a Hook asks the wrapper to do to one call.
type Fault struct {
	// Before runs before the call reaches the store: use it as a barrier, or
	// to commit a conflicting write first. Its error is returned unchanged.
	Before func(ctx context.Context) error
	// Delay waits before the call reaches the store. If ctx ends first the
	// command is not applied and ErrUnknownOutcome is returned.
	Delay time.Duration
	// DropBefore returns ErrUnknownOutcome without applying the command.
	DropBefore bool
	// DropResponse applies the command, then returns ErrUnknownOutcome.
	DropResponse bool
	// FailShards makes Scan report these shards as failed (Partial = true).
	FailShards []int
}

// Hook chooses a fault for each call; return the zero Fault for none.
type Hook func(c Call) Fault

// WithFaults wraps s so that hook can inject faults into every call.
func WithFaults(s store.Store, hook Hook) store.Store {
	panic("memstore: WithFaults not implemented")
}
