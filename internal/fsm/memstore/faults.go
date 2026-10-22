package memstore

import (
	"context"
	"errors"
	"fmt"
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
	// It needs the wrapped store to be a *Store.
	FailShards []int
}

// Hook chooses a fault for each call; return the zero Fault for none.
type Hook func(c Call) Fault

// WithFaults wraps s so that hook can inject faults into every call.
func WithFaults(s store.Store, hook Hook) store.Store {
	if hook == nil {
		hook = func(Call) Fault { return Fault{} }
	}
	return &faulty{s: s, hook: hook}
}

type faulty struct {
	s    store.Store
	hook Hook
}

var errDropped = errors.New("memstore: response dropped by fault hook")

// run applies f around call.
func run[T any](ctx context.Context, f Fault, call func(context.Context) (T, error)) (T, error) {
	var zero T
	if f.Before != nil {
		if err := f.Before(ctx); err != nil {
			return zero, err
		}
	}
	if f.Delay > 0 {
		timer := time.NewTimer(f.Delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return zero, fmt.Errorf("%w: %w", store.ErrUnknownOutcome, ctx.Err())
		}
	}
	if f.DropBefore {
		return zero, fmt.Errorf("%w: %w", store.ErrUnknownOutcome, errDropped)
	}
	v, err := call(ctx)
	if err == nil && f.DropResponse {
		return zero, fmt.Errorf("%w: %w", store.ErrUnknownOutcome, errDropped)
	}
	return v, err
}

func mutate[C any](ctx context.Context, w *faulty, method, key string, meta store.Meta, cmd C,
	fn func(context.Context, C) (store.Result, error)) (store.Result, error) {
	f := w.hook(Call{Method: method, Key: key, OperationID: meta.OperationID})
	return run(ctx, f, func(ctx context.Context) (store.Result, error) { return fn(ctx, cmd) })
}

func (w *faulty) CreateEntity(ctx context.Context, cmd store.CreateEntity) (store.Result, error) {
	return mutate(ctx, w, "CreateEntity", cmd.Key, cmd.Meta, cmd, w.s.CreateEntity)
}

func (w *faulty) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	return mutate(ctx, w, "CompareAndSet", cmd.Key, cmd.Meta, cmd, w.s.CompareAndSet)
}

func (w *faulty) TransitionRequest(ctx context.Context, cmd store.TransitionRequest) (store.Result, error) {
	return mutate(ctx, w, "TransitionRequest", cmd.Key, cmd.Meta, cmd, w.s.TransitionRequest)
}

func (w *faulty) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	return mutate(ctx, w, "RetryWorkflow", cmd.Key, cmd.Meta, cmd, w.s.RetryWorkflow)
}

func (w *faulty) TransitionResource(ctx context.Context, cmd store.TransitionResource) (store.Result, error) {
	return mutate(ctx, w, "TransitionResource", cmd.Key, cmd.Meta, cmd, w.s.TransitionResource)
}

func (w *faulty) TryClaim(ctx context.Context, cmd store.TryClaim) (store.Result, error) {
	return mutate(ctx, w, "TryClaim", cmd.ResourceKey, cmd.Meta, cmd, w.s.TryClaim)
}

func (w *faulty) CloseAttempt(ctx context.Context, cmd store.CloseAttempt) (store.Result, error) {
	return mutate(ctx, w, "CloseAttempt", cmd.ResourceKey, cmd.Meta, cmd, w.s.CloseAttempt)
}

func (w *faulty) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	return mutate(ctx, w, "MarkOutboxPublished", "", cmd.Meta, cmd, w.s.MarkOutboxPublished)
}

func (w *faulty) ReadRecord(ctx context.Context, key string) (store.Record, error) {
	return run(ctx, w.hook(Call{Method: "ReadRecord", Key: key}), func(ctx context.Context) (store.Record, error) {
		return w.s.ReadRecord(ctx, key)
	})
}

func (w *faulty) ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error) {
	return run(ctx, w.hook(Call{Method: "ReadClosure", Key: resourceKey}), func(ctx context.Context) (store.Closure, error) {
		return w.s.ReadClosure(ctx, resourceKey, attemptID)
	})
}

func (w *faulty) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	f := w.hook(Call{Method: "Scan"})
	return run(ctx, f, func(ctx context.Context) (store.ScanResult, error) {
		if len(f.FailShards) == 0 {
			return w.s.Scan(ctx, q)
		}
		ms, ok := w.s.(*Store)
		if !ok {
			return store.ScanResult{}, fmt.Errorf("memstore: FailShards needs a *memstore.Store, have %T", w.s)
		}
		return ms.scan(ctx, q, f.FailShards)
	})
}

func (w *faulty) PendingOutbox(ctx context.Context, shard, limit int) ([]store.OutboxEvent, error) {
	return run(ctx, w.hook(Call{Method: "PendingOutbox"}), func(ctx context.Context) ([]store.OutboxEvent, error) {
		return w.s.PendingOutbox(ctx, shard, limit)
	})
}

func (w *faulty) Operation(ctx context.Context, key, operationID string) (store.Result, error) {
	return run(ctx, w.hook(Call{Method: "Operation", Key: key}), func(ctx context.Context) (store.Result, error) {
		return w.s.Operation(ctx, key, operationID)
	})
}

func (w *faulty) Shards() int { return w.s.Shards() }
