// Package memstore is the in-memory store.Store (M8): the real internal/fsm
// rules on in-memory SQLite, one FSM per shard, commands applied serially and
// no Raft. Wrap it with WithFaults to inject lost responses, delays and
// conflicting writes.
//
// Stub: New returns an error until the FSM lands on s/S1.2-fsm-commands. The
// API is final, so workflow code and tests can be written against it now.
package memstore

import (
	"context"
	"errors"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

var errNotImplemented = errors.New("memstore: not implemented")

// Store implements store.Store with one internal/fsm FSM per shard.
type Store struct{}

var _ store.Store = (*Store)(nil)

type Option func(*config)

type config struct {
	shards int
	route  func(key string) int
}

// WithShards sets the shard count (default 3).
func WithShards(n int) Option { return func(c *config) { c.shards = n } }

// WithRouter replaces the default key-to-shard function (an FNV-1a hash until
// internal/ring lands).
func WithRouter(route func(key string) int) Option { return func(c *config) { c.route = route } }

func New(ctx context.Context, opts ...Option) (*Store, error) { return nil, errNotImplemented }

func (s *Store) Close() error { return errNotImplemented }

func (s *Store) CreateEntity(ctx context.Context, cmd store.CreateEntity) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) TransitionRequest(ctx context.Context, cmd store.TransitionRequest) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) TransitionResource(ctx context.Context, cmd store.TransitionResource) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) TryClaim(ctx context.Context, cmd store.TryClaim) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) CloseAttempt(ctx context.Context, cmd store.CloseAttempt) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) ReadRecord(ctx context.Context, key string) (store.Record, error) {
	return store.Record{}, errNotImplemented
}

func (s *Store) ReadClosure(ctx context.Context, resourceKey, attemptID string) (store.Closure, error) {
	return store.Closure{}, errNotImplemented
}

func (s *Store) Scan(ctx context.Context, q store.ScanQuery) (store.ScanResult, error) {
	return store.ScanResult{}, errNotImplemented
}

func (s *Store) PendingOutbox(ctx context.Context, shard, limit int) ([]store.OutboxEvent, error) {
	return nil, errNotImplemented
}

func (s *Store) Operation(ctx context.Context, key, operationID string) (store.Result, error) {
	return store.Result{}, errNotImplemented
}

func (s *Store) Shards() int { return 0 }
