package appmetrics

import (
	"context"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// ObservedStore delegates every command unchanged. It never adds reads,
// retries, IDs, timestamps or workflow decisions.
type ObservedStore struct {
	store.Store
	metrics *Process
}

func (p *Process) Store() store.Store { return &ObservedStore{Store: p.source, metrics: p} }
func (s *ObservedStore) CreateEntity(ctx context.Context, cmd store.CreateEntity) (store.Result, error) {
	result, err := s.Store.CreateEntity(ctx, cmd)
	s.metrics.observe("create", result, err)
	return result, err
}

func (s *ObservedStore) CompareAndSet(ctx context.Context, cmd store.CompareAndSet) (store.Result, error) {
	result, err := s.Store.CompareAndSet(ctx, cmd)
	s.metrics.observe("cas", result, err)
	return result, err
}

func (s *ObservedStore) TransitionRequest(ctx context.Context, cmd store.TransitionRequest) (store.Result, error) {
	result, err := s.Store.TransitionRequest(ctx, cmd)
	s.metrics.observe("request_transition", result, err)
	return result, err
}

func (s *ObservedStore) RetryWorkflow(ctx context.Context, cmd store.RetryWorkflow) (store.Result, error) {
	result, err := s.Store.RetryWorkflow(ctx, cmd)
	s.metrics.observe("retry", result, err)
	return result, err
}

func (s *ObservedStore) TransitionResource(ctx context.Context, cmd store.TransitionResource) (store.Result, error) {
	result, err := s.Store.TransitionResource(ctx, cmd)
	s.metrics.observe("resource_transition", result, err)
	return result, err
}

func (s *ObservedStore) TryClaim(ctx context.Context, cmd store.TryClaim) (store.Result, error) {
	result, err := s.Store.TryClaim(ctx, cmd)
	s.metrics.observe("claim", result, err)
	return result, err
}

func (s *ObservedStore) CloseAttempt(ctx context.Context, cmd store.CloseAttempt) (store.Result, error) {
	result, err := s.Store.CloseAttempt(ctx, cmd)
	s.metrics.observe("close", result, err)
	return result, err
}

func (s *ObservedStore) MarkOutboxPublished(ctx context.Context, cmd store.MarkOutboxPublished) (store.Result, error) {
	result, err := s.Store.MarkOutboxPublished(ctx, cmd)
	s.metrics.observe("outbox_mark", result, err)
	return result, err
}

func (s *ObservedStore) AgeRequest(ctx context.Context, cmd store.AgeRequest) (store.Result, error) {
	result, err := s.Store.AgeRequest(ctx, cmd)
	s.metrics.observe("age", result, err)
	return result, err
}
