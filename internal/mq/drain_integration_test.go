//go:build integration && a32

package mq_test

import (
	"context"
	"errors"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"testing"
)

func TestDrainIntegrationFinishesAdmittedDurableHandoff(t *testing.T) {
	ctx, conn, _ := isolatedBroker(t)
	_, s := realStore(t)
	defer func() { _ = s.Close() }()
	entered, release := make(chan struct{}), make(chan struct{})
	c := mq.Consumer{Store: s, Handle: func(work context.Context, rec store.Record) error {
		close(entered)
		<-release
		result, err := s.TransitionRequest(work, store.TransitionRequest{Meta: store.Meta{OperationID: "drain-handoff", At: 2}, Key: rec.Key, ExpectedVersion: rec.Version, ExpectedStatus: store.RequestPending, To: store.RequestWaiting, NextAttemptAt: 5000})
		if err != nil {
			return err
		}
		if result.Outcome != store.OutcomeOK {
			return errors.New("durable handoff rejected")
		}
		return nil
	}}
	admission, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Consume(admission, conn) }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	publishConfirmed(t, ctx, ch, "relief.requests", "request.created.medicine", eventDelivery(t, nil).Body, 2)
	select {
	case <-entered:
	case <-ctx.Done():
		close(release)
		t.Fatal("handler never entered")
	}
	cancel()
	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("consumer did not drain")
	}
	rec, err := s.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil || rec.Request.Status != store.RequestWaiting {
		t.Fatalf("SIGTERM cancelled admitted durable handoff: %+v %v", rec.Request, err)
	}
	d, ok, err := ch.Get(mq.MatchingQueue, false)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		_ = d.Reject(false)
		t.Fatal("completed handoff was not acknowledged before consumer closed")
	}
	t.Log("SIGTERM: admitted durable handoff committed and acked before channel closed")
}
