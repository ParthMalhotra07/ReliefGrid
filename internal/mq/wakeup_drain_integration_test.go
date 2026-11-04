//go:build integration && a32

package mq_test

import (
	"context"
	"fmt"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"testing"
)

func TestDrainIntegrationWakeupFinishesCommittedHandoff(t *testing.T) {
	ctx, conn, _ := isolatedBroker(t)
	_, s := realStore(t)
	entered, release := make(chan struct{}), make(chan struct{})
	c := mq.WakeupConsumer{Handle: func(work context.Context, w mq.Wakeup) error {
		close(entered)
		<-release
		rec, err := s.ReadRecord(work, store.RequestKey(w.EntityID))
		if err != nil {
			return err
		}
		result, err := s.TransitionRequest(work, store.TransitionRequest{Meta: store.Meta{OperationID: "wake-drain-handoff", At: 2}, Key: rec.Key, ExpectedVersion: rec.Version, ExpectedStatus: rec.Request.Status, To: store.RequestWaiting, NextAttemptAt: 5000})
		if err != nil {
			return err
		}
		if result.Outcome != store.OutcomeOK {
			return fmt.Errorf("handoff rejected: %s", result.Outcome)
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
	publishConfirmed(t, ctx, ch, "relief.events", "request.status.cancelling", wakeDelivery(t, nil).Body, 1)
	select {
	case <-entered:
	case <-ctx.Done():
		close(release)
		t.Fatal("wakeup never entered")
	}
	cancel()
	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("wakeup failed to join")
	}
	rec, err := s.ReadRecord(ctx, store.RequestKey(requestID))
	if err != nil || rec.Request.Status != store.RequestWaiting {
		t.Fatalf("shutdown cancelled admitted wakeup handoff: %+v %v", rec.Request, err)
	}
	d, ok, err := ch.Get(mq.WakeupQueue, false)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		_ = d.Reject(false)
		t.Fatal("committed wakeup was not acknowledged before channel close")
	}
	t.Log("admitted wakeup committed and acknowledged before shutdown")
}
