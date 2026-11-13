package mq_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	amqp "github.com/rabbitmq/amqp091-go"
)

// staleCoordinator answers the matcher's ReadRecord with a current PENDING
// request, labelled as served in the given mode.
func staleCoordinator(t *testing.T, served internalapi.ReadInfoConsistency) *coordclient.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var answer any
		switch r.Method + " " + r.URL.Path {
		case "GET /internal/ring":
			answer = map[string]any{"ringVersion": 1, "shards": 3, "vnodesPerShard": 128, "hash": "xxhash64", "checksum": "test-stale-read"}
		case "GET /internal/data/" + store.RequestKey(requestID):
			if got := r.URL.Query().Get("consistency"); got != "linearizable" {
				t.Errorf("matcher read sent consistency=%q, want linearizable (the default)", got)
			}
			answer = internalapi.RecordResponse{RingVersion: 1,
				Record: store.Record{Key: store.RequestKey(requestID), Kind: store.KindRequest, Version: 1,
					Request: &store.Request{ID: requestID, Type: store.TypeWater, Status: store.RequestPending, PeopleAffected: 1, Urgency: 1}},
				Read: internalapi.ReadInfo{Consistency: served, ServedBy: "storage-0-1", AppliedIndex: 7, StalePossible: served == internalapi.ReadInfoConsistencyStale}}
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusTeapot)
			return
		}
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(srv.Close)
	c, err := coordclient.New(t.Context(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// S4.1: the matcher's first read of a delivery (ReadRecord, no read mode set,
// so linearizable) answered stale is a retryable *coordclient.Error. The
// consumer treats it like any failed read: never a permanent absence (no
// reject), never handed to the workflow, and acknowledged only after the
// delivery's confirmed delayed handoff; without a retry path it stays unacked.
// The control: the same record served linearizable reaches the handler.
func TestMatcherStaleReadIsDeferredNeverRejectedOrHandled(t *testing.T) {
	t.Run("stale, deferred through retry", func(t *testing.T) {
		ack := &ackLog{t: t}
		var deferred int
		c := mq.Consumer{Store: staleCoordinator(t, internalapi.ReadInfoConsistencyStale),
			Handle: func(context.Context, store.Record) error { t.Fatal("a stale record reached the workflow"); return nil },
			Retry: func(_ context.Context, _ amqp.Delivery) error {
				if ack.acks != 0 {
					t.Fatal("acknowledged before the delayed handoff")
				}
				deferred++
				return nil
			}}
		if err := c.Process(t.Context(), eventDelivery(t, ack)); err != nil {
			t.Fatalf("Process: %v", err)
		}
		if deferred != 1 || ack.acks != 1 || ack.rejects+ack.nacks != 0 {
			t.Fatalf("stale read: deferred %d, disposition %+v; want one handoff then one ack", deferred, ack)
		}
	})
	t.Run("stale, no retry path", func(t *testing.T) {
		ack := &ackLog{t: t}
		c := mq.Consumer{Store: staleCoordinator(t, internalapi.ReadInfoConsistencyStale),
			Handle: func(context.Context, store.Record) error { t.Fatal("a stale record reached the workflow"); return nil }}
		err := c.Process(t.Context(), eventDelivery(t, ack))
		var ce *coordclient.Error
		if !errors.As(err, &ce) || !ce.Retryable || errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUnknownOutcome) {
			t.Fatalf("Process err = %v, want the retryable stale-read error", err)
		}
		if ack.acks+ack.rejects+ack.nacks != 0 {
			t.Fatalf("stale read without a retry path was disposed of: %+v; want it left unacked", ack)
		}
	})
	t.Run("linearizable control", func(t *testing.T) {
		ack := &ackLog{t: t}
		handled := 0
		c := mq.Consumer{Store: staleCoordinator(t, internalapi.ReadInfoConsistencyLinearizable),
			Handle: func(_ context.Context, rec store.Record) error {
				if rec.Request == nil || rec.Request.ID != requestID {
					t.Fatalf("handler got %+v", rec)
				}
				handled++
				return nil
			},
			Retry: func(context.Context, amqp.Delivery) error { t.Fatal("a good read was deferred"); return nil }}
		if err := c.Process(t.Context(), eventDelivery(t, ack)); err != nil {
			t.Fatalf("Process: %v", err)
		}
		if handled != 1 || ack.acks != 1 || ack.rejects+ack.nacks != 0 {
			t.Fatalf("linearizable read: handled %d, disposition %+v", handled, ack)
		}
	})
}
