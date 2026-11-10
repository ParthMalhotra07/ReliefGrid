package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestAgeRequestLostResponseRetainsImmutableCommand(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	const at = int64(1792000000000)
	q := store.Request{ID: "aging", Type: store.TypeWater, Geohash: "s000000", PeopleAffected: 1, Urgency: 1}
	if r, err := s.CreateEntity(ctx, store.CreateEntity{Meta: store.Meta{OperationID: "create", At: at}, Key: "req:aging", Request: &q}); err != nil || r.Outcome != store.OutcomeOK {
		t.Fatalf("create: %+v %v", r, err)
	}
	calls := 0
	wrapped := WithFaults(s, func(c Call) Fault {
		if c.Method == "AgeRequest" {
			calls++
			return Fault{DropResponse: calls == 1}
		}
		return Fault{}
	})
	method := reflect.ValueOf(wrapped).MethodByName("AgeRequest")
	if !method.IsValid() {
		t.Fatal("fault-wrapped memstore is missing AgeRequest")
	}
	typ := method.Type()
	if typ.NumIn() != 2 || typ.NumOut() != 2 {
		t.Fatalf("unexpected signature: %s", typ)
	}
	cmd := reflect.New(typ.In(1))
	body := `{"operationId":"age","at":1792000001000,"key":"req:aging","expectedVersion":1,"agingIntervalMS":1000,"outbox":[{"eventId":"age-event","eventType":"request.retry","schemaVersion":1,"exchange":"relief.requests","routingKey":"request.retry","payload":{"schemaVersion":1,"eventId":"age-event","entityId":"aging","entityVersion":2,"eventType":"request.retry","occurredAt":"2026-10-14T17:46:41Z","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","payload":{"requestId":"aging","workflowGeneration":0,"nextAttemptAt":1792000001000,"urgency":2}},"published":false}]}`
	if err := json.Unmarshal([]byte(body), cmd.Interface()); err != nil {
		t.Fatal(err)
	}
	invoke := func() (store.Result, error) {
		values := method.Call([]reflect.Value{reflect.ValueOf(ctx), cmd.Elem()})
		r, ok := values[0].Interface().(store.Result)
		if !ok {
			t.Fatal("AgeRequest must return store.Result")
		}
		if !values[1].IsNil() {
			return r, values[1].Interface().(error)
		}
		return r, nil
	}
	if _, err := invoke(); !errors.Is(err, store.ErrUnknownOutcome) {
		t.Fatalf("lost reply: %v", err)
	}
	before, err := s.ReadRecord(ctx, "req:aging")
	if err != nil {
		t.Fatal(err)
	}
	if before.Version != 2 {
		t.Fatalf("lost reply did not commit: %+v", before)
	}
	r, err := invoke()
	if err != nil || !r.Replayed || r.Outcome != store.OutcomeOK || calls != 2 {
		t.Fatalf("retry: %+v %v calls=%d", r, err, calls)
	}
	after, err := s.ReadRecord(ctx, "req:aging")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("retry mutated request again")
	}
}
