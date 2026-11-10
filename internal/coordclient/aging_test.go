package coordclient_test

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Reflection keeps the red phase executable before the additive command type
// exists. The real method must also be part of the Store interface.
func TestAgeRequestClientSurfaceAndWire(t *testing.T) {
	surface := reflect.TypeOf((*store.Store)(nil)).Elem()
	if _, ok := surface.MethodByName("AgeRequest"); !ok {
		t.Fatal("Store is missing AgeRequest")
	}
	f := newFake(t)
	c := newClient(t, f)
	method := reflect.ValueOf(c).MethodByName("AgeRequest")
	if !method.IsValid() {
		t.Fatal("coordinator client is missing AgeRequest")
	}
	typ := method.Type()
	if typ.NumIn() != 2 || typ.NumOut() != 2 {
		t.Fatalf("unexpected AgeRequest signature: %s", typ)
	}
	cmd := reflect.New(typ.In(1))
	wire := `{"operationId":"age-client","at":1792800000000,"key":"req:aging","expectedVersion":7,"agingIntervalMS":1000,"outbox":[{"eventId":"age-event","eventType":"request.retry","schemaVersion":1,"exchange":"relief.requests","routingKey":"request.retry","payload":{"urgency":2},"published":false}]}`
	if err := json.Unmarshal([]byte(wire), cmd.Interface()); err != nil {
		t.Fatal(err)
	}
	result := method.Call([]reflect.Value{reflect.ValueOf(testCtx(t)), cmd.Elem()})
	if !result[1].IsNil() {
		t.Fatalf("AgeRequest: %v", result[1].Interface())
	}
	reqs := f.requests()
	if len(reqs) != 1 {
		t.Fatalf("requests=%d, want one", len(reqs))
	}
	if reqs[0].method != http.MethodPost || reqs[0].path != "/internal/requests/aging/age" {
		t.Fatalf("wrong route: %+v", reqs[0])
	}
	var got, want map[string]json.RawMessage
	if err := json.Unmarshal(reqs[0].body, &got); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal([]byte(wire), &want)
	if _, ok := got["key"]; ok {
		t.Fatal("key must come from route")
	}
	if string(got["ringVersion"]) != "1" {
		t.Fatalf("ringVersion=%s", got["ringVersion"])
	}
	for _, field := range []string{"operationId", "at", "expectedVersion", "agingIntervalMS", "outbox"} {
		var a, b any
		_ = json.Unmarshal(got[field], &a)
		_ = json.Unmarshal(want[field], &b)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("%s changed: %s want %s", field, got[field], want[field])
		}
	}
}
