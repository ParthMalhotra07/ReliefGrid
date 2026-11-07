package storagenode

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestAgeRequestRoutePreservesCommand(t *testing.T) {
	rg := loadRing(t)
	key, id := keyOn(t, rg, testShard, "req:", "age")
	n := newFakeNode(t)
	body := map[string]any{"operationId": "age-route", "at": t0, "ringVersion": 1, "expectedVersion": 7, "agingIntervalMS": 1000, "outbox": []any{map[string]any{"eventId": "age-event", "eventType": "request.retry", "schemaVersion": 1, "exchange": "relief.requests", "routingKey": "request.retry", "payload": map[string]any{"urgency": 2}}}}
	answer := do(t, newTestServer(t, n).dataRoutes(), call{method: http.MethodPost, path: "/internal/requests/" + id + "/age", body: body})
	if answer.Code != http.StatusOK {
		t.Fatalf("age route status=%d: %s", answer.Code, answer.Body.String())
	}
	cmds := n.appliedCmds()
	if len(cmds) != 1 {
		t.Fatalf("proposals=%d, want one", len(cmds))
	}
	b, err := json.Marshal(cmds[0])
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	for field, want := range map[string]any{"key": key, "operationId": "age-route", "at": t0, "expectedVersion": 7, "agingIntervalMS": 1000, "outbox": body["outbox"]} {
		expected, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var a, b any
		_ = json.Unmarshal(got[field], &a)
		_ = json.Unmarshal(expected, &b)
		aa, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		if string(aa) != string(bb) {
			t.Fatalf("%s=%s, want %s", field, aa, bb)
		}
	}
}
