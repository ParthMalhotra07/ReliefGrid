package fsm

import (
	"encoding/json"
	"testing"
)

func TestWithTraceparentReplacesOnlyTheValue(t *testing.T) {
	in := []byte(`{ "a" : 1, "traceparent" :  "old", "payload": {"traceparent": "nested"} }`)
	got, ok := withTraceparent(in, json.RawMessage(`"new"`))
	if !ok {
		t.Fatal("withTraceparent: not ok")
	}
	if want := `{ "a" : 1, "traceparent":"new", "payload": {"traceparent": "nested"} }`; string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	for _, bad := range []string{
		`[1]`, `"traceparent"`, `{"a":1}`, `{"traceparent":"x","traceparent":"y"}`,
		`{"traceparent":"x"} {}`, `{"traceparent":"x"`, `{"payload":{"traceparent":"x"}}`, ``,
	} {
		if _, ok := traceparentField([]byte(bad)); ok {
			t.Errorf("traceparentField(%q) ok, want not ok", bad)
		}
	}
}
