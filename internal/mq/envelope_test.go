package mq_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestConsumerRejectsAmbiguousEnvelopeAndRetryProperties(t *testing.T) {
	ctx, s := realStore(t)
	created := eventDelivery(t, nil).Body
	var retry map[string]any
	if err := json.Unmarshal(created, &retry); err != nil {
		t.Fatal(err)
	}
	retry["eventType"] = "request.retry"
	retry["payload"] = map[string]any{"requestId": requestID, "workflowGeneration": 0, "nextAttemptAt": 5000, "urgency": 2}
	b, err := json.Marshal(retry)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, body string }{
		{"retry envelope property case", strings.Replace(string(b), `"entityId"`, `"EntityId"`, 1)},
		{"retry payload property case", strings.Replace(string(b), `"requestId"`, `"RequestId"`, 1)},
		{"duplicate identity", strings.TrimSuffix(string(b), "}") + `,"entityId":"` + requestID + `"}`},
		{"trailing document", string(created) + `{}`},
		{"unknown envelope property", strings.TrimSuffix(string(b), "}") + `,"unknown":true}`},
		{"invalid created payload", strings.Replace(string(created), `"MEDICINE"`, `"UNKNOWN"`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ack := &ackLog{t: t}
			c := mq.Consumer{Store: s, Handle: func(context.Context, store.Record) error {
				t.Fatal("malformed envelope reached durable handler")
				return nil
			}}
			d := eventDelivery(t, ack)
			d.Body = []byte(tc.body)
			if err := c.Process(ctx, d); err != nil {
				t.Fatal(err)
			}
			if ack.rejects != 1 || ack.acks != 0 || ack.nacks != 0 {
				t.Fatalf("malformed delivery disposition: %+v", ack)
			}
		})
	}
}
