package mq_test

import (
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
)

func TestEventPriorityPreservesPublicAndRetryUrgency(t *testing.T) {
	for _, tc := range []struct {
		body     string
		priority uint8
		bad      bool
	}{
		{`{"payload":{"urgency":"NORMAL"}}`, 1, false},
		{`{"payload":{"urgency":"HIGH"}}`, 2, false},
		{`{"payload":{"urgency":"CRITICAL"}}`, 3, false},
		{`{"payload":{"urgency":1}}`, 1, false},
		{`{"payload":{"urgency":2}}`, 2, false},
		{`{"payload":{"urgency":3}}`, 3, false},
		{`{"payload":{}}`, 1, false},
		{`{"payload":{"urgency":0}}`, 0, true},
		{`{"payload":{"urgency":4}}`, 0, true},
		{`{"payload":{"urgency":2.5}}`, 0, true},
		{`{"payload":{"urgency":"urgent"}}`, 0, true},
		{`{"payload":{"urgency":null}}`, 0, true},
		{`not-json`, 0, true},
	} {
		t.Run(tc.body, func(t *testing.T) {
			priority, err := mq.EventPriority([]byte(tc.body))
			if priority != tc.priority || (err != nil) != tc.bad {
				t.Fatalf("priority=%d error=%v", priority, err)
			}
		})
	}
}
