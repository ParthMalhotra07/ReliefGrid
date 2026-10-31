package api_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestAPIMutationErrorsPreserveKnownRefusals(t *testing.T) {
	for _, tc := range []struct {
		name               string
		err                error
		status             int
		code               string
		retryable, unknown bool
	}{
		{"invalid before proposal", &coordclient.Error{Status: 400, Code: internalapi.INVALID}, 400, "INVALID", false, false},
		{"local invalid not sent", &coordclient.Error{Code: internalapi.INVALID}, 400, "INVALID", false, false},
		{"not sent", &coordclient.Error{Message: "not sent: dial refused", Retryable: true}, 503, "STORAGE_UNAVAILABLE", true, false},
		{"domain refusal", &coordclient.Error{Status: 409, Code: internalapi.DOMAINCONFLICT}, 409, "DOMAIN_CONFLICT", false, false},
		{"version refusal", &coordclient.Error{Status: 412, Code: internalapi.VERSIONCONFLICT}, 409, "VERSION_CONFLICT", false, false},
		{"operation reuse refusal", &coordclient.Error{Status: 409, Code: internalapi.OPERATIONIDREUSED}, 409, "OPERATION_ID_REUSED", false, false},
		{"unclassified bad upstream route", &coordclient.Error{Status: 404, Message: "bad route"}, 503, "STORAGE_UNAVAILABLE", false, false},
		{"no answer after sending (status zero)", &coordclient.Error{Code: internalapi.OUTCOMEUNKNOWN, Retryable: true, OutcomeUnknown: true}, 503, "OUTCOME_UNKNOWN", true, true},
		{"lost committed response", &coordclient.Error{Status: 503, Code: internalapi.OUTCOMEUNKNOWN, Retryable: true, OutcomeUnknown: true}, 503, "OUTCOME_UNKNOWN", true, true},
		{"unclassified mutation transport", errors.New("response lost after write"), 503, "OUTCOME_UNKNOWN", true, true},
	} {
		for _, method := range []string{"CreateEntity", "CompareAndSet"} {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				ctx, s := realStore(t)
				faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
					if c.Method == method {
						return memstore.Fault{Before: func(context.Context) error { return fmt.Errorf("wrapped mutation: %w", tc.err) }}
					}
					return memstore.Fault{}
				})
				key := ""
				if method == "CompareAndSet" {
					key = fixtureKey("known-refusal")
				}
				w := call(ctx, handler(t, faulted, now, "dispatcher-a"), "POST", "/api/v1/requests", requestBody, key)
				expectError(t, w, tc.status, tc.code, tc.retryable, tc.unknown)
				if got := w.Header().Get("Retry-After"); (got != "") != tc.retryable {
					t.Errorf("Retry-After=%q retryable=%v", got, tc.retryable)
				}
				page, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindRequest, Limit: 10})
				if err != nil || page.Partial || len(page.Records) != 0 {
					t.Fatalf("refused fixture wrote a request: %+v %v", page, err)
				}
			})
		}
	}
}
