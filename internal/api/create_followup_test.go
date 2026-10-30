package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestAPICreationIntentCASLoserRereadsWinnerDeterministically(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed=%v", changed), func(t *testing.T) {
			ctx, s := realStore(t)
			firstCAS, secondCAS, firstDomain := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var writes atomic.Int32
			var domainSeen atomic.Bool
			faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
				if c.Method == "CompareAndSet" && strings.HasPrefix(c.Key, "api-create:") {
					n := writes.Add(1)
					return memstore.Fault{Before: func(ctx context.Context) error {
						if n == 1 {
							close(firstCAS)
							select {
							case <-secondCAS:
								return nil
							case <-ctx.Done():
								return ctx.Err()
							}
						}
						if n == 2 {
							close(secondCAS)
							select {
							case <-firstDomain:
								return nil
							case <-ctx.Done():
								return ctx.Err()
							}
						}
						return fmt.Errorf("unexpected intent write %d", n)
					}}
				}
				if c.Method == "CreateEntity" && domainSeen.CompareAndSwap(false, true) {
					return memstore.Fault{Before: func(context.Context) error { close(firstDomain); return nil }}
				}
				return memstore.Fault{}
			})
			h1, h2 := handler(t, faulted, now, "dispatcher-a"), handler(t, faulted, now.Add(time.Hour), "dispatcher-a")
			key := fixtureKey("forced-intent-cas-race")
			first, second := make(chan *httptest.ResponseRecorder, 1), make(chan *httptest.ResponseRecorder, 1)
			go func() { first <- call(ctx, h1, "POST", "/api/v1/requests", requestBody, key) }()
			select {
			case <-firstCAS:
			case <-ctx.Done():
				t.Fatal("first create never reached intent CAS")
			}
			body := requestBody
			if changed {
				body = strings.Replace(body, `"peopleAffected":2`, `"peopleAffected":3`, 1)
			}
			go func() { second <- call(ctx, h2, "POST", "/api/v1/requests", body, key) }()
			var a, b *httptest.ResponseRecorder
			select {
			case a = <-first:
			case <-ctx.Done():
				t.Fatal("first create did not complete")
			}
			select {
			case b = <-second:
			case <-ctx.Done():
				t.Fatal("second create did not complete")
			}
			original := object(t, a, 201)
			if changed {
				expectError(t, b, 409, "CONFLICT_IDEMPOTENCY_KEY", false, false)
			} else {
				object(t, b, 201)
				if a.Body.String() != b.Body.String() || a.Header().Get("Location") != b.Header().Get("Location") {
					t.Fatal("CAS loser did not replay the winner's original response")
				}
			}
			if writes.Load() != 2 {
				t.Fatalf("intent CAS calls=%d; want both creates forced through NOT_FOUND", writes.Load())
			}
			page, err := s.Scan(ctx, store.ScanQuery{Kind: store.KindRequest, Limit: 100})
			if err != nil || page.Partial || len(page.Records) != 1 || page.Records[0].Request.ID != original["requestId"] {
				t.Fatalf("race created inconsistent entities: %+v %v", page, err)
			}
		})
	}
}

func TestAPICreateIdentityConflictsOnlyUseIdempotencyCodeWithAKey(t *testing.T) {
	for _, endpoint := range []string{"requests", "resources"} {
		for _, keyed := range []bool{false, true} {
			for _, reuse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/keyed=%v/reuse=%v", endpoint, keyed, reuse), func(t *testing.T) {
					ctx, s := realStore(t)
					faulted := memstore.WithFaults(s, func(c memstore.Call) memstore.Fault {
						if c.Method != "CreateEntity" {
							return memstore.Fault{}
						}
						return memstore.Fault{Before: func(ctx context.Context) error {
							if reuse {
								r, err := s.CompareAndSet(ctx, store.CompareAndSet{Meta: store.Meta{OperationID: c.OperationID, At: now.UnixMilli()}, Key: c.Key, Value: json.RawMessage(`1`)})
								if err == nil && r.Outcome != store.OutcomeDomainConflict {
									return fmt.Errorf("operation collision setup: %+v", r)
								}
								return err
							}
							cmd := store.CreateEntity{Meta: store.Meta{OperationID: fixtureKey("external-create"), At: now.UnixMilli()}, Key: c.Key}
							if endpoint == "requests" {
								cmd.Request = &store.Request{ID: strings.TrimPrefix(c.Key, store.RequestKeyPrefix), Type: store.TypeWater, PeopleAffected: 1, Urgency: 1, Geohash: "s000000"}
							} else {
								cmd.Resource = &store.Resource{ID: strings.TrimPrefix(c.Key, store.ResourceKeyPrefix), Type: store.TypeWater, Name: "external collision", Geohash: "s000000", AvailableFrom: now.Add(-time.Hour).UnixMilli(), AvailableUntil: now.Add(time.Hour).UnixMilli(), Status: store.ResourceAvailable, Reusable: true, LocationRevision: 1}
							}
							r, err := s.CreateEntity(ctx, cmd)
							if err == nil && r.Outcome != store.OutcomeOK {
								return fmt.Errorf("entity collision setup: %+v", r)
							}
							return err
						}}
					})
					key := ""
					if keyed {
						key = fixtureKey("collision")
					}
					body := requestBody
					if endpoint == "resources" {
						body = resourceBody
					}
					want := "DOMAIN_CONFLICT"
					if reuse {
						want = "OPERATION_ID_REUSED"
					}
					if keyed {
						want = "CONFLICT_IDEMPOTENCY_KEY"
					}
					expectError(t, call(ctx, handler(t, faulted, now, "dispatcher-a"), "POST", "/api/v1/"+endpoint, body, key), 409, want, false, false)
				})
			}
		}
	}
}
