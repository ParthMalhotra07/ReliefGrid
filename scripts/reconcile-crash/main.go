// Command reconcile-crash is a test-only matcher worker. It pauses after a real
// coordinator mutation commits so the verifier can kill its pod at that boundary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
	"github.com/ParthMalhotra07/ReliefGrid/internal/workflow"
	"github.com/google/uuid"
)

type checkpoint struct {
	Phase      string          `json:"phase"`
	Stage      string          `json:"stage"`
	RequestID  string          `json:"requestId"`
	ResourceID string          `json:"resourceId"`
	AttemptID  string          `json:"attemptId,omitempty"`
	Claim      *store.TryClaim `json:"claim,omitempty"`
}
type hookStore struct {
	store.Store
	root        context.Context
	state       checkpoint
	blockClaims bool
}

func (h *hookStore) pause(point string) {
	if point != h.state.Stage {
		return
	}
	h.state.Phase = "READY"
	_ = json.NewEncoder(os.Stdout).Encode(h.state)
	<-h.root.Done()
	os.Exit(75)
}
func (h *hookStore) TransitionRequest(ctx context.Context, c store.TransitionRequest) (store.Result, error) {
	r, err := h.Store.TransitionRequest(ctx, c)
	if err != nil || r.Outcome != store.OutcomeOK {
		return r, err
	}
	if c.Attempt != nil {
		h.state.AttemptID = c.Attempt.AttemptID
		h.state.ResourceID = c.Attempt.ResourceID
	}
	point := ""
	switch c.To {
	case store.RequestMatching:
		point = "intent"
	case store.RequestMatched:
		point = "confirm"
	case store.RequestRetrying:
		point = "retry-intent"
	case store.RequestCancelling:
		point = "cancel-intent"
	case store.RequestFulfilling:
		point = "fulfil-intent"
	case store.RequestCancelled:
		point = "cancel-terminal"
	case store.RequestFulfilled:
		point = "fulfil-terminal"
	}
	h.pause(point)
	return r, err
}
func (h *hookStore) TryClaim(ctx context.Context, c store.TryClaim) (store.Result, error) {
	h.state.AttemptID = c.Attempt.AttemptID
	h.state.ResourceID = c.Attempt.ResourceID
	copy := c
	h.state.Claim = &copy
	if h.blockClaims {
		return store.Result{}, store.ErrUnknownOutcome
	}
	r, err := h.Store.TryClaim(ctx, c)
	if err == nil && r.Outcome == store.ClaimSuccess {
		if h.state.Stage == "old-claim" {
			// Persist a newer future schedule without an event hint. Periodic
			// recovery must still fence this already-old immutable attempt.
			rec, readErr := h.ReadRecord(ctx, store.RequestKey(c.RequestID))
			if readErr != nil {
				return store.Result{}, readErr
			}
			at := time.Now().UnixMilli()
			cmd := store.RetryWorkflow{Meta: store.Meta{OperationID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(c.OperationID+"/future-old-intent")).String(), At: at}, Key: rec.Key, ExpectedVersion: rec.Version, NextAttemptAt: at + 120000, LastError: "RECOVERY_TEST_FUTURE"}
			result, retryErr := h.RetryWorkflow(ctx, cmd)
			if retryErr = must(result, retryErr); retryErr != nil {
				return store.Result{}, retryErr
			}
			h.pause("old-claim")
		} else {
			h.pause("claim")
		}
	}
	return r, err
}
func (h *hookStore) CloseAttempt(ctx context.Context, c store.CloseAttempt) (store.Result, error) {
	r, err := h.Store.CloseAttempt(ctx, c)
	if err == nil && r.Outcome == store.OutcomeOK {
		point := "cancel-close"
		if strings.HasPrefix(h.state.Stage, "fulfil-") {
			point = "fulfil-close"
		}
		if strings.HasPrefix(h.state.Stage, "retry-") {
			point = "retry-close"
		}
		h.pause(point)
	}
	return r, err
}
func must(r store.Result, err error) error {
	if err != nil {
		return err
	}
	if r.Outcome != store.OutcomeOK {
		return fmt.Errorf("fixture mutation rejected: %s %s", r.Outcome, r.Reason)
	}
	return nil
}
func coordinatorClient(ctx context.Context) (*coordclient.Client, error) {
	return coordclient.New(ctx, os.Getenv("COORDINATOR_URL"), coordclient.WithInternalAuthFile(os.Getenv("INTERNAL_AUTH_TOKEN_FILE")))
}

func run() error {
	deferID := flag.String("defer-request", "", "test-only future schedule without a delivery hint")
	probe := flag.String("probe", "", "saved committed-boundary JSON for a late-claim check")
	stage := flag.String("stage", "", "workflow commit boundary")
	seed := flag.String("run", "", "unique fixture run")
	latitude := flag.Float64("latitude", 0, "isolated fixture latitude")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := coordinatorClient(ctx)
	if err != nil {
		return err
	}
	if *deferID != "" {
		rec, err := client.ReadRecord(ctx, store.RequestKey(*deferID))
		if err != nil {
			return err
		}
		at := time.Now().UnixMilli()
		cmd := store.RetryWorkflow{Meta: store.Meta{OperationID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(rec.Key+fmt.Sprint(rec.Version)+"/future-fixture")).String(), At: at}, Key: rec.Key, ExpectedVersion: rec.Version, NextAttemptAt: at + 120000, LastError: "RECOVERY_TEST_FUTURE"}
		result, err := client.RetryWorkflow(ctx, cmd)
		if err := must(result, err); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"requestId": *deferID, "nextAttemptAt": cmd.NextAttemptAt, "version": result.Version})
	}
	if *probe != "" {
		return probeClosed(ctx, client, *probe)
	}
	if *stage == "" || *seed == "" {
		return fmt.Errorf("--stage and --run are required")
	}
	rid := uuid.NewSHA1(uuid.NameSpaceURL, []byte(*seed+"/"+*stage+"/resource")).String()
	qid := uuid.NewSHA1(uuid.NameSpaceURL, []byte(*seed+"/"+*stage+"/request")).String()
	clock := time.Now().UTC()
	if *stage == "old-claim" {
		clock = clock.Add(-time.Minute)
	}
	location := store.Location{Lat: *latitude, Lng: 45}
	hash, err := geo.Encode(location, 7)
	if err != nil {
		return err
	}
	reusable := !strings.HasPrefix(*stage, "fulfil-")
	resource := store.Resource{ID: rid, Type: store.TypeWater, Name: "Synthetic recovery fixture", Location: location, Geohash: hash, Status: store.ResourceAvailable, Reusable: reusable, AvailableFrom: clock.Add(-time.Hour).UnixMilli(), AvailableUntil: clock.Add(time.Hour).UnixMilli(), LocationRevision: 1}
	details, _ := json.Marshal("Synthetic recovery fixture")
	request := store.Request{ID: qid, Type: store.TypeWater, Location: location, Geohash: hash, PeopleAffected: 1, Urgency: 1, Details: details}
	meta := func(name string) store.Meta {
		return store.Meta{OperationID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(*seed+"/"+*stage+"/"+name)).String(), At: clock.UnixMilli()}
	}
	r, err := client.CreateEntity(ctx, store.CreateEntity{Meta: meta("resource"), Key: store.ResourceKey(rid), Resource: &resource})
	if err := must(r, err); err != nil {
		return err
	}
	r, err = client.CreateEntity(ctx, store.CreateEntity{Meta: meta("request"), Key: store.RequestKey(qid), Request: &request})
	if err := must(r, err); err != nil {
		return err
	}
	h := &hookStore{Store: client, root: ctx, state: checkpoint{Phase: "SEEDED", Stage: *stage, RequestID: qid, ResourceID: rid}, blockClaims: strings.HasPrefix(*stage, "retry-")}
	if err := json.NewEncoder(os.Stdout).Encode(h.state); err != nil {
		return err
	}
	h.pause("pending")
	matcher, err := workflow.New(workflow.Config{Store: h, Now: func() time.Time { return clock }, ScanLimit: 32})
	if err != nil {
		return err
	}
	_, matchErr := matcher.Match(ctx, qid)
	if strings.HasPrefix(*stage, "retry-") {
		if !errors.Is(matchErr, store.ErrUnknownOutcome) {
			return fmt.Errorf("expected unknown pre-claim boundary: %w", matchErr)
		}
		h.blockClaims = false
		_, err := matcher.Abandon(ctx, qid)
		if err != nil {
			return err
		}
	} else {
		if matchErr != nil {
			return matchErr
		}
		if strings.HasPrefix(*stage, "cancel-") || strings.HasPrefix(*stage, "fulfil-") {
			rec, err := client.ReadRecord(ctx, store.RequestKey(qid))
			if err != nil {
				return err
			}
			if rec.Request == nil || rec.Request.Status != store.RequestMatched || rec.Request.CurrentAttempt == nil {
				return fmt.Errorf("fixture not matched")
			}
			h.state.AttemptID = rec.Request.CurrentAttempt.AttemptID
			target, disposition := store.RequestCancelling, store.ResourceAvailable
			if strings.HasPrefix(*stage, "fulfil-") {
				target, disposition = store.RequestFulfilling, store.ResourceConsumed
			}
			r, err := h.TransitionRequest(ctx, store.TransitionRequest{Meta: meta("cleanup"), Key: rec.Key, ExpectedVersion: rec.Version, ExpectedStatus: rec.Request.Status, ExpectedAttemptID: h.state.AttemptID, To: target, Disposition: disposition})
			if err := must(r, err); err != nil {
				return err
			}
			if _, err := matcher.Cleanup(ctx, qid); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("target boundary %q was not reached", *stage)
}
func probeClosed(ctx context.Context, client store.Store, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var c checkpoint
	if err := json.Unmarshal(raw, &c); err != nil {
		return err
	}
	if c.Claim == nil {
		return fmt.Errorf("checkpoint has no immutable claim command")
	}
	before, err := client.ReadRecord(ctx, store.ResourceKey(c.ResourceID))
	if err != nil {
		return err
	}
	historical, err := client.Operation(ctx, store.ResourceKey(c.ResourceID), c.Claim.OperationID)
	history := "NOT_FOUND"
	if err == nil {
		history = string(historical.Outcome)
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	late := *c.Claim
	late.OperationID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(late.OperationID+"/after-closure-probe")).String()
	result, err := client.TryClaim(ctx, late)
	if err != nil {
		return err
	}
	if result.Outcome != store.ClaimClosed {
		return fmt.Errorf("late attempt got %s, want CLOSED", result.Outcome)
	}
	after, err := client.ReadRecord(ctx, store.ResourceKey(c.ResourceID))
	if err != nil {
		return err
	}
	if before.Resource == nil || after.Resource == nil || !reflect.DeepEqual(before.Resource.ActiveClaim, after.Resource.ActiveClaim) {
		return fmt.Errorf("closed old attempt changed current ownership")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": c.Stage, "lateOutcome": result.Outcome, "historicalOutcome": history, "currentOwnershipUnchanged": true})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
