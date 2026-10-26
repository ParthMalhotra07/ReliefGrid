package workflow

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// These checks validate the fixtures' protocol claims, independently of the
// Markdown parity check. Executing fault schedules remains an A2.3 task.
func TestScheduleStateInvariants(t *testing.T) {
	for _, fixture := range Schedules {
		t.Run(fixture.Tag, func(t *testing.T) {
			for name, states := range map[string][]EntityState{"initial": fixture.InitialState, "final": fixture.FinalState} {
				for _, state := range states {
					if state.Version < 1 {
						t.Errorf("%s %s: version must be positive", name, state.Entity)
					}
					switch state.ReqStatus {
					case store.RequestMatching, store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling:
						if state.ActiveClaim == "" {
							t.Errorf("%s %s: %s must preserve its attempt", name, state.Entity, state.ReqStatus)
						}
					case store.RequestMatched, store.RequestInProgress:
						claims := 0
						for _, resource := range states {
							if state.ActiveClaim != "" && resource.ResStatus == store.ResourceAssigned && resource.ActiveClaim == state.ActiveClaim {
								claims++
							}
						}
						if claims != 1 {
							t.Errorf("%s %s: %s requires one reciprocal claim; got %d", name, state.Entity, state.ReqStatus, claims)
						}
					case store.RequestPending, store.RequestWaiting, store.RequestCancelled, store.RequestFulfilled:
						if state.ActiveClaim != "" {
							t.Errorf("%s %s: %s cannot hold a live claim", name, state.Entity, state.ReqStatus)
						}
					}
					if state.ResStatus != "" && (state.ResStatus == store.ResourceAssigned) != (state.ActiveClaim != "") {
						t.Errorf("%s %s: resource status %s disagrees with active claim %q", name, state.Entity, state.ResStatus, state.ActiveClaim)
					}
				}
			}
		})
	}
}

var requestMove = regexp.MustCompile(`\b(PENDING|WAITING|MATCHING|MATCHED|IN_PROGRESS|RETRYING|CANCELLING|FULFILLING|CANCELLED|FULFILLED)(?:\([^)]*\))?\s*→\s*([A-Z_]+)`)
var explicitVersionMove = regexp.MustCompile(`v(\d+)\s*→\s*v(\d+)`)

func TestScheduleRequestTransitions(t *testing.T) {
	for _, fixture := range Schedules {
		t.Run(fixture.Tag, func(t *testing.T) {
			var status store.RequestStatus
			for _, state := range fixture.InitialState {
				if state.ReqStatus != "" {
					status = state.ReqStatus
				}
			}
			for index, step := range fixture.Steps {
				if step.Kind == StepCloseAttempt && (status == store.RequestMatching || status == store.RequestMatched || status == store.RequestInProgress) {
					t.Errorf("step %d: close in %s must first fence confirmation with a cleanup state", index+1, status)
				}
				for _, move := range requestMove.FindAllStringSubmatch(step.Details, -1) {
					if move[1] == "RETRYING" && move[2] != "WAITING" && move[2] != "CANCELLING" {
						t.Errorf("step %d: %s→%s skips the required WAITING transition after closure", index+1, move[1], move[2])
					}
					if step.Kind == StepCAS && !strings.Contains(step.Details, "version mismatch") && !strings.Contains(step.Details, "rejected") {
						status = store.RequestStatus(move[2])
					}
				}
				for _, move := range explicitVersionMove.FindAllStringSubmatch(step.Details, -1) {
					before, _ := strconv.Atoi(move[1])
					after, _ := strconv.Atoi(move[2])
					if after != before+1 {
						t.Errorf("step %d: committed version move %s must increment once", index+1, move[0])
					}
				}
				if strings.Contains(step.Details, "CAS rejected") && strings.Contains(step.Details, "CONFLICT_CLOSED") {
					t.Errorf("step %d: a rejected request CAS cannot create a resource attempt closure", index+1)
				}
			}
		})
	}
}
