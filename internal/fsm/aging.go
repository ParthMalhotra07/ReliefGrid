package fsm

import (
	"encoding/json"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// ageRequest uses only replicated state and integer proposer inputs. Never use
// UpdatedAt as the age origin: unrelated retries may have advanced it.
func (t *txn) ageRequest(c *store.AgeRequest) (store.Result, error) {
	if c.AgingIntervalMS <= 0 {
		return invalid(c.Key, "aging interval must be positive"), nil
	}
	rec, miss, err := t.loadRequest(c.Key)
	if err != nil || miss != nil {
		return deref(miss), err
	}
	if rec.Version != c.ExpectedVersion {
		return reject(store.OutcomeVersionConflict, c.Key, rec.Version, "expected version %d, have %d", c.ExpectedVersion, rec.Version), nil
	}
	req := *rec.Request
	if (req.Status != store.RequestPending && req.Status != store.RequestWaiting) || req.CurrentAttempt != nil || req.AssignedResourceID != "" {
		return reject(store.OutcomeDomainConflict, c.Key, rec.Version, "aging requires unassigned PENDING/WAITING without an intent"), nil
	}
	if req.Urgency < 1 || req.Urgency > 3 || req.EffectiveUrgency < 0 || req.EffectiveUrgency > 3 || req.CreatedAt <= 0 {
		return invalid(c.Key, "invalid persisted urgency or creation timestamp"), nil
	}
	current := max(req.Urgency, req.EffectiveUrgency)
	target := req.Urgency
	if t.at >= req.CreatedAt {
		// Both timestamps are positive, so subtraction cannot overflow. Cap the
		// interval count before addition, even for an extreme proposer timestamp.
		steps := min((t.at-req.CreatedAt)/c.AgingIntervalMS, int64(3-req.Urgency))
		target += int(steps)
	}
	if target <= current {
		return result(store.OutcomeOK, c.Key, rec.Version), nil
	}
	if reason, err := t.checkOutbox(c.Outbox); err != nil || reason != "" {
		return invalid(c.Key, "%s", reason), err
	}
	if !validAgingEvent(c.Outbox, rec, t.at, target) {
		return invalid(c.Key, "aging requires one matching effective-priority wake event"), nil
	}
	req.EffectiveUrgency = target
	req.NextAttemptAt, req.UpdatedAt = t.at, t.at
	rec.Request, rec.Version = &req, rec.Version+1
	return t.commit(rec, c.Outbox)
}

// Validate the semantic envelope as well as the server-filled outbox columns:
// arbitrary event bytes must not advertise another request/version/priority.
func validAgingEvent(events []store.OutboxEvent, rec store.Record, at int64, priority int) bool {
	if len(events) != 1 {
		return false
	}
	ev := events[0]
	if ev.EventType != "request.retry" || ev.SchemaVersion != 1 || ev.Exchange != store.AgingWakeExchange || ev.RoutingKey != store.AgingWakeRoutingKey || ev.Published {
		return false
	}
	var envelope struct {
		SchemaVersion int    `json:"schemaVersion"`
		EventID       string `json:"eventId"`
		EntityID      string `json:"entityId"`
		EntityVersion uint64 `json:"entityVersion"`
		EventType     string `json:"eventType"`
		Payload       struct {
			RequestID          string `json:"requestId"`
			WorkflowGeneration uint64 `json:"workflowGeneration"`
			NextAttemptAt      int64  `json:"nextAttemptAt"`
			Urgency            int    `json:"urgency"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(ev.Payload, &envelope); err != nil {
		return false
	}
	p := envelope.Payload
	return envelope.SchemaVersion == 1 && envelope.EventID == ev.EventID && envelope.EntityID == rec.Request.ID && envelope.EntityVersion == rec.Version+1 && envelope.EventType == ev.EventType && p.RequestID == rec.Request.ID && p.WorkflowGeneration == rec.Request.WorkflowGeneration && p.NextAttemptAt == at && p.Urgency == priority
}
