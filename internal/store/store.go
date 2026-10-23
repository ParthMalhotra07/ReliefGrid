// Package store defines the authoritative storage surface: the Store
// interface, its commands, records, outcomes and errors. It has no
// dependencies so the API and matcher can use it without linking SQLite.
package store

import (
	"context"
	"encoding/json"
	"errors"
)

// Store is the authoritative storage surface the workflow, API and relay
// program against. The in-memory adapter (internal/fsm/memstore) implements
// it on the real FSM rules; the coordinator client implements it against the
// cluster. A returned Result is a committed, deterministic outcome. A returned
// error means the outcome is not known to be a rejection: retry with the same
// command (same OperationID and payload) or inspect with Operation.
//
// Retarget safety is the workflow's job. The FSM cannot see a closure on
// another shard, so it accepts MATCHING(a) -> MATCHING(b) or -> WAITING on
// the request's version alone. The workflow may retarget away from attempt a
// only after a committed CONFLICT_CLOSED, REJECTED_CLOSED or CLOSED result
// (or a closure from ReadClosure) for that exact resource and attempt. A
// resource merely observed as held by someone else, or an unknown outcome, is
// not a closure.
type Store interface {
	CreateEntity(ctx context.Context, cmd CreateEntity) (Result, error)
	CompareAndSet(ctx context.Context, cmd CompareAndSet) (Result, error)
	TransitionRequest(ctx context.Context, cmd TransitionRequest) (Result, error)
	RetryWorkflow(ctx context.Context, cmd RetryWorkflow) (Result, error)
	TransitionResource(ctx context.Context, cmd TransitionResource) (Result, error)
	TryClaim(ctx context.Context, cmd TryClaim) (Result, error)
	CloseAttempt(ctx context.Context, cmd CloseAttempt) (Result, error)
	MarkOutboxPublished(ctx context.Context, cmd MarkOutboxPublished) (Result, error)

	// ReadRecord is a strong read of one record. It returns ErrNotFound for an
	// absent key; a tombstone is returned with Deleted set.
	ReadRecord(ctx context.Context, key string) (Record, error)
	// ReadClosure returns the durable closure for an exact resource/attempt,
	// or ErrNotFound. Only a closure (or a CONFLICT_CLOSED, REJECTED_CLOSED or
	// CLOSED result) permits retargeting away from that attempt.
	ReadClosure(ctx context.Context, resourceKey, attemptID string) (Closure, error)
	// Scan reads every shard. A failed shard sets Partial; it is never
	// reported as an empty result.
	Scan(ctx context.Context, q ScanQuery) (ScanResult, error)
	// PendingOutbox pages unpublished events of one shard in commit order.
	PendingOutbox(ctx context.Context, shard, limit int) ([]OutboxEvent, error)
	// Operation returns the historical result stored for an operation ID on
	// the shard owning key. It is never proof of current ownership.
	Operation(ctx context.Context, key, operationID string) (Result, error)
	// Shards returns the number of shards, numbered 0..n-1.
	Shards() int
}

var (
	// ErrUnknownOutcome: the command may or may not have committed (timeout,
	// lost response, leader change). Never treat it as a rejection.
	ErrUnknownOutcome = errors.New("store: outcome unknown")
	// ErrNotFound: a read found no committed record, closure or operation.
	ErrNotFound = errors.New("store: not found")
)

// Key prefixes route domain records; any other key is a generic KV record.
const (
	RequestKeyPrefix  = "req:"
	ResourceKeyPrefix = "res:"
)

func RequestKey(id string) string  { return RequestKeyPrefix + id }
func ResourceKey(id string) string { return ResourceKeyPrefix + id }

type Kind string

const (
	KindRequest  Kind = "request"
	KindResource Kind = "resource"
	KindKV       Kind = "kv"
)

// ResourceType is shared by requests (what is needed) and resources (what is offered).
type ResourceType string

const (
	TypeWater          ResourceType = "WATER"
	TypeFood           ResourceType = "FOOD"
	TypeMedicine       ResourceType = "MEDICINE"
	TypeTransportation ResourceType = "TRANSPORTATION"
	TypeShelter        ResourceType = "SHELTER"
)

type RequestStatus string

const (
	RequestPending    RequestStatus = "PENDING"
	RequestWaiting    RequestStatus = "WAITING"
	RequestMatching   RequestStatus = "MATCHING"
	RequestMatched    RequestStatus = "MATCHED"
	RequestInProgress RequestStatus = "IN_PROGRESS"
	RequestRetrying   RequestStatus = "RETRYING"
	RequestCancelling RequestStatus = "CANCELLING"
	RequestFulfilling RequestStatus = "FULFILLING"
	RequestCancelled  RequestStatus = "CANCELLED"
	RequestFulfilled  RequestStatus = "FULFILLED"
)

type ResourceStatus string

const (
	ResourceAvailable ResourceStatus = "AVAILABLE"
	ResourceAssigned  ResourceStatus = "ASSIGNED"
	ResourceOffline   ResourceStatus = "OFFLINE"
	ResourceConsumed  ResourceStatus = "CONSUMED"
)

// Location is stored, never used for an FSM decision; distance and radius
// are the matcher's job.
type Location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Attempt is a request's recorded intent. All fields are immutable once
// recorded; a new candidate always gets a new AttemptID.
type Attempt struct {
	AttemptID   string `json:"attemptId"`
	ResourceID  string `json:"resourceId"`
	Fingerprint string `json:"fingerprint"` // matcher's hash over the immutable eligibility inputs
}

// Claim is a resource's active claim.
type Claim struct {
	RequestID   string `json:"requestId"`
	AttemptID   string `json:"attemptId"`
	Fingerprint string `json:"fingerprint"`
}

type Request struct {
	ID                 string          `json:"id"`
	Type               ResourceType    `json:"type"`
	Location           Location        `json:"location"`
	Geohash            string          `json:"geohash"`
	PeopleAffected     int             `json:"peopleAffected"`
	Urgency            int             `json:"urgency"`
	Details            json.RawMessage `json:"details,omitempty"`
	Status             RequestStatus   `json:"status"`
	WorkflowGeneration uint64          `json:"workflowGeneration"`
	CurrentAttempt     *Attempt        `json:"currentAttempt,omitempty"`
	AssignedResourceID string          `json:"assignedResourceId,omitempty"`
	Disposition        ResourceStatus  `json:"disposition,omitempty"` // cleanup disposition chosen for CANCELLING/FULFILLING
	NextAttemptAt      int64           `json:"nextAttemptAt,omitempty"`
	RetryCount         int             `json:"retryCount"`
	LastError          string          `json:"lastError,omitempty"`
	CreatedAt          int64           `json:"createdAt"`
	UpdatedAt          int64           `json:"updatedAt"`
}

type Resource struct {
	ID               string         `json:"id"`
	Type             ResourceType   `json:"type"`
	Name             string         `json:"name"`
	Location         Location       `json:"location"`
	Geohash          string         `json:"geohash"`
	AvailableFrom    int64          `json:"availableFrom"`  // unix ms, inclusive
	AvailableUntil   int64          `json:"availableUntil"` // unix ms, exclusive
	Status           ResourceStatus `json:"status"`
	Reusable         bool           `json:"reusable"`
	ActiveClaim      *Claim         `json:"activeClaim,omitempty"`
	LocationRevision uint64         `json:"locationRevision"`
	CreatedAt        int64          `json:"createdAt"`
	UpdatedAt        int64          `json:"updatedAt"`
}

// Record is one committed row. Exactly one of Request, Resource or Value is
// set, according to Kind. Version is a per-key revision that is never reused,
// even across a tombstone.
type Record struct {
	Key       string          `json:"key"`
	Kind      Kind            `json:"kind"`
	Version   uint64          `json:"version"`
	Deleted   bool            `json:"deleted,omitempty"`
	Request   *Request        `json:"request,omitempty"`
	Resource  *Resource       `json:"resource,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
	UpdatedAt int64           `json:"updatedAt"`
}

// Closure is the permanent fence for one resource/attempt.
type Closure struct {
	ResourceKey string         `json:"resourceKey"`
	AttemptID   string         `json:"attemptId"`
	RequestID   string         `json:"requestId"`
	Fingerprint string         `json:"fingerprint"`
	Reason      string         `json:"reason"`
	Disposition ResourceStatus `json:"disposition,omitempty"` // set by CloseAttempt; empty for a claim rejection
	ClosedAt    int64          `json:"closedAt"`
}

// OutboxEvent is written in the same transaction as the state change it
// describes. The proposer supplies EventID and payload; the FSM fills
// EntityKey, EntityVersion and CreatedAt, so a proposer leaves them empty.
type OutboxEvent struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	SchemaVersion int             `json:"schemaVersion"`
	Exchange      string          `json:"exchange"`
	RoutingKey    string          `json:"routingKey"`
	Payload       json.RawMessage `json:"payload"`
	EntityKey     string          `json:"entityKey,omitempty"`     // server-filled
	EntityVersion uint64          `json:"entityVersion,omitempty"` // server-filled
	CreatedAt     int64           `json:"createdAt,omitempty"`     // server-filled
	Published     bool            `json:"published"`
}

// Meta is carried by every mutation. OperationID and At are chosen by the
// proposer and must be identical on every retry: the FSM never reads a clock.
type Meta struct {
	OperationID string `json:"operationId"`
	At          int64  `json:"at"` // unix ms
}

// CreateEntity is create-only: an existing key, live or tombstoned, gives EXISTS.
type CreateEntity struct {
	Meta
	Key      string          `json:"key"`
	Request  *Request        `json:"request,omitempty"`
	Resource *Resource       `json:"resource,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
	Outbox   []OutboxEvent   `json:"outbox,omitempty"`
}

// CompareAndSet is the generic KV write. ExpectedVersion 0 means the key must
// be absent. Domain keys (req:, res:) are rejected with DOMAIN_CONFLICT.
type CompareAndSet struct {
	Meta
	Key             string          `json:"key"`
	ExpectedVersion uint64          `json:"expectedVersion"`
	Value           json.RawMessage `json:"value,omitempty"`
	Delete          bool            `json:"delete,omitempty"`
}

// TransitionRequest moves a request along the lifecycle in v3.1 §8 and writes
// its outbox rows atomically. Expected* must all match the current record.
type TransitionRequest struct {
	Meta
	Key               string         `json:"key"`
	ExpectedVersion   uint64         `json:"expectedVersion"`
	ExpectedStatus    RequestStatus  `json:"expectedStatus"`
	ExpectedAttemptID string         `json:"expectedAttemptId"` // "" when no intent is expected
	To                RequestStatus  `json:"to"`
	Attempt           *Attempt       `json:"attempt,omitempty"`     // required for To == MATCHING
	Disposition       ResourceStatus `json:"disposition,omitempty"` // required for CANCELLING/FULFILLING with an intent
	NextAttemptAt     int64          `json:"nextAttemptAt,omitempty"`
	LastError         string         `json:"lastError,omitempty"`
	Outbox            []OutboxEvent  `json:"outbox,omitempty"`
}

// RetryWorkflow records a due time and retry event without changing status or intent.
type RetryWorkflow struct {
	Meta
	Key             string        `json:"key"`
	ExpectedVersion uint64        `json:"expectedVersion"`
	NextAttemptAt   int64         `json:"nextAttemptAt"`
	LastError       string        `json:"lastError,omitempty"`
	Outbox          []OutboxEvent `json:"outbox,omitempty"`
}

// TransitionResource is the public resource update (v3.1 §19). Status, if
// set, may only be AVAILABLE or OFFLINE; ASSIGNED and CONSUMED are reached
// only through TryClaim and CloseAttempt. An ASSIGNED resource cannot change
// status at all (that would drop or orphan its claim), and no field of this
// command can touch activeClaim. Violations give DOMAIN_CONFLICT.
type TransitionResource struct {
	Meta
	Key             string         `json:"key"`
	ExpectedVersion uint64         `json:"expectedVersion"`
	Status          ResourceStatus `json:"status,omitempty"`       // "" leaves status unchanged
	Availability    *Window        `json:"availability,omitempty"` // nil leaves the window unchanged
	Outbox          []OutboxEvent  `json:"outbox,omitempty"`
}

// Window is a validated availability window in unix ms, From < Until.
type Window struct {
	From  int64 `json:"from"`
	Until int64 `json:"until"`
}

// TryClaim asks the resource shard to claim ResourceKey for one bound attempt.
// Eligibility is discrete: type equality and At within the availability window.
type TryClaim struct {
	Meta
	ResourceKey string       `json:"resourceKey"`
	RequestID   string       `json:"requestId"`
	Attempt     Attempt      `json:"attempt"`
	Type        ResourceType `json:"type"`
}

// CloseAttempt records the permanent fence for an attempt, whether or not its
// claim has arrived, and releases the resource to Disposition only if this
// exact attempt holds it. Disposition is AVAILABLE (reusable, still in its
// window), CONSUMED (consumable) or OFFLINE (window expired); anything else is
// INVALID. The first closure's disposition is kept on every retry.
type CloseAttempt struct {
	Meta
	ResourceKey string         `json:"resourceKey"`
	RequestID   string         `json:"requestId"`
	Attempt     Attempt        `json:"attempt"`
	Disposition ResourceStatus `json:"disposition"`
	Reason      string         `json:"reason"`
}

// MarkOutboxPublished marks up to 100 events of one shard as published.
type MarkOutboxPublished struct {
	Meta
	Shard    int      `json:"shard"`
	EventIDs []string `json:"eventIds"`
}

type Outcome string

const (
	OutcomeOK              Outcome = "OK"
	OutcomeVersionConflict Outcome = "VERSION_CONFLICT" // expected version/state/attempt did not match (412)
	OutcomeNotFound        Outcome = "NOT_FOUND"        // 404
	OutcomeExists          Outcome = "EXISTS"           // create on an existing key (412)
	OutcomeInvalid         Outcome = "INVALID"          // malformed command: missing field, bad enum, From >= Until (400)
	// OutcomeDomainConflict: well-formed but forbidden by a domain rule:
	// illegal transition, protected key, immutable-input mismatch, public
	// update of an assigned resource (409).
	OutcomeDomainConflict   Outcome = "DOMAIN_CONFLICT"
	OutcomeOperationIDReuse Outcome = "OPERATION_ID_REUSED" // same ID, different payload; never stored (409)

	ClaimSuccess        Outcome = "SUCCESS"
	ClaimConflictClosed Outcome = "CONFLICT_CLOSED"
	ClaimRejectedClosed Outcome = "REJECTED_CLOSED"
	ClaimClosed         Outcome = "CLOSED"
)

// Result is the committed outcome of one mutation. Every outcome except
// OPERATION_ID_REUSED is stored under its operation ID and returned verbatim,
// with Replayed set, when the same command is applied again.
type Result struct {
	Outcome      Outcome `json:"outcome"`
	OperationID  string  `json:"operationId"`
	Key          string  `json:"key"`
	Version      uint64  `json:"version"` // entity version after apply, or the current one on rejection
	Reason       string  `json:"reason,omitempty"`
	Replayed     bool    `json:"replayed,omitempty"`
	Shard        int     `json:"shard"`
	AppliedIndex uint64  `json:"appliedIndex"`
}

// ScanQuery selects records of one Kind. Empty filters match everything.
// Statuses holds RequestStatus or ResourceStatus values for that Kind. Each
// geohash prefix is the key range [p, p+"\xff"), so a matcher's geohash cover
// is a list of prefixes; a record matches if any prefix does. Results are
// ordered by key within each shard.
type ScanQuery struct {
	Kind            Kind         `json:"kind"`
	Type            ResourceType `json:"type,omitempty"`
	Statuses        []string     `json:"statuses,omitempty"`
	GeohashPrefixes []string     `json:"geohashPrefixes,omitempty"`
	Limit           int          `json:"limit,omitempty"` // per shard; 0 means no limit
}

type ScanResult struct {
	Records      []Record        `json:"records"`
	Partial      bool            `json:"partial"`
	FailedShards []int           `json:"failedShards,omitempty"`
	Positions    []ShardPosition `json:"positions"`
}

type ShardPosition struct {
	Shard        int    `json:"shard"`
	AppliedIndex uint64 `json:"appliedIndex"`
}
