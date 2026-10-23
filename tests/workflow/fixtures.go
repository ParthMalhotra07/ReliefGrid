// Package workflow contains data-only table fixtures for the mandatory adversarial
// workflow schedules defined in §27 of the ReliefGrid architecture plan.
//
// These fixtures are pure data: initial state, ordered interleaving steps, expected
// final state and the invariant each schedule protects.  No FSM or broker logic is
// imported here.  Phase-1 tests (A1.2, A2.x) will drive these fixtures against the
// real in-memory FSM once S1.2 lands.
//
// Every entry maps 1-to-1 with a named schedule in tests/workflow/schedules.md.
// Tags (SCH01_… – SCH17_…) are the canonical identifiers checked by verify-A0.2.sh.
package workflow

import "github.com/ParthMalhotra07/ReliefGrid/internal/store"

// StepKind classifies the type of operation in an interleaving step.
type StepKind string

const (
	StepCAS           StepKind = "CAS"           // Conditional atomic state transition
	StepTryClaim      StepKind = "TryClaim"      // Resource claim attempt
	StepCloseAttempt  StepKind = "CloseAttempt"  // Attempt closure / fence
	StepRetryWorkflow StepKind = "RetryWorkflow" // Durable retry handoff
	StepPublish       StepKind = "Publish"       // Outbox / broker publish
	StepConsume       StepKind = "Consume"       // Broker message consume
	StepFault         StepKind = "Fault"         // Injected fault (crash/drop/outage)
	StepBarrier       StepKind = "Barrier"       // Synchronisation point (no state change)
	StepReconcile     StepKind = "Reconcile"     // Reconciler intervention
	StepReread        StepKind = "Reread"        // Authoritative re-read
)

// Step is one operation in the adversarial interleaving.
type Step struct {
	Actor   string // e.g. "Worker-A", "Reconciler", "Dispatcher", "FSM"
	Kind    StepKind
	Details string // human description matching schedules.md
}

// EntityState captures the expected durable state of a request or resource entity.
type EntityState struct {
	Entity      string               // "R" or "Res" (or named variant like "Res2")
	ReqStatus   store.RequestStatus  // non-empty for request entities
	ResStatus   store.ResourceStatus // non-empty for resource entities
	ActiveClaim string               // attempt ID, or "" if none
	Version     int                  // expected committed version
}

// Invariant references the AGENTS.md invariant number(s) and §17–19 rules this schedule tests.
type Invariant string

// ScheduleFixture is the complete data fixture for one mandatory adversarial schedule.
type ScheduleFixture struct {
	Tag          string        // canonical tag; checked by verify-A0.2.sh
	Name         string        // short human name
	Source       string        // §27 verbatim text that this schedule satisfies
	InitialState []EntityState // durable state before any step
	Steps        []Step        // ordered interleaving (including faults and barriers)
	FinalState   []EntityState // expected durable state after all steps settle
	Protects     Invariant     // invariant(s) this schedule validates
}

// Schedules is the authoritative list of all §27 mandatory adversarial schedules.
// verify-A0.2.sh checks that each Tag appears exactly once in this file.
var Schedules = []ScheduleFixture{

	// ── SCH01 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH01_BothCASQueuedBeforeEitherApplies",
		Name:   "Both CAS proposals queued before either applies",
		Source: "both CAS proposals queued before either applies",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Worker-A", Kind: StepCAS, Details: "PENDING→MATCHING(attemptA) v1→v2"},
			{Actor: "Worker-B", Kind: StepCAS, Details: "PENDING→MATCHING(attemptB) v1→v2"},
			{Actor: "FSM", Kind: StepBarrier, Details: "both CAS commands queued in log before either applies"},
			{Actor: "FSM", Kind: StepCAS, Details: "Worker-A CAS applies first; R=MATCHING(attemptA) v2"},
			{Actor: "FSM", Kind: StepCAS, Details: "Worker-B CAS rejected (version mismatch); VERSION_CONFLICT; no resource closure"},
			{Actor: "Worker-A", Kind: StepTryClaim, Details: "TryClaim(Res, attemptA) → ASSIGNED v2"},
			{Actor: "Worker-A", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→MATCHED; match.completed outbox"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Protects: "I1/§17: serial FSM apply; exactly one CAS wins; no double-claim",
	},

	// ── SCH02 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH02_DroppedResponseAfterCommit",
		Name:   "Dropped response after commit",
		Source: "dropped response after commit",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Worker", Kind: StepCAS, Details: "PENDING→MATCHING(attemptA) operationId=op-1; commit succeeds"},
			{Actor: "Network", Kind: StepFault, Details: "response dropped; Worker never receives reply"},
			{Actor: "Worker", Kind: StepReread, Details: "treats missing reply as unknown outcome (I5); retains intent"},
			{Actor: "Worker", Kind: StepCAS, Details: "retry operationId=op-1; FSM returns cached OK (deduped)"},
			{Actor: "Worker", Kind: StepTryClaim, Details: "TryClaim(Res, attemptA) → ASSIGNED v2"},
			{Actor: "Worker", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→MATCHED"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Protects: "I5/§18: unknown outcome ≠ rejection; retry same operationId returns original result",
	},

	// ── SCH03 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH03_CrashBeforeMatchingStep",
		Name:   "Crash before matching step",
		Source: "crash before/after every matching step",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Worker-A", Kind: StepReread, Details: "reads R=PENDING"},
			{Actor: "Worker-A", Kind: StepFault, Details: "crash before CAS PENDING→MATCHING; no intent written"},
			{Actor: "Reconciler", Kind: StepReconcile, Details: "detects R still PENDING; no orphan; no-op"},
			{Actor: "Worker-B", Kind: StepCAS, Details: "PENDING→MATCHING(attemptB); R v1→v2"},
			{Actor: "Worker-B", Kind: StepTryClaim, Details: "TryClaim(Res, attemptB) → ASSIGNED v2"},
			{Actor: "Worker-B", Kind: StepCAS, Details: "MATCHING(v2,attemptB)→MATCHED"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptB", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptB", Version: 2},
		},
		Protects: "§17/I3: crash before intent leaves no orphan claim; recovery is transparent",
	},

	// ── SCH04 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH04_CrashAfterMatchingStep",
		Name:   "Crash after matching step",
		Source: "crash before/after every matching step",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Worker-A", Kind: StepCAS, Details: "PENDING→MATCHING(attemptA); R v1→v2"},
			{Actor: "Worker-A", Kind: StepFault, Details: "crash immediately after CAS commit; TryClaim never ran"},
			{Actor: "Reconciler", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→RETRYING; confirmation fenced; R v2→v3"},
			{Actor: "Reconciler", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA) → OK; closure committed; no claim, so Res remains v1"},
			{Actor: "Reconciler", Kind: StepCAS, Details: "RETRYING(v3,attemptA)→WAITING; nextAttemptAt and retry outbox committed; R v3→v4"},
			{Actor: "Worker-B", Kind: StepCAS, Details: "WAITING(v4)→MATCHING(attemptB); R v4→v5"},
			{Actor: "Worker-B", Kind: StepTryClaim, Details: "TryClaim(Res, attemptB) → ASSIGNED v2"},
			{Actor: "Worker-B", Kind: StepCAS, Details: "MATCHING(v5,attemptB)→MATCHED; R v5→v6"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptB", Version: 6},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptB", Version: 2},
		},
		Protects: "I3/§17: MATCHING intent is durable; reconciler resumes without losing the workflow",
	},

	// ── SCH05 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH05_CancelBeforeTryClaim",
		Name:   "Cancellation before TryClaim",
		Source: "cancellation before/after TryClaim",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Worker-A", Kind: StepCAS, Details: "PENDING→MATCHING(attemptA); R v1→v2"},
			{Actor: "Dispatcher", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→CANCELLING; intent retained; v2→v3"},
			{Actor: "Reconciler", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA); no activeClaim; closure recorded; Res remains v1"},
			{Actor: "Worker-A-delayed", Kind: StepTryClaim, Details: "TryClaim(Res, attemptA); committed closure exists → CLOSED"},
			{Actor: "Reconciler", Kind: StepCAS, Details: "CANCELLING(v3)→CANCELLED; request.cancelled event"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestCancelled, Version: 4},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Protects: "§19/I3: cancellation fence stops late TryClaim; Res never gets orphan claim",
	},

	// ── SCH06 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH06_CancelAfterTryClaim",
		Name:   "Cancellation after TryClaim",
		Source: "cancellation before/after TryClaim",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatching, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Steps: []Step{
			{Actor: "Dispatcher", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→CANCELLING; intent+claim retained; v2→v3"},
			{Actor: "Reconciler", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA); release activeClaim; Res→AVAILABLE v3"},
			{Actor: "Reconciler", Kind: StepCAS, Details: "CANCELLING(v3)→CANCELLED; request.cancelled event"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestCancelled, Version: 4},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 3},
		},
		Protects: "§19/I3: CloseAttempt is the only correct release path; no orphan claim after CANCELLED",
	},

	// ── SCH07 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH07_StaleTryClaimAfterCloseAttempt",
		Name:   "Stale TryClaim after CloseAttempt",
		Source: "stale TryClaim after CloseAttempt",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatching, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Worker-A", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→RETRYING; confirmation fenced; R v2→v3"},
			{Actor: "Worker-A", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA); closure recorded; no active claim, so Res remains v1"},
			{Actor: "Worker-A-delayed", Kind: StepTryClaim, Details: "stale TryClaim(Res,attemptA); FSM: closure exists → CLOSED; no claim"},
			{Actor: "Reconciler", Kind: StepCAS, Details: "RETRYING(v3,attemptA)→WAITING; retry outbox committed; R v3→v4"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestWaiting, Version: 4},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Protects: "I4/§17: historical operation result is not current ownership; closure stops delayed TryClaim",
	},

	// ── SCH08 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH08_ConfirmationRacingWithRetarget",
		Name:   "Confirmation racing with retargeting",
		Source: "confirmation racing with retargeting",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatching, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res2", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Worker-A", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→RETRYING; v2→v3"},
			{Actor: "Worker-B", Kind: StepCAS, Details: "confirm MATCHING(v2,attemptA)→MATCHED; version mismatch (R=RETRYING v3) → VERSION_CONFLICT"},
			{Actor: "Worker-A", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA); Res→AVAILABLE v3; closure recorded"},
			{Actor: "Worker-A", Kind: StepCAS, Details: "RETRYING(v3,attemptA)→WAITING; retry outbox committed; R v3→v4"},
			{Actor: "Worker-A", Kind: StepCAS, Details: "WAITING(v4)→MATCHING(attemptB,Res2); R v4→v5"},
			{Actor: "Worker-A", Kind: StepTryClaim, Details: "TryClaim(Res2,attemptB) → ASSIGNED v2"},
			{Actor: "Worker-A", Kind: StepCAS, Details: "MATCHING(v5,attemptB)→MATCHED; R v5→v6"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptB", Version: 6},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 3},
			{Entity: "Res2", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptB", Version: 2},
		},
		Protects: "§17/§18/I3: RETRYING fences confirmation; exact CAS ensures only one path succeeds",
	},

	// ── SCH09 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH09_StaleOperationResultReplayAfterClose",
		Name:   "Stale successful operation-result replay after attempt closed",
		Source: "stale successful operation-result replay after an attempt was closed",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatching, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Steps: []Step{
			{Actor: "Dispatcher", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→CANCELLING; R v2→v3"},
			{Actor: "Reconciler", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA)→AVAILABLE v3; closure recorded"},
			{Actor: "Reconciler", Kind: StepCAS, Details: "CANCELLING(v3)→CANCELLED v4; request.cancelled event committed"},
			{Actor: "Worker-delayed", Kind: StepTryClaim, Details: "retry original TryClaim operationId; dedup returns historical SUCCESS; Res remains AVAILABLE v3"},
			{Actor: "Worker-delayed", Kind: StepReread, Details: "re-reads R=CANCELLED (terminal); historical result is not ownership"},
			{Actor: "Worker-delayed", Kind: StepBarrier, Details: "no fresh TryClaim or confirmation issued; obsolete delivery acked"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestCancelled, Version: 4},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 3},
		},
		Protects: "I4/§18: historical SUCCESS for closed attempt is not current ownership; consumers re-read state",
	},

	// ── SCH10 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH10_DuplicateTerminalEvents",
		Name:   "Duplicate terminal events",
		Source: "duplicate terminal events",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Steps: []Step{
			{Actor: "Worker", Kind: StepCAS, Details: "MATCHED(v3)→FULFILLING v4"},
			{Actor: "Worker", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA,AVAILABLE) → Res:AVAILABLE v3"},
			{Actor: "Worker", Kind: StepCAS, Details: "FULFILLING(v4)→FULFILLED v5; operationId=op-ff1; event committed"},
			{Actor: "Relay", Kind: StepPublish, Details: "request.fulfilled delivered to consumer (first)"},
			{Actor: "Consumer", Kind: StepConsume, Details: "processes event 1; R=FULFILLED; acks"},
			{Actor: "Relay", Kind: StepPublish, Details: "duplicate delivery of same event"},
			{Actor: "Consumer", Kind: StepConsume, Details: "operationId=op-ff1 already committed; idempotent no-op; acks"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestFulfilled, Version: 5},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 3},
		},
		Protects: "I6/§18: operationId deduplication; duplicate terminal event is a harmless no-op",
	},

	// ── SCH11 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH11_RepeatedCancelFulfillCalls",
		Name:   "Repeated cancel/fulfill calls",
		Source: "repeated cancel/fulfill calls",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Steps: []Step{
			{Actor: "Dispatcher", Kind: StepCAS, Details: "MATCHED(v3)→CANCELLING v4"},
			{Actor: "Reconciler", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA) → Res:AVAILABLE v3"},
			{Actor: "Reconciler", Kind: StepCAS, Details: "CANCELLING(v4)→CANCELLED v5"},
			{Actor: "Dispatcher", Kind: StepCAS, Details: "repeated CANCEL call; R=CANCELLED(terminal); returns existing result (200), no write"},
			{Actor: "Dispatcher", Kind: StepCAS, Details: "FULFILL call on CANCELLED R; returns 409"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestCancelled, Version: 5},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 3},
		},
		Protects: "§19/I3: repeated cancellation returns its terminal result; incompatible fulfillment returns 409; no duplicate release",
	},

	// ── SCH12 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH12_RestartDuringSnapshotCreation",
		Name:   "Restart during snapshot creation",
		Source: "restart during snapshot creation/restore",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatching, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Steps: []Step{
			{Actor: "Leader", Kind: StepBarrier, Details: "initiates FSM snapshot (§16)"},
			{Actor: "StorageNode", Kind: StepFault, Details: "crashes mid-snapshot; snapshot file incomplete"},
			{Actor: "StorageNode", Kind: StepBarrier, Details: "restarts; detects incomplete snapshot; discards it"},
			{Actor: "StorageNode", Kind: StepBarrier, Details: "replays Raft log from last committed checkpoint"},
			{Actor: "Worker", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→MATCHED; outbox event written"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Protects: "§16/I2: incomplete snapshot discarded; log replay restores committed state deterministically",
	},

	// ── SCH13 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH13_RestartDuringSnapshotRestore",
		Name:   "Restart during snapshot restore",
		Source: "restart during snapshot creation/restore",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatching, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Steps: []Step{
			{Actor: "NewNode", Kind: StepBarrier, Details: "begins installing snapshot from leader"},
			{Actor: "NewNode", Kind: StepFault, Details: "crashes during snapshot installation"},
			{Actor: "NewNode", Kind: StepBarrier, Details: "restarts; discards incomplete snapshot"},
			{Actor: "NewNode", Kind: StepBarrier, Details: "requests fresh snapshot from leader"},
			{Actor: "NewNode", Kind: StepBarrier, Details: "full snapshot installs; remaining log entries applied"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatching, ActiveClaim: "attemptA", Version: 2},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Protects: "§16/I2: node never applies partial snapshot; retries install; matches leader committed state",
	},

	// ── SCH14 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH14_CoordinatorRestartWithShardUnavailable",
		Name:   "Coordinator restart with one shard unavailable",
		Source: "coordinator restart with one shard unavailable",
		InitialState: []EntityState{
			// Shard 0: healthy; Shard 1: unavailable (no quorum)
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Coordinator", Kind: StepFault, Details: "pod restarts"},
			{Actor: "Coordinator", Kind: StepBarrier, Details: "reloads ring; same version+checksum; Shard 1 still unavailable"},
			{Actor: "Coordinator", Kind: StepBarrier, Details: "Shard 1 ops return explicit error; never silent empty result (I7)"},
			{Actor: "Worker", Kind: StepCAS, Details: "R on Shard 0: PENDING→MATCHING(attemptA); normal"},
			{Actor: "Worker", Kind: StepTryClaim, Details: "TryClaim(Res,attemptA) on Shard 0 → ASSIGNED"},
			{Actor: "Worker", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→MATCHED"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Protects: "I7/§10: coordinator restart preserves ring checksum; partial scan never returns empty; unavailable shard returns explicit error",
	},

	// ── SCH15 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH15_BrokerOutageDuringRetryScheduling",
		Name:   "Broker outage during retry scheduling",
		Source: "broker outage during retry scheduling",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestRetrying, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Steps: []Step{
			{Actor: "Relay", Kind: StepPublish, Details: "tries to publish pending outbox event to relief.requests; broker down"},
			{Actor: "Relay", Kind: StepFault, Details: "no routed confirm received; outbox row remains unpublished (I6)"},
			{Actor: "FSM", Kind: StepBarrier, Details: "R remains RETRYING; intent preserved (§18)"},
			{Actor: "Relay", Kind: StepBarrier, Details: "backs off; bounded exponential delay"},
			{Actor: "Broker", Kind: StepBarrier, Details: "recovers"},
			{Actor: "Relay", Kind: StepPublish, Details: "republishes with confirms+mandatory; routed confirm received"},
			{Actor: "Relay", Kind: StepBarrier, Details: "marks event published only after confirm"},
			{Actor: "Consumer", Kind: StepConsume, Details: "picks up retry event; re-reads R=RETRYING; resumes"},
			{Actor: "Worker", Kind: StepCloseAttempt, Details: "CloseAttempt(Res,attemptA) → OK; Res→AVAILABLE v3; closure committed"},
			{Actor: "Worker", Kind: StepCAS, Details: "RETRYING(v3,attemptA)→WAITING; retry outbox committed; R v3→v4"},
			{Actor: "Worker", Kind: StepCAS, Details: "WAITING(v4)→MATCHING(attemptB); R v4→v5"},
			{Actor: "Worker", Kind: StepTryClaim, Details: "TryClaim(Res,attemptB) → ASSIGNED v4"},
			{Actor: "Worker", Kind: StepCAS, Details: "MATCHING(v5,attemptB)→MATCHED; R v5→v6"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptB", Version: 6},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptB", Version: 4},
		},
		Protects: "I6/§18: no outbox completion before routed confirm; relay ensures at-least-once; intent preserved across outage",
	},

	// ── SCH16 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH16_PoisonMessages",
		Name:   "Poison messages",
		Source: "poison messages",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1}, // valid request
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
		},
		Steps: []Step{
			{Actor: "Consumer", Kind: StepConsume, Details: "receives malformed message (unparseable)"},
			{Actor: "Consumer", Kind: StepConsume, Details: "nack(requeue=false); never ack and never tight-loop requeue"},
			{Actor: "Broker", Kind: StepBarrier, Details: "configured DLX routes the rejected delivery to matching.dead (§21)"},
			{Actor: "Consumer", Kind: StepConsume, Details: "processes valid R message on matching.requests normally"},
			{Actor: "Worker", Kind: StepCAS, Details: "PENDING(v1)→MATCHING(attemptA); R v1→v2"},
			{Actor: "Worker", Kind: StepTryClaim, Details: "TryClaim(Res,attemptA) → ASSIGNED v2"},
			{Actor: "Worker", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→MATCHED; R v2→v3"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
			// malformed message: landed in matching.dead; retained for investigation
		},
		Protects: "§21/I6: poison messages don't block healthy traffic; DLQ retains them; no tight nack-requeue loop",
	},

	// ── SCH17 ──────────────────────────────────────────────────────────────────
	{
		Tag:    "SCH17_PartialCandidateScan",
		Name:   "Partial candidate scan",
		Source: "a partial candidate scan",
		InitialState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestPending, Version: 1},
			{Entity: "Res", ResStatus: store.ResourceAvailable, Version: 1},
			// Shard 1 holds some candidate resources but is unavailable
		},
		Steps: []Step{
			{Actor: "Matcher", Kind: StepBarrier, Details: "initiates authoritative candidate scan across all shards"},
			{Actor: "Shard1", Kind: StepFault, Details: "scan times out / returns error"},
			{Actor: "Matcher", Kind: StepBarrier, Details: "partial scan: NEVER treated as empty result (I7)"},
			{Actor: "Matcher", Kind: StepBarrier, Details: "records scan as incomplete; does NOT issue NO_CANDIDATE"},
			{Actor: "Matcher", Kind: StepBarrier, Details: "R remains PENDING v1; no durable handoff; delivery remains unacked while consumer backs off"},
			{Actor: "Shard1", Kind: StepBarrier, Details: "recovers; full scan completes; eligible candidates found"},
			{Actor: "Worker", Kind: StepCAS, Details: "PENDING→MATCHING(attemptA)"},
			{Actor: "Worker", Kind: StepTryClaim, Details: "TryClaim(Res,attemptA) → ASSIGNED"},
			{Actor: "Worker", Kind: StepCAS, Details: "MATCHING(v2,attemptA)→MATCHED"},
		},
		FinalState: []EntityState{
			{Entity: "R", ReqStatus: store.RequestMatched, ActiveClaim: "attemptA", Version: 3},
			{Entity: "Res", ResStatus: store.ResourceAssigned, ActiveClaim: "attemptA", Version: 2},
		},
		Protects: "I7/§17: partial scan is never empty result; NO_CANDIDATE only after complete authoritative scan",
	},
}
