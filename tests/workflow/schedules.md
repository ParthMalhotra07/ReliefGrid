# Adversarial Workflow Schedules — §27

Every schedule below comes from the mandatory list in §27 ("Test strategy and safety cases").
References: §17 (Matching with fast retarget), §18 (Ambiguous claims / retry states), §19 (Cancellation and completion).

Each schedule specifies:
- **Initial state** — the durable state of request R and resource Res before any step.
- **Step-by-step interleaving** — each operation by actor/worker, in order; barriers mark synchronisation points.
- **Expected final state** — request status + resource status after all operations settle.
- **Invariant protected** — the AGENTS.md invariant (I1–I8) or §17–19 rule this schedule tests.

The request's `activeClaim` column denotes its recorded attempt/assignment, not
a resource-side claim. Cleanup retains that attempt until closure commits.
Resources released to AVAILABLE below are reusable and inside their validated
availability window. Each committed request transition increments R once;
claim acquisition/release increments Res once, while fence-only closure does not.

---

## SCH01 — Both CAS proposals queued before either applies

**Source:** §27 "both CAS proposals queued before either applies"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | PENDING   | —           | 1       |
| Res     | AVAILABLE | —           | 1       |

### Interleaving
```
Worker-A reads R (v1=PENDING), selects Res, issues CAS PENDING→MATCHING(attemptA)
Worker-B reads R (v1=PENDING), selects Res, issues CAS PENDING→MATCHING(attemptB)
── BARRIER: both CAS commands are queued in the FSM log before either is applied ──
FSM applies Worker-A's CAS first  →  R: MATCHING(attemptA), v2
FSM applies Worker-B's CAS        →  version mismatch; CAS rejected; returns VERSION_CONFLICT
Worker-B never acquired an intent; no TryClaim or resource closure is issued
Worker-A receives OK; proceeds to TryClaim(Res, attemptA)
TryClaim commits Res.activeClaim = attemptA, Res.status = ASSIGNED, v2
Worker-A CAS R: MATCHING(v2,attemptA) → MATCHED; writes match.completed outbox event
```

### Expected final state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | MATCHED | attemptA    | 3       |
| Res     | ASSIGNED| attemptA    | 2       |

### Invariant protected
**I1 / §17:** Exactly one CAS succeeds for a given request version; no request ever holds two live claims simultaneously. The FSM applies CAS operations serially in log order; the second proposer gets a version mismatch, not a silent double-claim.

---

## SCH02 — Dropped response after commit

**Source:** §27 "dropped response after commit"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | PENDING   | —           | 1       |
| Res     | AVAILABLE | —           | 1       |

### Interleaving
```
Worker reads R, issues CAS PENDING→MATCHING(attemptA)  [operationId=op-1]
FSM commits R: MATCHING(attemptA), v2  →  response is committed
── FAULT: network drops the response; Worker never sees the reply ──
Worker treats missing reply as unknown outcome (I5)
Worker retries with same operationId=op-1  →  FSM returns cached deduplicated result: OK
Worker proceeds with confirmed attempt
Worker TryClaim(Res, attemptA); Res: ASSIGNED, v2
Worker CAS R: MATCHING(v2,attemptA) → MATCHED
```

### Expected final state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | MATCHED | attemptA    | 3       |
| Res     | ASSIGNED| attemptA    | 2       |

### Invariant protected
**I5 / §18:** Unknown outcome ≠ rejection. A dropped reply does not clear the intent. Retrying the same operationId returns the original committed result without creating a second claim.

---

## SCH03 — Crash before matching step

**Source:** §27 "crash before/after every matching step"

### Initial state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | PENDING | —           | 1       |
| Res     | AVAILABLE | —         | 1       |

### Interleaving
```
Worker reads R (PENDING)
── FAULT: Worker crashes before it issues CAS PENDING→MATCHING ──
R and Res remain unchanged (no intent was written)
Reconciler detects no progress; R is still PENDING; no cleanup needed
New worker picks up R; reads PENDING; selects Res; CAS PENDING→MATCHING(attemptB)
TryClaim(Res, attemptB) → ASSIGNED
CAS R: MATCHING(v2,attemptB) → MATCHED
```

### Expected final state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | MATCHED | attemptB    | 3       |
| Res     | ASSIGNED| attemptB    | 2       |

### Invariant protected
**§17 / I3:** No stale state is left when a crash precedes intent creation. The FSM state is unchanged; recovery is transparent; no orphan claim exists.

---

## SCH04 — Crash after matching step

**Source:** §27 "crash before/after every matching step"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | PENDING   | —           | 1       |
| Res     | AVAILABLE | —           | 1       |

### Interleaving
```
Worker-A CAS PENDING→MATCHING(attemptA)  →  R: MATCHING(attemptA), v2
── FAULT: Worker-A crashes immediately after the CAS commit ──
R is stuck in MATCHING; Res is still AVAILABLE (TryClaim never ran)
Reconciler observes MATCHING with no progress past threshold
Reconciler CAS R: MATCHING(v2,attemptA) → RETRYING(v3); confirmation is fenced
Reconciler issues CloseAttempt(Res, attemptA) → OK; closure commits; Res stays AVAILABLE v1
Reconciler CAS R: RETRYING(v3,attemptA) → WAITING(v4); retry schedule and outbox committed
New worker CAS R: WAITING(v4) → MATCHING(attemptB), v5
TryClaim(Res, attemptB) → ASSIGNED
CAS R: MATCHING(v5,attemptB) → MATCHED, v6
```

### Expected final state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | MATCHED | attemptB    | 6       |
| Res     | ASSIGNED| attemptB    | 2       |

### Invariant protected
**I3 / §17:** A crashed worker leaves a durable MATCHING intent that the reconciler can resume without losing the workflow.

---

## SCH05 — Cancellation before TryClaim

**Source:** §27 "cancellation before/after TryClaim"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | PENDING   | —           | 1       |
| Res     | AVAILABLE | —           | 1       |

### Interleaving
```
Worker-A CAS PENDING→MATCHING(attemptA)  →  R: MATCHING(attemptA), v2
── BARRIER: cancellation arrives before TryClaim is issued ──
Dispatcher CAS R: MATCHING(v2,attemptA) → CANCELLING(v3) (retains intent, disposition=AVAILABLE)
Reconciler sees CANCELLING; issues CloseAttempt(Res, attemptA) → closure recorded (no claim to release; Res remains v1)
Delayed Worker-A issues TryClaim(Res, attemptA)
FSM: checks the committed closure for attemptA → returns CLOSED
No claim is written to Res; Res remains AVAILABLE
Reconciler CAS R: CANCELLING → CANCELLED
```

### Expected final state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | CANCELLED | —           | 4       |
| Res     | AVAILABLE | —           | 1       |

### Invariant protected
**§19 / I3:** Cancellation before TryClaim results in no live claim; CloseAttempt creates the fence. A late TryClaim sees CLOSED and fails safely without assigning the resource.

---

## SCH06 — Cancellation after TryClaim

**Source:** §27 "cancellation before/after TryClaim"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | MATCHING  | attemptA    | 2       |
| Res     | ASSIGNED  | attemptA    | 2       |

### Interleaving
```
── Attempt is live; Res is already ASSIGNED ──
Dispatcher CAS R: MATCHING(v2,attemptA) → CANCELLING(v3) (retains intent+claim, disposition=AVAILABLE)
Reconciler/helper issues CloseAttempt(Res, attemptA)
FSM: activeClaim == attemptA → release claim; Res: AVAILABLE, v3; closure recorded
CAS R: CANCELLING → CANCELLED; writes request.cancelled outbox event
```

### Expected final state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | CANCELLED | —           | 4       |
| Res     | AVAILABLE | —           | 3       |

### Invariant protected
**§19 / I3:** Cancellation after TryClaim must go through CloseAttempt to release the resource; the resource is never left with an orphan active claim after a terminal transition.

---

## SCH07 — Stale TryClaim after CloseAttempt

**Source:** §27 "stale TryClaim after CloseAttempt"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | MATCHING  | attemptA    | 2       |
| Res     | AVAILABLE | —           | 1       |

### Interleaving
```
Worker-A CAS R: MATCHING(v2,attemptA) → RETRYING(v3); confirmation is fenced
Worker-A issues CloseAttempt(Res, attemptA) → OK; closure recorded (no activeClaim; Res remains v1)
── FAULT: Worker-A's TryClaim from a delayed/stale execution arrives ──
Delayed Worker-A issues TryClaim(Res, attemptA)
FSM: checks closure table for attemptA → CLOSED; returns CLOSED; no claim written
Res remains AVAILABLE (no orphan assignment)
Reconciler CAS R: RETRYING(v3,attemptA) → WAITING(v4); retry outbox committed
```

### Expected final state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | WAITING | —           | 4       |
| Res     | AVAILABLE | —         | 1       |

### Invariant protected
**I4 / §17:** A historical operation result is never proof of current ownership. A closure already in the table stops any delayed TryClaim from succeeding, even if the worker previously held the attempt.

---

## SCH08 — Confirmation racing with retargeting

**Source:** §27 "confirmation racing with retargeting"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | MATCHING  | attemptA    | 2       |
| Res     | ASSIGNED  | attemptA    | 2       |
| Res2    | AVAILABLE | —           | 1       |

### Interleaving
```
Worker-A observes a transient error; decides to retarget to attemptB
Worker-A CAS R: MATCHING(v2,attemptA) → RETRYING   [starts retarget]
── RACE: Worker-B (slow confirmation of attemptA) also runs ──
Worker-B issues CAS R: MATCHING(v2,attemptA) → MATCHED   [confirmation attempt]
FSM: R is RETRYING(v3), not MATCHING(v2); version CAS fails → VERSION_CONFLICT
Worker-B's confirmation is rejected safely; RETRYING wins
Worker-A issues CloseAttempt(Res, attemptA) → Res: AVAILABLE, v3; closure recorded
Worker-A CAS R: RETRYING(v3,attemptA) → WAITING(v4); retry outbox committed
Worker-A CAS R: WAITING(v4) → MATCHING(attemptB,Res2), v5
TryClaim(Res2, attemptB) → ASSIGNED
CAS R: MATCHING(v5,attemptB) → MATCHED(v6)
```

### Expected final state
| Entity   | Status   | activeClaim | version |
|----------|----------|-------------|---------|
| R        | MATCHED  | attemptB    | 6       |
| Res      | AVAILABLE| —           | 3       |
| Res2     | ASSIGNED | attemptB    | 2       |

### Invariant protected
**§17 / §18 / I3:** RETRYING fences confirmation; the exact request-state/version CAS ensures that only one of {confirmation, retarget} succeeds. Neither a stale confirmation nor a premature retarget can leave the system with two live claims.

---

## SCH09 — Stale successful operation-result replay after attempt closed

**Source:** §27 "stale successful operation-result replay after an attempt was closed"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | MATCHING  | attemptA    | 2       |
| Res     | ASSIGNED  | attemptA    | 2       |

### Interleaving
```
Dispatcher CAS R: MATCHING(v2,attemptA) → CANCELLING, v3
CloseAttempt(Res, attemptA) → Res: AVAILABLE, v3; closure recorded
CAS R: CANCELLING(v3) → CANCELLED, v4; request.cancelled event committed
── REPLAY: delayed worker retries TryClaim with its original operationId and payload ──
FSM returns the historical SUCCESS without recreating a claim; Res remains AVAILABLE v3
Worker re-reads R  →  R is CANCELLED (terminal); historical result is not ownership
Worker does NOT issue a fresh TryClaim or confirmation; obsolete delivery is acknowledged
Res remains AVAILABLE; no spurious MATCHED state
```

### Expected final state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | CANCELLED | —           | 4       |
| Res     | AVAILABLE | —           | 3       |

### Invariant protected
**I4 / §18:** A historical SUCCESS result for a closed attempt is not a current ownership grant. Consumers re-read authoritative state; a terminal request discards redelivered intermediate results.

---

## SCH10 — Duplicate terminal events

**Source:** §27 "duplicate terminal events"

### Initial state
| Entity  | Status   | activeClaim | version |
|---------|----------|-------------|---------|
| R       | MATCHED  | attemptA    | 3       |
| Res     | ASSIGNED | attemptA    | 2       |

### Interleaving
```
CAS R: MATCHED(v3) → FULFILLING, v4
CloseAttempt(Res, attemptA, disposition=AVAILABLE) → Res: AVAILABLE, v3
CAS R: FULFILLING(v4) → FULFILLED, v5; writes request.fulfilled event [operationId=op-ff1]
── FAULT: outbox relay delivers request.fulfilled twice ──
Consumer processes event 1: R is FULFILLED; records result; acks
Consumer processes event 2 (duplicate): R is already FULFILLED; operationId=op-ff1 already committed; idempotent no-op; acks
```

### Expected final state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | FULFILLED | —           | 5       |
| Res     | AVAILABLE | —           | 3       |

### Invariant protected
**I6 / §18:** Idempotent consumers with operationId dedupe; a duplicate terminal event does not advance state, create a second closure, or corrupt version counters.

---

## SCH11 — Repeated cancel/fulfill calls

**Source:** §27 "repeated cancel/fulfill calls"

### Initial state
| Entity  | Status   | activeClaim | version |
|---------|----------|-------------|---------|
| R       | MATCHED  | attemptA    | 3       |
| Res     | ASSIGNED | attemptA    | 2       |

### Interleaving
```
Dispatcher calls CANCEL on R  →  CAS R: MATCHED(v3) → CANCELLING(v4)
CloseAttempt(Res, attemptA) → Res: AVAILABLE, v3
CAS R: CANCELLING(v4) → CANCELLED(v5)
── Repeated CANCEL call (idempotency test) ──
Dispatcher calls CANCEL again on R  →  R is already CANCELLED (terminal); returns the existing result (200), no write
── Repeated FULFILL call ──
Dispatcher calls FULFILL on CANCELLED R  →  409; terminal state; no-op
```

### Expected final state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | CANCELLED | —           | 5       |
| Res     | AVAILABLE | —           | 3       |

### Invariant protected
**§19 / I3:** Repeating cancellation returns the existing cancelled result without a write or a second release. An incompatible fulfillment of that cancelled request returns 409. Repeated fulfillment of a fulfilled request follows the same idempotent rule.

---

## SCH12 — Restart during snapshot creation

**Source:** §27 "restart during snapshot creation/restore"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | MATCHING  | attemptA    | 2       |
| Res     | ASSIGNED  | attemptA    | 2       |

### Interleaving
```
Raft leader initiates FSM snapshot (§16)
── FAULT: storage node crashes mid-snapshot; snapshot file is incomplete ──
Node restarts; detects incomplete snapshot; discards it
Node replays Raft log from the last committed checkpoint
All committed state (R=MATCHING, Res=ASSIGNED) is fully restored
Worker resumes: CAS R: MATCHING(v2,attemptA) → MATCHED; writes outbox event
```

### Expected final state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | MATCHED | attemptA    | 3       |
| Res     | ASSIGNED| attemptA    | 2       |

### Invariant protected
**§16 / I2:** A partial snapshot is never used; log replay restores the exact committed FSM state deterministically. No confirmed write is lost.

---

## SCH13 — Restart during snapshot restore

**Source:** §27 "restart during snapshot creation/restore"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | MATCHING  | attemptA    | 2       |
| Res     | ASSIGNED  | attemptA    | 2       |

### Interleaving
```
New node begins installing a snapshot from leader (§16)
── FAULT: node crashes during snapshot installation ──
Node restarts; discards incomplete snapshot
Node requests a new snapshot from the leader
Full snapshot installs successfully
Node catches up remaining log entries
Confirmed state (R=MATCHING, Res=ASSIGNED) fully present
```

### Expected final state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | MATCHING  | attemptA    | 2       |
| Res     | ASSIGNED  | attemptA    | 2       |

### Invariant protected
**§16 / I2:** A node joining mid-snapshot-install never applies a partial snapshot; it retries the install. FSM state matches the leader's committed state after successful installation.

---

## SCH14 — Coordinator restart with one shard unavailable

**Source:** §27 "coordinator restart with one shard unavailable"

### Initial state
| Entity      | Status      | activeClaim | version | Notes |
|-------------|-------------|-------------|---------|-------|
| Coordinator | Running     | —           | —       | Fixed ring v1 |
| Shard 1     | UNAVAILABLE | —           | —       | Lost quorum |
| R           | PENDING     | —           | 1       | Routed to Shard 0 |
| Res         | AVAILABLE   | —           | 1       | Known eligible resource on Shard 0 |

### Interleaving
```
Coordinator pod restarts
Coordinator reloads ring config (same ring version + checksum; §10/§17)
Coordinator discovers Shard 1 is still unavailable
Coordinator routes reads/writes for Shard 0 keys normally
Shard 1 operations return explicit error; not empty/success
Worker resumes the known candidate on Shard 0; no incomplete scan is treated as NO_CANDIDATE
CAS R: PENDING(v1) → MATCHING(attemptA), v2
TryClaim(Res,attemptA) on Shard 0 → ASSIGNED v2
CAS R: MATCHING(v2,attemptA) → MATCHED(v3)
Shard 1 requests remain queued; no silent data loss
```

### Expected final state
| Entity      | Status   | activeClaim | version | Notes |
|-------------|----------|-------------|---------|-------|
| Coordinator | Running  | —           | —       | Same ring checksum after restart |
| R           | MATCHED  | attemptA    | 3       | Normal completion on Shard 0 |
| Res         | ASSIGNED | attemptA    | 2       | Reciprocal resource claim on Shard 0 |
| Shard 1 ops | ERROR    | —           | —       | Explicit failure; never empty result |

### Invariant protected
**I7 / §10:** A partial shard scan is never an empty result. After coordinator restart the ring checksum is identical; no accidental re-bootstrap or lost routing state occurs.

---

## SCH15 — Broker outage during retry scheduling

**Source:** §27 "broker outage during retry scheduling"

### Initial state
| Entity  | Status    | activeClaim | version |
|---------|-----------|-------------|---------|
| R       | RETRYING  | attemptA    | 3       |
| Res     | ASSIGNED  | attemptA    | 2       |

### Interleaving
```
Outbox relay tries to publish the pending retry event to RabbitMQ relief.requests exchange
── FAULT: broker is down; publish fails (no confirm received) ──
Relay leaves the outbox row unpublished; there is no inbound delivery to ack
R remains RETRYING in FSM (intent preserved, §18)
Outbox relay detects pending row; backs off with bounded exponential delay
Broker recovers; relay republishes with confirms and mandatory
Relay marks event published only after routed confirm received
Matcher consumer picks up retry event; re-reads R=RETRYING with attemptA preserved
CloseAttempt(Res,attemptA) → OK; closure committed; Res AVAILABLE v3
CAS R: RETRYING(v3,attemptA) → WAITING(v4); retry outbox committed
CAS R: WAITING(v4) → MATCHING(attemptB), v5
TryClaim(Res,attemptB) → ASSIGNED v4
CAS R: MATCHING(v5,attemptB) → MATCHED(v6)
```

### Expected final state
| Entity  | Status  | activeClaim | version |
|---------|---------|-------------|---------|
| R       | MATCHED | attemptB    | 6       |
| Res     | ASSIGNED| attemptB    | 4       |

### Invariant protected
**I6 / §18:** Broker outage during retry scheduling must not result in a lost intent. The outbox row is not marked published before a routed confirm. The relay ensures at-least-once delivery after recovery.

---

## SCH16 — Poison messages

**Source:** §27 "poison messages"

### Initial state
| Entity            | Status    | activeClaim | version | Notes |
|-------------------|-----------|-------------|---------|-------|
| matching.requests | —         | —           | —       | Queue with one malformed message |
| R                 | PENDING   | —           | 1       | Separate valid request |
| Res               | AVAILABLE | —           | 1       | Eligible resource for valid R |

### Interleaving
```
Matcher consumer receives malformed message (unparseable / missing required fields)
Consumer cannot parse and nacks with requeue=false; it never acks or tight-loop requeues
RabbitMQ routes the rejected delivery to matching.dead through the configured DLX (§20/21)
Alert triggered on dead-letter queue depth
Valid R message processed normally on matching.requests
CAS R: PENDING(v1) → MATCHING(attemptA), v2; TryClaim(Res,attemptA) → ASSIGNED v2
CAS R: MATCHING(v2,attemptA) → MATCHED(v3)
Dead-lettered message is never silently dropped; retained for investigation
```

### Expected final state
| Entity              | Status   | activeClaim | version | Notes |
|---------------------|----------|-------------|---------|-------|
| R                   | MATCHED  | attemptA    | 3       | Valid request processing unaffected |
| Res                 | ASSIGNED | attemptA    | 2       | Reciprocal claim for R |
| malformed message   | DLX'ed   | —           | —       | Landed in matching.dead; not lost |

### Invariant protected
**§21 / I6:** Malformed messages must not block healthy messages or cause tight nack-requeue loops. Dead-lettering is the correct disposal path; the DLQ retains the message for forensics.

---

## SCH17 — Partial candidate scan

**Source:** §27 "a partial candidate scan"

### Initial state
| Entity  | Status      | activeClaim | version | Notes |
|---------|-------------|-------------|---------|-------|
| R       | PENDING     | —           | 1       | type=MEDICINE, radius=50 km |
| Res     | AVAILABLE   | —           | 1       | Eligible candidate on Shard 1 |
| Shard 1 | UNAVAILABLE | —           | —       | Holds some candidate resources |
| Shard 0 | AVAILABLE   | —           | —       | Holds other candidate resources |
| Shard 2 | AVAILABLE   | —           | —       | Holds other candidate resources |

### Interleaving
```
Matcher initiates authoritative candidate scan across all shards
Shard 1 scan times out / returns error
── RULE: partial scan is NEVER treated as empty result (I7) ──
Matcher records scan as incomplete; does not issue NO_CANDIDATE
Matcher does NOT CAS R → WAITING with reason NO_CANDIDATE
Matcher returns partial-scan indicator to scheduler
R remains PENDING v1; no durable handoff has committed
Consumer keeps the delivery unacked and backs off/reconnects until Shard 1 recovers
Shard 1 recovers; full scan completes; eligible candidates found
Matcher proceeds: CAS PENDING→MATCHING; TryClaim; CAS→MATCHED
```

### Expected final state
| Entity  | Status   | activeClaim | version | Notes |
|---------|----------|-------------|---------|-------|
| R       | MATCHED  | attemptA    | 3       | Only after full scan completes |
| Res     | ASSIGNED | attemptA    | 2       | Reciprocal claim for R |

### Invariant protected
**I7 / §17:** A failed or partial shard scan is never reported as an empty result. NO_CANDIDATE is only issued after a complete authoritative scan of all shards returns zero eligible resources.
