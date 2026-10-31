# A2.2 — state-aware retry and confirmed transport handoffs

Phase 2 proceeds under Parth's explicit scheduling exception. This is a stacked
goal; completion still needs dependency merges, ownership review and main Verify.
The verifier re-ran A1.2, A1.3 and A2.1 on the starting stack (dependency-*.log).

## Behaviour and acceptance mapping

| Requirement | Exercised failure and assertion |
| --- | --- |
| State-aware table, Part 2 §18/C4 | All six work states preserve status/intent/disposition while RetryWorkflow atomically commits the expected-version due time and one event. |
| Unknown claim is not rejection | Both before-commit and lost-response TryClaim failures retain MATCHING, then a restarted helper retries the byte-identical original claim command. |
| RETRYING fences confirmation, SCH08 | Controlled real-FSM interleavings: confirmation first keeps its resource with no close; RETRYING first defeats stale confirmation, requires exact closure before WAITING, and a late claim returns CLOSED. |
| Unknown closure preserves intent | Failed CloseAttempt leaves RETRYING and emits no WAITING retry; a subsequent helper resumes closure. |
| Retry mutation replay | Lost RetryWorkflow response retries exactly the same command; one retry count increment and one outbox row. |
| Broker down, SCH15 | Confirmed durable workflow retry remains pending in the outbox; failing publication never marks it and never clears the intent. |
| Healthy waiting is not poison | Four due scans without candidates use 5/30/120/120s tiers; duplicate hints before due never advance count/version. Partial scans retain PENDING with INCOMPLETE_SCAN. |
| Healthy work survives >20 failures, issue #69 | Live pinned RabbitMQ: 13 transient read failures plus 12 handoff failures, 25 confirmed fixed-5s deferrals, then recovery; delivery limit remains 20 and business retryCount changes only once. |
| No ack before durable transport handoff | Unit return/nack/unknown cases and live mandatory NO_ROUTE preserve the original unacked delivery. Source ack follows a persistent routed positive confirm. |
| Lost transport confirm is safe | Live confirmed copy plus an unacked/redelivered original both reach the healthy workflow; the second hint leaves the existing schedule/version/count unchanged. |
| Poison remains separate | Malformed/permanently absent work still rejects once to matching.dead; existing A1.3 live DLX tests also run. |

## Evidence and commands

test-first.log records the initial missing implementation failures. The additional
waiting-test-first.log catches the real FSM rejecting WAITING -> WAITING; due
WAITING now uses RetryWorkflow. first-unit-failure.log preserves a test assertion
type mismatch (json.RawMessage versus []byte); comparing actual bytes corrected
that observation without loosening payload identity. unit-green.log is the next
passing run. No existing test was skipped/deleted or timeout increased.

make verify-A2.2 passed against the exact Helm-pinned RabbitMQ 4.3.7 image. The
actual output in verify-first.log records:

    readFailures=13 handoffFailures=12 confirmedDeferrals=25 deliveryLimit=20 businessRetryCount=1
    mandatory NO_ROUTE leaves original unacknowledged and redeliverable
    lost confirm preserves original plus delayed copy; both hints produce one durable business retry
    A2.2 VERIFY: ALL PASS

The new integration,a22-tagged live test needs at least 25 * 5s = 125s. Its new
180s budget is derived from that path; existing 30s tests retain their budgets.
The separate tag avoids repeating the long acceptance test inside A1.3 Verify.
The shared Makefile runs A2.2 explicitly in CI, alongside existing A1.3/A1.4/A2.1
verifiers. Broker absence is a failure. Only isolated test vhosts/containers are
cleaned up; no dev-cluster PVC or storage ownership is changed by these tests.

check.log records GITHUB_ACTIONS=true make check, including the full repository
race suite and all four goal verifiers. tagged-vet.log/tagged-lint.log include
the live a22-tagged code; gen-check.log proves frozen generated paths unchanged;
tidy.log is empty. check-before-dependency-refresh.log preserves the initial full
pass before updating the parent branches with ownership review fixes.

## Limits

Transport copies retain the byte-identical event hint and derive its priority;
they are not proof of ownership or a committed domain result. Failure to obtain
a routed confirm keeps the original recoverable and uses existing reconnect
backoff. Broker x-death/x-delivery headers are not forwarded into the fresh copy;
logs distinguish transport history from persisted business retryCount. No broker
delivery limit, frozen contract, FSM, Raft or invariant has been changed.

These focused fault/barrier tests are not the complete 21-schedule suite or a
real-cluster crash campaign. A2.3 supplies those; periodic due scans, wakeup
processing and durable workflow repair remain A2.4. The CANCELLING/FULFILLING
cleanup implementation and its schema checks remain the separate A2.1 PR.
