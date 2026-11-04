# Executable adversarial schedule checkpoint (A2.3)

`make test-schedules SEEDS=50` runs the real M8 FSM first, then a clean local
kind deployment through the production coordinator client. SCH01 uses #94's
committed-log/before-apply barrier. SCH12 interrupts snapshot persistence; SCH13
is split into SCH13a (transfer) and SCH13b (FSM staging), with the original
workflow states and exact final versions preserved. Missing or expired fault
boundaries fail the case; no successful run is inferred from an unobserved fault.

There are 18 physical cases for the 17 original source schedules. M8 runs 17:
SCH13a is explicitly not applicable because M8 has no leader or InstallSnapshot
transport. Kind runs all 18, with real container SIGKILLs (no pod-delete fallback).
Both backend results are needed for complete A2.3 acceptance.

Each physical fault must recover in the same pod UID before its assertions
complete. Only then does teardown normally replace that pod, retaining its PVC,
and require a new Ready UID with restartCount zero for the next independent case.
This avoids cross-seed exponential CrashLoopBackOff (the first bulk run reached
2m40s at seed 5). A failed case is reported before cleanup; replacing the pod is
never counted as the fault recovery. The runner stops at the first failure.

The implemented drivers cover dropped committed replies, stopped actors before
and after intent creation, cancellation before/after claim, late claims after
closure, RETRYING vs confirmation, historical claim results, duplicate terminal
commands/outbox dedupe, repeated API status updates, coordinator restart with
an unavailable shard, broker transport outage, poison dead-lettering and
partial candidate scans. The assertions use the canonical A0.2 final versions,
statuses and reciprocal claims. They use the public Store API and production
workflow/messaging helpers, not a second FSM.

SCH01's separate `TestCASProposalBarrierProbe` proves rejection of two
same-version proposals queued before Store invocation. It cannot prove that both
commands reached the FSM log before apply and is not counted as SCH01 acceptance; the real seam-backed driver is counted instead.
SCH10 checks duplicate terminal command replay and one frozen fulfilled outbox
event; it does not claim a new consumer for state-change events (matching consumes
created/retry hints). M8 SCH14 checks explicit partial reads and persisted-state
recovery; physical coordinator replacement is exercised only by the kind driver.
Crash cases stop a logical worker at the command boundary; they do not claim an
OS process was killed for each in-memory case.

For a development subset (explicitly not full acceptance):

```sh
SCHEDULE_BACKEND=m8 SCHEDULE_IDS=2,3,4,5,6,7,8,9,10,11,14,15,16,17 \
  SCHEDULE_RUN_ID=my-run make test-schedules SEEDS=50
```

Use `SCHEDULE_BACKEND=cluster` for kind or leave it unset for both. The cluster
path runs demo-reset unless `SCHEDULE_RESET=0` is explicitly supplied. It pauses
matcher admission, verifies no foreign pending outbox work for the relay case, uses a reconnecting
loopback coordinator port-forward (NetworkPolicy stays enabled), and restores
both shard replicas and matcher admission on exit, including a Go test timeout.
Do not run it alongside another cluster test or on a non-disposable deployment.

For SCH14 alone, `SCHEDULE_BACKEND=cluster SCHEDULE_IDS=14
SCHEDULE_HOLD_SHARD=1` keeps shard 1 unavailable across all seeds, matching its
initial fixture. Every seed still restarts the coordinator and requires a new
Ready UID, unchanged ring, explicit failed shard and healthy-shard progress.
The outer harness restores the original shard replicas after the whole batch.

The broker is disposable, pinned to the chart's exact RabbitMQ image/digest,
with a separate vhost for each case. Supplying both MQ_AMQP_URL and
MQ_MANAGEMENT_URL instead requires an isolated test broker. The refused-endpoint
fault is a real connection failure; recovery uses real mandatory confirms.

Each failed case prints run ID, seed and canonical fixture tag. Reproduce with
the same `SCHEDULE_RUN_ID`, `SEEDS` and `SCHEDULE_IDS` on another clean deployment.
IDs, proposer timestamps and matcher seed are explicit; cases occupy isolated
geographic cells so earlier AVAILABLE resources cannot affect later candidates.
SEEDS is bounded to 1..50 by that fixture grid.

The aggregate Go timeout is 150 minutes: #94 measured SCH13a at 44.23s and
SCH13b at 53.02s per run, before the other physical fault cases across 50 seeds.
Only kind snapshot cases get a three-minute case budget; ordinary workflows keep
their 30-second limit. Post-assertion fixture teardown has its own 90-second rollback budget so it can still run after a test-work timeout. Storage requests keep their existing deadlines, and the
SCH01 barrier stays at 2.5s under storage's 3s request deadline. The runner installs
the seams image only for cases that need it, then restores the captured image tag,
isolation labels, shard replicas and matcher admission even on failure. The outer rollback is tested independently
because a Go timeout can bypass `t.Cleanup`. Cleanup still attempts matcher
restoration if storage restoration fails, and reports the failure.

Normal `make check` remains cluster-free. The `schedules` tag opts into these
infrastructure tests; a draft PR is not evidence that A2.3 acceptance passed.
