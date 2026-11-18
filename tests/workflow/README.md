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
matcher admission, verifies no foreign pending outbox work for every cluster selection, uses a reconnecting
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

## Durable-handoff follow-up (#109)

SCH04 reads attemptA's exact durable closure before retargeting and again after
attemptB owns the resource, and tries the late attempt at both boundaries. The
resource/request/attempt/fingerprint, retry reason, disposition and frozen
closure timestamp must match; a later successful match cannot substitute for
this proof.

Every applicable schedule now observes production matcher/cleanup transitions,
checks the expected event family, immutable payload, entity version/time and
operation result, then replays the unchanged command. Current entity state and
the strong pending-outbox rows must remain identical. Fixture final versions and
claims are unchanged. Direct synthetic confirmation steps now use the production
confirmation helper. SCH13a/b restore an already-matched fixture without a new
workflow handoff; their snapshot row-equivalence assertions remain authoritative.

Kind fixtures publish their verified pending events with real mandatory confirms
to a disposable broker/vhost before marking them. A durable audit queue receives
events on `relief.events`. This avoids hiding later seeds behind the public
pending endpoint's 100-row bound. A foreign pending entity or incomplete drain
fails; no foreign work is silently marked, and no event is marked without a routed
confirm. All cluster selections require an initially clean pending outbox, even
with `SCHEDULE_RESET=0`. Matcher admission and storage are restored on exit.

The omission negative control runs only against M8: it commits the terminal
entity while deliberately stripping its outbox event. The old final-state fixture
still passes, while the durable-row assertion rejects it. Pure negative cases
also reject missing/duplicate/corrupted rows and missing or foreign closures.
No deliberately broken command is sent to kind by these negative controls.

## Internal authentication

The cluster runner uses the shared `scripts/internal-auth-env.sh` setup from
#174 after any requested reset and before changing storage images or scaling
workers. Unreadable auth state or invalid/missing Secrets aborts the run. The
shell's ring/outbox preflights and the workflow client's first ring lookup,
mutations and restart ring probes use the app sender credential. Management
seam endpoints keep their separate operator bearer credential. Temp sender and
header files are removed by the outer exit trap even if a child test fails.

Memory-only M8 has no coordinator identity and ignores cluster credential files.
For an already enabled joint kind check, use `SCHEDULE_RESET=0` and the agreed
schedule/seed subset; the ordinary default reset installs the clean default
profile. Setup rereads sender Secrets on each runner invocation; credential
rotation needs the agreed receiver overlap and a fresh invocation rather than
assuming an existing staged token changes in place. Authentication and full
schedule acceptance remain separate observations.
