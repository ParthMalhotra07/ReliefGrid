# Requirement-to-demo matrix and measured results

## Course requirement mapping

| Requirement | Demonstration | Source and evidence |
| --- | --- | --- |
| Create and retrieve request | Demo1 | [script](../../scripts/demos/demo-1.sh) |
| Register and update resource availability | Demo2 | [script](../../scripts/demos/demo-2.sh) |
| Match by type/location/window with reciprocal ownership | Demo3 | [script](../../scripts/demos/demo-3.sh), [workflow tests](../../internal/workflow) |
| Distribute data across three logical shards | Demo4 | [script](../../scripts/demos/demo-4.sh), [ring contract](../../contracts/ring.yaml) |
| Asynchronous burst processing through RabbitMQ | Demo5 | [script](../../scripts/demos/demo-5.sh), [messaging tests](../../internal/mq) |
| Continue via replicas after storage leader failure | Demo6 | [script](../../scripts/demos/demo-6.sh) |
| Recover/replay/catch up a restarted node | Demo7 | [script](../../scripts/demos/demo-7.sh) |
| Application pod failure and Kubernetes recovery | Demo8 | [script](../../scripts/demos/demo-8.sh), [recovery evidence](../evidence/A3.2) |
| Kubernetes microservice scaling | Demo9 | [script](../../scripts/demos/demo-9.sh) |

All nine scripts were recorded twice in the
[4 November gate report](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/gate.txt):
18/18 PASS. The report records clean deployment per round, reset durations 129 s and
104 s, no cordons and the exact tested code-tree hash. These are observed times
on Hrishabh's machine, not guaranteed setup times on every laptop.

## Historical gate measurements

The retained 4 November gate on dba92c514ef1, code-tree fingerprint ecc68871df28,
records the following values. Shard counts refer only to demo 4's deterministic
300-key seed, not the total changing application dataset:

- Round 1: shard counts `107/84/109`; election `3642 ms`; acknowledged `2692`; while down `2089`; replay command index `7911 -> 13690`.
- Round 2: shard counts `107/84/109`; election `5007 ms`; acknowledged `2635`; while down `2017`; replay command index `7320 -> 12188`.

Sources: [round1 demo 4](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/round-1/demo-4.txt),
[demo 6](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/round-1/demo-6.txt),
[demo 7](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/round-1/demo-7.txt);
[round2 demo 4](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/round-2/demo-4.txt),
[demo 6](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/round-2/demo-6.txt),
[demo 7](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/round-2/demo-7.txt).
Demo6 used seed 6, four writers for 40 s, and stopped shard1's leader about 8 s into
the workload while keeping its replacement Pending. All 2,692/2,635 acknowledged
writes were read back, with zero lost in these runs; 2,089/2,017 were acknowledged
while that leader was down. The observed elections were approximately 3.6 s/5.0 s,
not an SLA. Demo7 continued the same stopped-node handoff, added 100 writes while
it was down, and verified all 100 in the restarted node's own FSM. The replay
figures above are completed SQLite command indexes, not Raft log indexes.

The gate host is hrishabh-sharrma-ThinkPad-E14-Gen-7. Separate retained records
describe that host as **8 CPUs, Intel Core Ultra 7 258V**
([CPU record](../evidence/S4.4/bench-apply-20261103T085339Z.txt#L10)) and show
**31,094 MiB total usable memory** (about 30.37 GiB)
([resource record](../evidence/S4.3/suite-20261101T112942Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/resources.txt#L1)).
Those are separate host records, not synchronized measurements of the4 November
gate. That gate's CPU allocation, power policy and unrelated concurrent load were
not retained; this missing context prevents treating its functional numbers as
a controlled capacity benchmark. Application background activity was present.

## Retained reliability evidence and limits

These are retained observations with their original source/tree/image
qualifications. No historical run is promoted to acceptance of integrated main
`82cc664` or pending PR combinations.

| Evidence | What it supports | Boundary |
|---|---|---|
| [Course gate](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/gate.txt#L1) | Recorded 18/18 demo passes across two resets; clean recorded tree, fingerprint ecc68871df28 | Historical course gate, not a fresh gate on integrated main or later PRs |
| [S4.2 — Replacement/admission](../evidence/S4.2/admission-replace-hrishabh-sharrma-ThinkPad-E14-Gen-7.txt#L1) | Recorded 63/63 checks, including interrupted replacement | Recorded host/date/tree; not arbitrary multi-disk disaster recovery |
| [S4.6b — Physical schedule seams](../evidence/S4.6b/kind-hrishabh-sharrma-ThinkPad-E14-Gen-7.txt#L1) | crictl kills at proven log/snapshot boundaries; SCH01/12/13a/13b retained PASS | Seams image and selected schedules, not every failure timing |
| [S4.5 — Same-index audit](../evidence/S4.5/kind-20261103T120239Z.txt#L1) | Retained replica audit observations at a common index | Sampled-state digest equality, not complete protocol correctness |
| [S4.3 — Chaos re-judging](../evidence/S4.3/rejudge-20261101/README.md#L1) | Eleven saved reports re-judged using conservative reconstructed coverage; coordinator physically re-run separately | Original ledgers not re-read; not twelve newly executed passes |
| [Application trace](../evidence/S4.4/verify-app-trace-20261104T180039Z.txt#L1) and adjacent JSON | One Jaeger trace includes API, coordinator, storage, relay/matcher path | One observed trace, not full delivery/correctness proof |
| [Aging seam](../evidence/S4.4-aging/README.md#L1) | Merged transactional seam, focused/full check logs and four mutation failures | README explicitly records no physical cluster; not enabled application aging |

S4.6's [retained CI verification](../evidence/S4.6/verify-hrishabh-sharrma-ThinkPad-E14-Gen-7.txt)
records its original tooling/check outcome; it is not a current integrated gate.
S4.2, S4.3, S4.5, S4.6 and S4.6b are Hrishabh's storage/reliability goals.

The S4.3 [suite definition](../../chaos/scripts/suite.sh) contains twelve scenarios:

- `kill-leader`: election and acknowledged-write retention.
- `kill-follower`: majority service and return/catch-up.
- `restart-shard`: explicit outage followed by group recovery.
- `isolate-replica`: partitioned-replica behavior and recovery.
- `isolate-leader`: the isolated leader must not commit or serve fenced reads.
- `delay-isolate-leader`: delayed acknowledgments followed by leader isolation.
- `lose-quorum`: no committed/fenced service while a majority is unavailable.
- `delay-follower`: majority progress and later lag convergence.
- `stop-worker`: data-worker outage with shard-majority behavior.
- `restart-coordinator`: routing outage, unchanged ring and recovery.
- `control-lost-write`: injected acknowledged-loss negative control; success
  means the judge detects that injected fault and no unrelated failure.
- `control-stale-reader`: injected stale-read negative control with the same
  isolation of the intended failure.

These are ten faults plus two judge controls, not twelve newly executed clean
production-fault passes. The retained re-judging/coverage limits below still apply.

Chaos failures remain evidence. The [10-second coordinator run](../evidence/S4.3/suite-20261101T144808Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/summary.txt#L1)
reports FAILED for insufficient per-shard read coverage. The later 30-second run
was re-judged using aggregate write coverage for the shared coordinator fault;
the [re-judging explanation](../evidence/S4.3/rejudge-20261101/README.md) records
that change. These are measurement/judge qualifications, not observed data-loss
incidents. During the observed coordinator outage, client availability through
the single routing service was zero while shard terms stayed unchanged. The
retained no-lost/no-stale observations do not establish universal losslessness.

Unit/integration tests cover same-version CAS, identical replay, dedupe, late
claim after closure and historical success versus ownership
([FSM tests](../../internal/fsm/fsm_test.go#L125)). Fence tests cover isolated
leaders, old acknowledgments and waiting for SQLite
([fence tests](../../internal/raftstore/fence_test.go#L192)). M8 executes the real
SQLite FSM serially without Raft; passing it does not establish network, quorum
or disk behavior. CI implements generated-contract drift/full checks, separate
seams/schedule jobs and a kind smoke path ([CI](../../.github/workflows/ci.yml)).
An implemented check is not evidence that current CI or a current integrated
gate passed.

[A2.3 evidence](../evidence/A2.3/README.md) covers the reproducible adversarial
schedule campaign. The [shared demo library](../../scripts/demos/lib.sh) and
[application verifier](../../scripts/demos/app_demo.py) identify assertions for
ownership, durable handoffs, interrupted calls, recovery and replica traffic.

`invcheck` walks every request/resource page, checks claim uniqueness,
reciprocal matched/in-progress ownership, terminal cleanup and retained intents,
and reports outbox backlog ([implementation](../../internal/invcheck/invcheck.go#L1)).
Complete, clean and quiescent must all hold. A nil-error result is needed for
completeness; applied indexes are checked before/after the walk because these
cross-shard reads are not one snapshot
([quiescence fields](../../internal/invcheck/invcheck.go#L82),
[completeness boundary](../../internal/invcheck/invcheck.go#L89)). Quiesce admission and
settle workflows before treating this as stable-state evidence. An empty
violations list on incomplete or busy input is insufficient.

Tracing supplies W3C HTTP/AMQP propagation, producer/consumer spans and
retry/redelivery links ([tracing](../../internal/obs/obs.go#L129)). Trace metadata
does not influence domain decisions. Export is disabled by default; an overlay
enables OTLP ([storage tracing values](../../deploy/helm/storage/values.yaml#L61)).
Storage metrics use bounded labels and private scrape ports. Failed outbox
observations are absent with an explicit failure flag, never a fresh zero
([metrics contract](../operations/storage-metrics.md#L7)). App/dashboard acceptance
retains its own [evidence lifecycle](validation-addendum.md).

## Accepted S5.2 Phase 1 measurements

#126 is merged. Seven retained load reports (three baseline, three step and one
soak) describe measured source `7dd19404d6df0f7c098810d4c1ad093355a0cd3a`,
recorded clean. The report/checker and aggregate rounding correction are accepted;
this replaces the former blanket pending table, not the raw evidence.

The single host is an Intel Core Ultra 7 258V, eight logical CPUs and 30.4 GiB
recorded RAM. kind used CPUs 0-5 and k6 6-7; the power profile was performance,
restored afterward. Configured resources were Guaranteed, the Raft load profile
and fixed API2/matcher2 were used, tracing/autoscaling were off. The storage fence
setting alone does not establish that application callers used it: Phase 1
predates #166's linearizable default. Recorded image IDs and source metadata
must not be strengthened into an independently proven immutable build mapping.
[Example environment/facts](../evidence/S5.2/S5.2-baseline-20261111T105012Z/facts.json).

The seeded mix is 15% request creates, 10% resource registrations, 10% match,
55% ID reads and 10% lists, seed 52. Baseline and soak exclude 120 s HTTP warm-up;
the step run records no such warm-up. k6 uses open arrival rates, not a fixed
closed-loop concurrency capacity test. Percentiles below are client HTTP
milliseconds; repeated-row values are medians of each run's percentile, not
pooled-request percentiles or confidence intervals.

| Scenario | Repeats / measured duration | Offered / achieved req/s | Write / ID-read / list p99 ms | HTTP error fraction |
|---|---|---|---|---|
| Baseline | 3 x 600 s | 25 / 25 | 684.00 / 79.94 / 291.22 | 0.001 = 0.100% |
| Step | 3 x 960 s | 10-80 steps / 43.86 aggregate median | 1049.34 / 102.43 / 367.06 across the run | 0.00095 = 0.095% |
| Soak | 1 x 1800 s | 35 / 35 | 838.84 / 93.75 / 330.37 | 0.001275 = 0.1275% |

Baseline write p99 spans 665.75-690.92 ms, ID reads 79.12-80.48 ms and lists
280.82-305.65 ms; the error fraction spans 0.0005-0.00125. The three step runs
all bracket the Phase 1 HTTP threshold at 50 req/s, failing the write-p99 1 s
criterion at 60. This is distinct from the 43.86 aggregate rate over the stepped
run, and is not sustainable matching throughput. The other acceptance limits
are ID-read p99 <=250 ms, list p99 <=500 ms, HTTP errors <=1% and no dropped
iterations. Baseline/soak retain zero dropped iterations.

Sources: [baseline aggregate](../evidence/S5.2/aggregate-baseline.json),
[step aggregate](../evidence/S5.2/aggregate-step.json),
[soak report](../evidence/S5.2/S5.2-soak-20261111T112933Z/summary.json),
[checker/method](../../loadtest/k6/README.md).

Important qualification: HTTP completion did not mean the system quiesced under
continued admission. Baseline matched/WAITING counts were 1483/780; step counts
4276/2253. Matched latency excludes WAITING. Broker publish-confirm gaps and retry
backlog are retained, not rounded away. The soak records 416 unconfirmed publishes
during load; one step run records 5968, with 5209 in its after-run window. These
observations are not automatically acknowledged data loss. Audits stopped retry
admission, waited for historical ready+unacked headroom, drained durable handoffs,
then checked invariants and restored matcher replicas. That is a stopped-admission
audit, not proof of a stable busy-system equilibrium. #170's later total-message/
returning-retry check is a separate harness change; historical reports keep their
original audit rule. Off-repo raw files remain listed by hash, not revalidated as
if their contents were present. [Retained verifier output](../evidence/S5.2/verify-hrishabh-sharrma-ThinkPad-E14-Gen-7.txt).

| Still pending | Required evidence |
|---|---|
| Accepted later linearizable-versus-Barrier system-load comparison | Immutable run/source/image records and reviewed raw reports; do not substitute chat numbers |
| Exploratory 100-110 req/s claim | n=1 with incomplete audit is exploratory, not validated capacity |
| Final integrated gate and enhancements | Actual final source/images, alias/form checks and separately accepted auth/aging/autoscaling scope |

Historical [FSM tracing microbenchmarks](../evidence/S4.4/bench-apply-20261103T085339Z.txt#L1)
and [strong-read tracing microbenchmarks](../evidence/S4.4/bench-strongread-20261103T085339Z.txt#L1)
record a concurrently busy host; one fence/parallel comparison reports a
significant slowdown. They do not justify an absolute zero-overhead statement
or current application throughput. No microbenchmark value fills the pending table.

## Other limitations and final-package work

- The single coordinator is a routing availability limit; its loss is not
  transparent high availability. Quorum loss can make shard service unavailable.
- Stale hints cannot prove no candidates or current ownership. List pages are
  per-shard observations, not a global snapshot.
- #134/#136 ETag/cursor code is merged in this source baseline; neither that fact
  nor later PR approvals relabel the older course gate. Other app enhancements
  retain their reviewed implementation/evidence state.
- The original PS field labels and implementation choices are recorded in
  [PS mapping](ps-compliance.md). #128 is merged with per-path tests; the scoped
  gate did not execute its ingress alias verifier, so final deployed alias checks
  remain necessary. `/api/v1` equivalence alone is insufficient.
- The professor confirmed a report under 10 pages. The agreed nine-page
  [submission outline](submission-outline.md), final reviewed SHA, final gate,
  rehearsals, recording/playback and submission receipt remain final-package
  deliverables. Public visibility is observed separately from actual submission.
- Both owners agreed to use the existing S4.1 barrier-versus-fence study as the
  single M6 experiment. [ADR006](../adr/006-read-fence-study.md) is merged
  via #152 and is included in this baseline. This is a bounded historical study, not a new protocol extension or
  a present-system performance guarantee; other major extensions remain future work.
  Hrishabh built the fence implementation and benchmark as S4.1; #152 packages
  that existing study as M6. All three replicas run in one Go test process over
  127.0.0.1, not three separate processes or hosts. Go 1.27.2 was pinned in source
  and recorded in the separate safety run; the benchmark raw output omits its
  runtime version, so the benchmark runtime remains unrecorded.
- Application aging #151 is merged in this baseline, but remains disabled by
  default. The [application aging policy](../operations/application-aging.md)
  requires coordinator plus all nine storage replicas to support the seam and
  rollout verification before enabling it. No enabled-cluster aging acceptance
  or completed enhanced gate is inferred from that merge.

## Agreed core-gate scope and open enhancements

The [agreed scope](../operations/gate-scope.md) was executed in the
[12 November scoped gate](../evidence/gate-20261112T100532Z/report.md),
source 1623c23. Recorded course reports are 18/18, chaos 12/12 runs (ten faults
plus two controls, 22 checks), Merkle 7/7 and strict invcheck clean/quiescent.
The fixed-profile collector records API2/matcher2, no HPA/ScaledObject and
runtimeBuildBinding NOT_ESTABLISHED. The reset wrappers took 107/116 s on that
host. These later snapshots do not fill missing restoration evidence in #123's
earlier scaling run. Some console/chaos/dashboard/trace material remains in the
original run root; the retained report explains that boundary.

Positive aging still needs coordinator/all-nine-replica rollout verification and
policy agreement. Auth requires all caller wiring plus joint enablement/rotation
checks. Autoscaling, if included, needs final-cohort acceptance; otherwise label
it untested on that cohort while retaining the qualified historical #123 result.
The final gate must exercise the aliases and forms on its own recorded source.
No schedule date or merged implementation alone establishes these acceptances.

## Phase 2 read-path comparison: reviewed evidence #176

[#176 comparison](https://github.com/ParthMalhotra07/ReliefGrid/blob/25ea8bc5675ab935d0b4852f8adbacb4cf14b9b3/docs/evidence/S5.2/phase2-linearizable/comparison.md) binds three baselines and three step runs
to historical measured source 514bb33e1ffd3c6f8a83591fc82e6347b837d7ac, compared with
Phase 1 source 7dd19404d6df0f7c098810d4c1ad093355a0cd3a. The current integrated
source includes merged #166; it was not itself newly load-tested by this review.

| Measurement (medians of three runs) | Phase 1 strong | Phase 2 linearizable |
|---|---:|---:|
| HTTP threshold bound | 50 req/s; fails at 60 | >80 req/s; no tested step fails |
| Baseline write / ID-read / list p99, ms | 684 / 79.94 / 291.22 | 93.57 / 56.43 / 90.31 |
| Write p99 at offered 80 req/s, ms | 1180.37 | 388.50 |
| Committed barriers per measured step run | 270191 | 0 |
| Global broker refusals per measured step run | 6324 | 8 |

All six retained reports/checksums and the request-CSV comparison were replayed
without drift. Phase 2 audits, invariant checks and restoration pass; the
above-range classifier preserves safety failures and rejects fabricated
saturation points. The first acceptance tail was completed by hand as disclosed.
Phase 2 adds no soak. Error fractions remain nonzero (baseline 0.000167, step
0.001093). The 29 remaining counter differences are applied commands in flight
at the end scrape, not committed barriers.

The two exploratory runs near 100-110 req/s are n=1 each with incomplete audits:
one drain was refused and the other retained a WAITING retry backlog. Generator
memory or the 400-VU cap constrained higher rates. They are request-path
indications, not audited capacity or sustainable matching throughput. At upper
steps the coordinator approaches 98% of 250 mCPU and the request queue is near
capacity; this is observed pressure, not proof of a sole bottleneck. Retain the
full [method/deviation/omitted-file qualifications](https://github.com/ParthMalhotra07/ReliefGrid/blob/25ea8bc5675ab935d0b4852f8adbacb4cf14b9b3/docs/evidence/S5.2/phase2-linearizable/README.md). The
scoped gate, later chaos campaign, load comparison and final gate remain
separate evidence sets.
