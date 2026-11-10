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
[4November gate report](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/gate.txt):
18/18PASS. The report records clean deployment per round, reset durations129s and
104s, no cordons and the exact tested code-tree hash. These are observed times
on Hrishabh's machine, not guaranteed setup times on every laptop.

## Retained reliability evidence and limits

These are retained observations with their original source/tree/image
qualifications. No historical run is promoted to acceptance of integrated main
`42741b1` or pending PR combinations.

| Evidence | What it supports | Boundary |
|---|---|---|
| [Course gate](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/gate.txt#L1) | Recorded 18/18 demo passes across two resets; clean recorded tree, fingerprint ecc68871df28 | Historical course gate, not a fresh gate on integrated main or later PRs |
| [Replacement/admission](../evidence/S4.2/admission-replace-hrishabh-sharrma-ThinkPad-E14-Gen-7.txt#L1) | Recorded 63/63 checks, including interrupted replacement | Recorded host/date/tree; not arbitrary multi-disk disaster recovery |
| [Physical schedule seams](../evidence/S4.6b/kind-hrishabh-sharrma-ThinkPad-E14-Gen-7.txt#L1) | crictl kills at proven log/snapshot boundaries; SCH01/12/13a/13b retained PASS | Seams image and selected schedules, not every failure timing |
| [Same-index audit](../evidence/S4.5/kind-20261103T120239Z.txt#L1) | Retained replica audit observations at a common index | Sampled-state digest equality, not complete protocol correctness |
| [Chaos re-judging](../evidence/S4.3/rejudge-20261101/README.md#L1) | Eleven saved reports re-judged using conservative reconstructed coverage; coordinator physically re-run separately | Original ledgers not re-read; not twelve newly executed passes |
| [Application trace](../evidence/S4.4/verify-app-trace-20261104T180039Z.txt#L1) and adjacent JSON | One Jaeger trace includes API, coordinator, storage, relay/matcher path | One observed trace, not full delivery/correctness proof |
| [Aging seam](../evidence/S4.4-aging/README.md#L1) | Merged transactional seam, focused/full check logs and four mutation failures | README explicitly records no physical cluster; not enabled application aging |

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
([index checks](../../internal/invcheck/invcheck.go#L89)). Quiesce admission and
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

## Performance methodology and pending measurements

#126 remains draft at `a5785cee7f797bdc68b511a91df383a889477b41`, with no final
load evidence. A dirty-tree one-minute smoke is not a final benchmark. The
integrated baseline has ledger support; proposed seeded datasets, k6 scenarios,
report generator and run/check scripts remain pending that PR. Older #129/#130
prerequisites must be reconciled with the final integrated revision, not copied
as current blockers.

The proposed method uses seeded datasets; smoke, step-to-saturation, baseline
and soak scenarios; the load Raft profile; declared Guaranteed resources;
controlled CPU placement for kind versus k6; and a recorded power policy restored
afterward. Record exact source SHA/dirty state and image identities, host CPU/RAM,
tool pins, dataset/workload mix, read mode, topology, resource limits, timeouts,
tracing sample ratio, warm-up, durations and offered/achieved load. Repeat
controlled runs, report median plus min/max, and preserve raw and rejected runs.
Review/merge the final tooling and acceptance criteria before describing this
as executed methodology.

Report client p50/p95/p99 separately for writes, ID reads and lists, alongside
errors by status/code and unknown outcomes. Pair offered/achieved throughput
with Raft commit/apply histograms, per-pod CPU/memory and CPU-limit saturation.
Histogram-derived quantiles are approximate and are not interchangeable with
client latency percentiles. Verify acknowledged creates read back using the
ledger; report unknown outcomes separately from acknowledged loss. Functional
scaling/recovery observations do not replace saturation measurements.

| Pending #126 result | Value | Required evidence |
|---|---|---|
| Final measured revision, runtime images and host configuration | PENDING | Clean source manifest and image/build binding |
| Write, ID-read, list p50/p95/p99 | PENDING — no numbers claimed | Per-run raw k6 and report outputs |
| Offered/achieved throughput and saturation step | PENDING | Step run plus acceptance/judge output |
| Error rate, status/code breakdown and unknown resolution | PENDING | Client outcomes and ledger |
| Raft commit/apply quantiles, CPU and memory | PENDING | Time-aligned raw metrics with units |
| Acknowledged-create ledger read-back | PENDING | Complete ledger verification |
| Repeat count, median/min/max and soak behavior | PENDING | All repeated run directories and comparison report |
| Final integrated enhanced-gate result | PENDING | Named integrated revision, runtime evidence and full acceptance output |

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
  [architecture](architecture.md); bare alias compliance still needs #128's
  review. `/api/v1` equivalence alone is insufficient.
- Instructor-confirmed report length/format, final reviewed SHA, final gate,
  rehearsals, recording/playback and submission receipt remain final-package
  deliverables. Public visibility is observed separately from actual submission.
- Both owners agreed to use the existing S4.1 barrier-versus-fence study as the
  single M6 experiment. Its ADR006 decision record remains in
  [#152](https://github.com/ParthMalhotra07/ReliefGrid/pull/152), outside this source
  baseline. This is a bounded historical study, not a new protocol extension or
  a present-system performance guarantee; other major extensions remain future work.
