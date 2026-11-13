# Submission outline v2: nine pages inclusive

The professor requires **under 10 pages**. Retain the original nine sections,
order and page budgets below. This is an editorial plan, not an exported or
page-counted report. Title, figures, captions, whitespace and references all count;
there is no additional cover, contents page or appendix outside this budget.

Integrated planning baseline: `5965f840f6fcf1e3d1044ed913def8982363fd31`. The report chapters merged in #137;
Hrishabh's outline-v2 refines their allocations without adding report sections.
Source links below use that full planning SHA. Historical measurements retain
their own source/tree/image identities; this baseline does not certify their
runtime provenance. Refresh affected descriptions and source links against the
accepted final revision before export.

## Allocations and writing split

S = Hrishabh; A = Parth. Accept S's proposed **5.5 pages**, including his figures,
captions and table share, leaving **3.5 pages** for A. These are editing envelopes
within the shared nine pages, not additional author sections. Joint content must
be counted once. A leads public/worker interfaces, workflow/messaging and the
reference index; S leads storage/consistency, deployment topology and measured
storage/load results. Both check cross-boundary claims.

| Section (original order) | Pages | Lead and review | Visual allocation, including captions | Prose/citations |
|---|---:|---|---|---:|
| Title, abstract and problem statement | 0.50 | Joint | None; T1 goes in validation | 0.50 |
| Requirement/service/API mapping | 1.00 | A; S internal/failure mapping | T3 0.25, T4 0.20, T2 role column 0.10 | 0.45 |
| Architecture, sharding, consistency and schemas | 1.50 | S; A domain-state check | F1 0.35, F2 0.35 | 0.80 |
| Workflow, claims, messaging and recovery method | 1.00 | A; S consensus/closure check | F3 0.40 | 0.60 |
| Docker/Kubernetes deployment and reproducibility | 1.00 | Joint | F4 0.35 | 0.65 |
| Validation, gate and fault results with limitations | 1.50 | Joint; S storage, A app/browser/broker | T1 0.30, T5 0.35 | 0.85 |
| Performance and the selected S4.1/M6 experiment | 1.00 | S #126; A joint M6 record | T6 0.45 | 0.55 |
| Scope, limitations and conclusion | 0.75 | Joint | T7 0.50 | 0.25 |
| References and repository evidence index | 0.75 | A assembles; both verify | Compact reference/evidence index 0.50 | 0.25 |
| **Total** | **9.00** | | **4.10** | **4.90** |

One workable A allocation is mapping 1.00, workflow 1.00, references 0.75,
title 0.25, deployment 0.15, validation 0.15, M6 integration 0.10 and scope 0.10:
3.50 pages. S's complementary share is 5.50. Revise paragraph lengths jointly if
rendering needs it, while keeping the section totals unchanged. S drafts F1-F4;
A checks F2 domain states and F3 handoff/ack ordering before publication.

## 1. Title, abstract and problem statement: 0.50 page

Joint: one emergency request/resource scenario, asynchronous matching objective,
three replicated shards, local Kubernetes scope and evidence method. No separate
title page. Distinguish required local deployment from optional parked cloud
work. Do not claim readiness unsupported by the final submitted artifact.

## 2. Requirement/service/API mapping: 1.00 page

A writes public API/matcher interfaces; S writes coordinator/storage mapping and
unavailable/incomplete behavior. T3 maps all seven PS Table 2 operations to the
actual `/api/v1` contracts and the seven ingress aliases. Lists/SSE are extensions.
T4 maps registration, heartbeat and internal data PUT/GET/DELETE to PS Table 3
examples. Fold T2 into a technology/role column. Explain matching's 202 acceptance
versus completed matching, errors/preconditions, PS fields/type mapping, and
which timestamp the server supplies. Keep exact field wording tied to the
original problem statement, not inferred from endpoint examples.

Full mappings: [PS compliance](ps-compliance.md). Full schemas:
[contracts](https://github.com/ParthMalhotra07/ReliefGrid/tree/5965f840f6fcf1e3d1044ed913def8982363fd31/contracts).

## 3. Architecture, sharding, consistency and schemas: 1.50 pages

S leads F1: browser/API/coordinator, three three-voter groups, outbox/relay/
RabbitMQ/matcher path, authority and protocol boundaries. The fixed ring has
128 virtual nodes **per shard**, 384 tokens overall, with ringVersion/checksum.
No online migration claim. F2 shows the five physical tables (`records`,
`operation_results`, `attempt_closures`, `outbox`, `meta`) and logical request/
resource states; JSON bodies are not separate SQL tables. Describe atomic
committed preconditions, deterministic proposer-supplied inputs, PVC Raft history
and rebuilt SQLite.

Contrast Barrier with the quorum/term/completed-FSM fence, explicit diagnostic
stale reads and errors on failed authority. At the planning baseline, default
app calls still use `strong`/Barrier. #166 proposes the linearizable default;
describe it as deployed only after the accepted revision/configuration proves
that. Requested minimum consistency must not be presented as proof of the
executed read algorithm. Details: [architecture](architecture.md).

## 4. Workflow, claims, messaging and recovery method: 1.00 page

A leads prose and F3; S checks consensus/closure safety. Sequence: state/outbox
commit -> mandatory routed publish confirmation -> published marking; intent ->
resource claim -> reciprocal request transition -> durable handoff -> ack.
After an unknown outcome retry the same operationId; only a committed closure
permits retargeting. Historical success does not establish present ownership.
Cover type/location/window matching, quorum queues, manual ack/prefetch and
reconciliation briefly. Keep full protocols in
[workflow/messaging](workflow-and-messaging.md),
[ADR004](../adr/004-attempt-closure.md) and
[ADR005](../adr/005-outbox-handoff.md).

## 5. Docker/Kubernetes deployment and reproducibility: 1.00 page

Joint F4: five-node kind topology, nine storage replicas, PVC versus emptyDir,
Services, app Deployments, probes and policies. S covers storage admission,
offline artifacts and tracing; A covers app images/chart, dashboards and scaling
controls. Explain cached install and source/image binding, fixed profile versus
optional autoscaling/observability. A reproducibility tool or a rehearsal
procedure is not evidence of a completed physical execution. Link
[deployment](deployment-and-demo.md) and
[rehearsals](../operations/rehearsals.md). Cloud remains parked; all workers share
one host, so placement is not independent-machine failure tolerance.

## 6. Validation, gate and fault results with limitations: 1.50 pages

Joint T1: nine PS section 10 rows, command/run ID, acceptance and actual result;
keep pending entries visible. T5 compresses leader/partition/quorum loss,
timeout/closure, partial scan, broker return/redelivery, app death, intact
restart, empty-disk/M7 and stale projection. Link the full scenario list rather
than dropping individual cases. Keep negative controls and their expected
failures distinct from successful fault runs; do not pool different judges/runs
into a new pass total.

S supplies storage/chaos/audit; A supplies app/browser/broker and scaling.
Cover acknowledged-write readback, fresh reads, reciprocal claims, terminal
cleanup and complete clean quiescent audit. Recovery requires caughtUp,
equal-index state, readback and nine UP observations, not only Ready. Retain
qualifications of reconstructed judgments, historical source/images and missing
build bindings. Fixed API2/matcher2 restoration is a separate observation from
the measured autoscaling state.

The approved [#165 scoped gate evidence](https://github.com/ParthMalhotra07/ReliefGrid/pull/165)
records 18/18 course reports, 12/12 chaos, 7/7 Merkle and clean invcheck/collector
at `1623c23`, with app-quiesced chaos, aging 0s, autoscaling off and auth excluded.
It is not acceptance of later forms/auth/linearizable changes or every enhanced
goal. The later final gate must retain its own alias/form checks and deployment
identity. Details: [validation](validation-addendum.md),
[agreed scope](../operations/gate-scope.md).

## 7. Performance and the selected S4.1/M6 experiment: 1.00 page

S supplies #126 measurements; A integrates the jointly chosen S4.1/M6 study.
T6 has three compact blocks: baseline/step/soak offered versus achieved load,
latency/errors/saturation; M6 Barrier/fence medians and entries/read; synthetic
ring movement (24.93%, not live migration). #126 Phase 1 is now merged; use its
accepted raw reports and exact nonzero error rates, not the former draft's
pending label. Check any later Phase 2 comparison against its own immutable run
artifacts before adding its numbers. A message reporting results is not the raw
evidence. The final integrated revision/performance claim remains separately
qualified.

Keep one-host/seed scope, excluded HTTP warm-up, 503 errors, matched-only latency
excluding WAITING, and the distinction between HTTP and sustainable matching
capacity. M6's three replicas share one process over loopback; the study comes
from S4.1. The benchmark output omits runtime Go version; Go 1.27.2 is source-pinned
and recorded separately in its safety run. That study cannot establish system-
load causality. [Results](requirements-and-results.md),
[M6 ADR](../adr/006-read-fence-study.md).

## 8. Scope, limitations and conclusion: 0.75 page

T7 takes 0.50 page; use 0.25 for demonstrated scope, singleton availability
limits, unresolved evidence and conclusion. PS section **12** is the source of
the decisions below; it does not require a twelfth report section. The matrix
has ten rows. Cover Go/net/http and service boundaries in mapping, type/location
matching in workflow, and monitoring/logging/tracing/cloud in deployment.
These rationales are not performance proofs.

| Decision | Alternatives | Why chosen and cost | Evidence |
|---|---|---|---|
| Raft (S) | Primary-backup | Quorum-ordered commits/elections; majority required | [Raft](../../internal/raftstore) |
| xxhash ring, 128 vnodes/shard (S) | Modulo; dynamic placement | Deterministic/versioned placement; fixed membership limits migration | [Ring](../../internal/ring) |
| Deterministic derived SQLite FSM (S) | Mutable primary DB; memory only | Transactional preconditions/replay; rebuild cost | [FSM](../../internal/fsm) |
| Transactional outbox (A) | DB/broker dual write | State/event atomicity; duplicates require dedupe | [ADR005](../adr/005-outbox-handoff.md) |
| Quorum queues (A) | Classic/transient queues | Durable queue semantics; resource cost, singleton broker still limits availability | [MQ](../../internal/mq) |
| Read fence versus Barrier (S) | Barrier each read | Avoids steady-state read log entries; proof complexity | [ADR006](../adr/006-read-fence-study.md) |
| Linearizable default, conditional (S/A) | Stale default; strong only | Fresh ownership decisions, quorum latency/unavailability; verify accepted default and remaining Barrier calls | [Public contract](../../contracts/public.yaml) |
| kind versus cloud (S/A) | Managed cloud Kubernetes | Reproducible required local deployment; one-host limit, cloud optional | [Deployment](deployment-and-demo.md) |
| Open-model load (S) | Fixed closed-loop VUs | Exposes overload; report achieved rates/dropped iterations | [k6](../../loadtest/k6) |
| Merkle audit (S) | Full dumps; row-copy repair | Equal-index comparison localizes divergence; diagnostic, not live repair | [Audit](../operations/merkle-audit.md) |

## 9. References and repository evidence index: 0.75 page

A assembles; both verify PS, final plan (Part 1 precedence), accepted immutable
source, contracts, ADR004-006, selected run manifests/results and repo chapters.
Convert relative repository links into final immutable references for submission;
bind each measurement to its historical source/images rather than one blanket
SHA. Export, render and count **all** pages. Compress if ten or more. Nine pages
is the editing budget, not a measured layout result.

## Overflow routing (drafting metadata; exclude from submission)

- Full fence algorithm/clock/index and M6 samples/causality: ADR006 and
  [retained M6 replay](../evidence/A5.3-read-fence).
- Full timeout/operationId/closure/late-claim method: ADR004 and workflow chapter.
- Detailed failures, coverage rejudgments and per-run controls: validation chapter
  and [S4.3 evidence](../evidence/S4.3). Preserve failed coverage judgments.
- Replacement/admission phases and rebuild detail: architecture chapter and
  [replacement implementation](../../cmd/storagectl/replace.go).
- Merkle equal-index/observation rules: audit guide.
- Complete load setup, CPU/tool/profile/workload metadata, repeat/soak tables,
  queue bounds, raw outputs and unknown/WAITING limitations:
  [S5.2 evidence](../evidence/S5.2), [load methodology](../../loadtest/k6/README.md)
  and results chapter. Keep the HTTP bound separate from sustainable matching.
- Gate, app recovery and rehearsal procedures/results: validation, deployment,
  [evidence index](../evidence) and rehearsals. Procedures remain procedures until
  real executions are retained.

Do not paste earlier S draft section numbering into the nine-page structure.
Detailed material stays in repository docs; neither the outline nor this routing
list establishes final report completion or a current integrated gate.
