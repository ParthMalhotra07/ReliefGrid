# ReliefGrid — S sections, final draft v3

Page allocations follow outline-v2: nine pages inclusive. The S contributions total **5.55 allocated pages**, including their figure and table space. A-led contributions retain the remaining space. These are layout budgets, not measured PDF pages.

The appendix supplies drawing instructions for F1–F4. It is editorial material outside the submission budget; the drawn figures occupy their existing slots. The gate, #167 chaos campaign and Phase 2 study remain separate observations.

Repository links use `{FINAL_SHA}` for submission re-pinning. Historical measurement revisions remain in the prose and evidence manifests. The Phase 2 link remains `TODO-after-#166/Phase2-PR`. The plan PDF is untracked and is omitted from the references.

## Title, abstract and problem statement

**Section budget: 0.50 page. S contribution: 0.20 page; joint title/abstract integration: 0.30 page.**

**ReliefGrid: replicated storage and asynchronous emergency-resource matching**

An emergency request may remain unmatched until a compatible resource becomes available. ReliefGrid separates accepting that request durably from completing its assignment. Three storage shards each use a three-voter Raft group to order state changes. Asynchronous workers match resources through durable intents and fenced claims. The local Kubernetes evaluation combines acknowledged-write ledgers, authoritative reads, invariant checks, replica audits and controlled faults. A repeated before/after study moves the validated HTTP saturation bound from 50 to above 80 requests/s. Committed mutation rules remain unchanged. These results describe a bounded local deployment, with background matching capacity and singleton availability reported separately.

## Requirement/service/API mapping

**Section budget: 1.00 page. S contribution: 0.30 page, including T4 (0.20); A interfaces/T3 and shared stack column: 0.70 page.**

The API and matcher access storage through the coordinator; they never mutate SQLite directly. The coordinator selects the fixed logical owner and discovers its leader. The committed state machine (FSM) decides whether each mutation is legal. A successful submission establishes durable acceptance, not a completed match. Internal calls preserve operationId, expected version and routing metadata across retries.

**T4 — Internal service mapping and failure semantics.** Technology roles occupy the T2 column within the mapping rather than a separate table.

| PS internal function | Implemented interface | Authority / technology role |
|---|---|---|
| Registration; heartbeat | POST `/internal/nodes/register`; POST `/internal/nodes/heartbeat` | Coordinator discovery and health; Go HTTP |
| Data PUT / GET / DELETE | `/internal/data/{key}` | Coordinator routes; Raft/SQLite commit writes and fence fresh reads |
| Workflow extensions | Claim, transition, close-attempt, operation lookup | Committed domain and deduplication rules |
| Enumeration / publication | Scan; outbox read and published marking | Complete-shard results; durable event handoff |

Failed CAS returns a precondition failure; committed domain conflicts remain distinct from unavailable authority. A failed or partial shard scan cannot establish absence. Mutation uncertainty remains explicit even when a later routing refusal occurs. Exact schemas and endpoint extensions are retained in the [internal contract][internal] and [architecture chapter][architecture].

## Architecture, sharding, consistency and schemas

**Section budget and S use: 1.50 pages: F1 0.35, F2 0.35, prose/citations 0.80. A checks logical domain states.**

**F1 — System architecture.** Diagram specification in the appendix; draw within the allocated figure slot.

Placement uses xxhash64 and 128 virtual nodes per shard. Keys select a logical shard from a fixed ring carrying ringVersion and checksum. Transient replica failures do not remap ownership. Leader discovery changes the destination replica within that shard. The coordinator is a routing and discovery dependency, not another consensus group. The implementation supports placement simulation, but does not claim online migration.

Each command checks deduplication, expected version, state and attempt rules inside one committed SQLite transaction. That transaction records the entity change, historical result, event and completed command index together. Proposers supply identifiers and integer timestamps; apply uses no wall-clock, randomness or unordered decisions. A resource has at most one active claim. A request has at most one live claim. Live assignments require reciprocal ownership. Terminal requests retain no live claims.

**F2 — Physical schema and logical states.** Diagram specification in the appendix; draw within the allocated figure slot.

Raft log/stable state and snapshots live on PVCs. SQLite uses WAL and `synchronous=OFF` ([`internal/fsm/fsm.go:146`][sqlite-config]). This is safe because SQLite is derived and rebuilt from the durable Raft log and snapshots on startup.

The rebuilt file lives in emptyDir. Startup reconstruction remains a cost.

`strong` and `linearizable` provide fresh reads through different mechanisms. A **barrier** is a committed log marker that makes a read wait behind earlier work. A **read fence** checks current leader authority with a majority and waits until SQLite contains the required committed state.

The fence first establishes a current-term commit and captures a read index. It requires quorum evidence sent after the read began. It then rechecks leadership and term, and waits for SQLite to complete the corresponding command. Queued Raft apply progress and old heartbeat replies are insufficient. Authority failure returns an error. Explicit `stale` reads have separate semantics. [ADR006][m6].

The initial term gate can still require a barrier.

Phase 1 (`7dd1940`) defaulted to `strong`; the measured Phase 2 revision (`514bb33`) defaulted to `linearizable`. Explicit `strong` callers retain Barrier behavior, including the post-load invariant audit. Linearizable reads retain the freshness check. Committed apply still checks compare-and-swap (CAS) on version and TryClaim rules. A fresh read never grants an unchecked future write.

## Workflow, claims, messaging and recovery method

**Section budget: 1.00 page. S contribution: 0.30 page; A prose and shared F3: 0.70 page. F3 occupies 0.40 page within the joint section.**

**F3 — Commit/publish/claim/ack sequence.** Diagram specification in the appendix; draw within the allocated figure slot.

A timeout after transmission may conceal a committed command. It preserves the intent rather than authorizing another claim. Repeating the same operationId resolves the historical command through deduplication; present ownership still requires authoritative state. CloseAttempt durably closes the attempt, even if its claim is delayed. It releases only that attempt’s claim and prevents its later recreation.

The #168 fix preserves uncertainty across routing retries. After an unknown attempt, a later 4xx without a committed `Result` returns 503 OUTCOME_UNKNOWN. A 4xx carrying the committed FSM result, or a successful response, resolves the command; a first definite 4xx remains definite. 

The linked PR records this fix. It was not certified by the earlier gate. [#168][unknown].

## Docker/Kubernetes deployment and reproducibility

**Section budget: 1.00 page. S contribution: 0.55 page including F4 (0.35); A app/metrics contribution: 0.45 page.**

**F4 — Placement and persistence.** Diagram specification in the appendix; draw within the allocated figure slot.

An intact-disk restart reconstructs SQLite from persisted history. An empty disk stays unadmitted: pods never self-bootstrap Raft. Guarded replacement removes the old membership and admits the new incarnation through recorded stages before opening data/Raft service. Readiness alone does not establish recovery; caught-up membership, completed state and successful read-back must agree.

Reproducibility requires cached dependencies/images, installation manifests and source-to-image binding, not merely a successful archive. The 12 November gate used development images and did not establish runtime build binding. 

Its fixed profile was API 2/matcher 2, autoscaling off and aging interval 0s. Optional tracing propagates context across HTTP, outbox/AMQP and Raft apply through Collector/Jaeger; metrics use a separate restricted listener. Offline procedures and rehearsal requirements remain in the [deployment record][deployment] and [rehearsal guide][rehearsals].

## Validation, gate and fault results with limitations

**Section budget: 1.50 pages. S contribution: 0.95 page including T5 (0.35); joint T1 and A results: 0.55 page. T1 occupies 0.30 page within that shared space.**

The 12 November scoped enhanced gate recorded **18/18 course demo outcomes**, covering two rounds of nine demonstrations. Replacement passed 63/63 checks with 1,794 acknowledged writes and none lost. With the app quiesced, chaos passed **12/12 runs, 22/22 checks**. The ten experiments reported no lost acknowledged writes, unresolved unknowns or stale reads. Both negative controls detected their faults. Merkle passed **7/7**, with **216 acknowledged writes**, none lost. All three replicas of every shard had equal roots ([`docs/evidence/gate-20261112T100532Z/report.md:58`][merkle-count]). Strict invcheck was clean and quiescent. [Gate report and retained evidence][gate].

**T1 — Nine PS demonstration outcomes (joint slot, 0.30 page).** Demo 1–9: PASS in each of rounds 1 and 2; acceptance is the individual demo's assertions after each clean reset. The retained [gate matrix][gate-matrix] supplies the nine named rows, commands and per-round evidence. These 18 outcomes do not certify every enhanced-gate scope item.

**T5 — Faults, safe behavior and evidence (0.35 page).**

| Fault | Required safe behavior | Evidence |
|---|---|---|
| Leader/partition/quorum loss | Fresh authority or unavailable; no lost acknowledged write | [Chaos campaign][chaos] |
| Timeout / later routing refusal | Same operationId; unknown persists; closure before retarget | [#168][unknown], [ADR004][closure] |
| Partial shard scan | Incomplete/error, never empty-by-default | [Internal contract][internal] |
| Broker return / redelivery / app death | Retain durable work; confirm and handoff before ack | [Workflow][workflow], [gate][gate] |
| Intact restart / empty disk | Rebuild from PVC / explicit M7 admission | [Replacement record][gate] |
| Replica lag / stale projection | Raft-level lag plus equal completed-FSM state and fresh reads | [#167 campaign][chaos] |

The later #167 campaign ran with API 2/matcher 2/UI 1 active and passed all 22 checks. It corrects lag to `max(0, leaderCommitIndex − followerRaftAppliedIndex)`. Subtracting the last FSM command index incorrectly counts barriers, no-ops and configuration entries as missing replication. In the shard 2 idle sample, leader commit advanced 86174→86237 while every FSM stayed at 85904. The old formula produced **333 entries of false lag; the Raft-level value was 0**. Under an actual delay, lag rose to 51 and settled to zero; the stale-reader control detected 514 stale reads. [#167 evidence][chaos].

Raft-level progress does not prove SQLite completion. Settle requires a caught-up group with equal completed-FSM indexes. Recovery also checks acknowledged ledgers, missed reads and all nine members UP. Unreadable status produces no sample, never zero. Merkle compares complete replica state at equal command indexes; it diagnoses divergence without proving the whole read protocol or repairing live rows.

The scoped gate excludes positive-interval aging, HPA/KEDA scaling acceptance and internal-auth rollout.

Its chaos run quiesced the app; #167 kept the app running. Neither campaign establishes chaos safety under measured high load.

## Performance and the selected S4.1/M6 experiment

**Section budget and S use: 1.00 page: T6 0.45, prose/citations 0.55. A integrates the joint M6 record.**

**T6 — Load comparison, selected M6 study and placement scope (0.45 page).** Load values are medians of three repeated runs per phase; baselines offer 25 req/s. Step runs offer 10→80 req/s. The saturation bound concerns HTTP thresholds, not sustained matching completion.

| System-load measure | Phase 1: strong default | Phase 2: linearizable default |
|---|---:|---:|
| Validated saturation result, n=3 | 50 req/s; write p99 fails at 60 | **>80 req/s**; no measured step fails |
| Baseline write p99 | 684 ms | 94 ms |
| Baseline read / list p99 | 80 / 291 ms | 56 / 90 ms |
| Step-run barriers, approximately | 270k | 0 |
| Step-run broker refusals, global | 6,324 | 8 |
| Write p99 at offered 80 req/s | 1,180 ms | 389 ms |
| Exploratory Phase 2, n=1 | — | **≥110 req/s**, request-path indication only; audit incomplete; generator-limited above |

| M6 loopback medians | Barrier | Fence |
|---|---:|---:|
| Serial / parallel ns per operation | 449,566 / 112,730 | 126,084 / 23,335 |
| Steady-state entries/read | 1 | 0 |

M6 source: `docs/evidence/A5.3-read-fence/summary.json`. Serial medians: [file:11][m6-serial-barrier] (449,566) and [file:16][m6-serial-fence] (126,084). Parallel medians: [file:26][m6-parallel-barrier] (112,730) and [file:31][m6-parallel-fence] (23,335).

| Placement simulation | Result | Scope |
|---|---|---|
| Three→four shards | 24.93% of 10,000 keys move | Synthetic; no live migration |

The comparison used seed 52 and fresh kind per run. Cluster CPUs were 0–5; generator CPUs were 6–7. Pods used Guaranteed resources and fixed API2/matcher2. Each step has a 10 s ramp and 110 s hold; baseline HTTP warm-up is excluded. Phase 2 repeats three baselines and three step runs, with clean quiescent checks and passing audits. It adds no soak. Its initial report labelled an unbroken range INCONCLUSIVE; the retained acceptance and revised check correctly report a lower bound. Full achieved rates, errors, ranges, manifests and deviations remain in the [comparison][performance].

The roughly 270k→0 barrier reduction removes read traffic from the committed log. Phase 2's residual 29 counter-difference entries are already-applied commands at the scrape boundary, not barriers. The coordinator is the next observed limit. From 50 req/s upward, it uses around 98% of its 250m CPU allowance. It is **87–95% throttled**, measured as the share of CPU quota periods (`docs/evidence/S5.2/phase2-linearizable/README.md:92–93`; publication: [pending][performance]). Background forwarding increases as work completes faster; this identifies pressure without proving a sole bottleneck.

The wider exploration met request-path thresholds through offered 110 req/s, achieving about 108.56 req/s. Broker pressure was already substantial.

The audit handoff drain failed; invcheck never ran. Thus ≥110 is exploratory, not an audited capacity claim.

Above 110, generator memory pressure and dropped iterations prevent a system-saturation verdict.

M6 uses five samples per mode on three loopback replicas. [ADR006][m6]; [placement result][simulation].

## Scope, limitations and conclusion

**Section budget: 0.75 page. S contribution: 0.50 page; A messaging and joint conclusion integration: 0.25 page. T7 occupies 0.50 page in the joint section.**

**T7 — S rows of the PS §12 decision matrix.** A's outbox/quorum-queue rows share this slot; service/language, matching/routing and observability rationales appear in the mapping, workflow and deployment sections.

| Decision | Alternatives | Why chosen | Evidence |
|---|---|---|---|
| Raft | Primary-backup | Quorum-ordered writes/elections; majority required | [Architecture][architecture] |
| xxhash, 128 vnodes | Modulo; dynamic placement | Stable versioned ownership; migration absent | [Ring study][simulation] |
| Derived SQLite FSM | Primary DB; memory only | Transactional preconditions and replay; rebuild cost | [Architecture][architecture] |
| Read fence | Barrier/read | Fresh authority without steady-state read entries; proof complexity | [ADR006][m6] |
| Linearizable default | Stale; explicit strong only | Fresh reads plus committed CAS/TryClaim; quorum dependency | [Phase 2][performance] |
| kind | Managed cloud | Repeatable required local scope; single-host limits | [Deployment][deployment] |
| Open-model load | Fixed closed-loop clients | Exposes offered/achieved gap and dropped work | [Load record][performance] |
| Merkle audit | Full transfer; row-copy repair | Equal-index localization; diagnostic only | [Audit guide][merkle] |

The load comparison uses one host and one seed. Nonzero errors and near-full queues constrain generalization. Matched-only latency excludes WAITING requests and includes resource availability. M6 supports the read-cost mechanism, not whole-system causal proof.

ReliefGrid demonstrates durable local matching with explicit uncertainty, committed claim fences and independently checked recovery. The repeated read-path comparison improves the validated HTTP bound while retaining mutation safety. Coordinator and broker singleton availability, fixed membership, unfinished exploratory auditing and the gate's excluded scopes bound the result; cloud readiness and sustainable high-rate matching are not established.

## References and repository evidence index

**Section budget: 0.75 page. S contribution: 0.25 page; A assembly and remaining references: 0.50 page.**

The [architecture][architecture], [internal contracts][internal], [ADR004][closure], [ADR006][m6] and [deployment chapter][deployment] define storage authority, closure, reads and deployment. Measurements bind to their own historical revisions: the [12 November gate][gate], [#167 campaign][chaos] and [Phase 2 comparison][performance] are distinct evidence sets. The [Merkle guide][merkle] defines audit completeness. PR [#168][unknown] documents the later unknown-outcome fix. The exploratory ≥110 req/s observation comes from the Phase 2 draft report. Its audit did not complete, and no repository report was committed.

[architecture]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/report/architecture.md
[internal]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/contracts/internal.yaml
[closure]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/adr/004-attempt-closure.md
[workflow]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/report/workflow-and-messaging.md
[deployment]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/report/deployment-and-demo.md
[rehearsals]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/operations/rehearsals.md
[m6]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/adr/006-read-fence-study.md
[merkle]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/operations/merkle-audit.md
[simulation]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/contracts/internal.yaml#L950
[gate]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/evidence/gate-20261112T100532Z/report.md
[gate-matrix]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/evidence/gate-20261112T100532Z/gate.txt
[chaos]: https://github.com/ParthMalhotra07/ReliefGrid/tree/{FINAL_SHA}/docs/evidence/S4.3/suite-20261112T155808Z-hrishabh-sharrma-ThinkPad-E14-Gen-7
[unknown]: https://github.com/ParthMalhotra07/ReliefGrid/pull/168
[performance]: TODO-after-#166/Phase2-PR

[sqlite-config]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/internal/fsm/fsm.go#L146
[merkle-count]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/evidence/gate-20261112T100532Z/report.md#L58
[m6-serial-barrier]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/evidence/A5.3-read-fence/summary.json#L11
[m6-serial-fence]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/evidence/A5.3-read-fence/summary.json#L16
[m6-parallel-barrier]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/evidence/A5.3-read-fence/summary.json#L26
[m6-parallel-fence]: https://github.com/ParthMalhotra07/ReliefGrid/blob/{FINAL_SHA}/docs/evidence/A5.3-read-fence/summary.json#L31

## Appendix — diagram specifications for F1–F4

**Editorial drawing appendix; no additional submission pages allocated.** Use Mermaid flowcharts with the node IDs below. Figure space remains in the main sections.

### F1 — System architecture; 0.35 page

- Direction: left to right.
- Boxes: `browser` Browser; `api` API; `coord` Coordinator; `relay` Outbox relay; `broker` RabbitMQ; `matcher` Matcher/reconcilers.
- Shard subgraphs: `s0`, `s1`, `s2`. Each contains three voter boxes, labelled leader/follower roles as illustrative.
- Within each shard: label “Raft-ordered commands → deterministic SQLite FSM”. Label logical outbox storage inside the shard boundary.
- Arrows: browser → api (“HTTP”); api → coord (“internal HTTP”); matcher → coord (“claims/transitions”).
- Arrows: coord → each shard (“fixed ring owner; leader discovery”); connect voters with “Raft replication”.
- Arrows: shard outboxes → relay (“committed events”); relay → broker (“mandatory publish + confirm”); broker → matcher (“delivery; manual ack”).
- Boundary labels: “application”, “routing/discovery”, “per-shard consensus authority”, “broker handoff”.
- Caption: The broker schedules work; shard consensus authorizes state changes. Management and read-only observability use separate interfaces.

### F2 — Physical schema and logical states; 0.35 page

- Direction: top to bottom; two subgraphs, “Physical SQLite tables” and “Logical entity states”.
- Table boxes: `records` (“key, kind, version, JSON body, tombstone”); `results` operation_results (“operationId, payload hash, historical result”).
- Other boxes: `closures` attempt_closures (“resource + attempt fence”); `outbox` (“ordered publication work”); `meta` (“schema + completed-command index”).
- A surrounding label says “one committed apply transaction”. These boxes describe stored data, not foreign-key constraints.
- Dashed arrows: records → request/resource state subgraphs (“JSON state, not separate SQL tables”).
- Request boxes: WAITING, MATCHED, IN_PROGRESS, RETRYING, terminal states. Arrows: WAITING → MATCHED → IN_PROGRESS → terminal.
- Recovery arrows: live attempt → RETRYING (“resolve or close attempt”); RETRYING → WAITING (“committed closure before retarget”).
- Resource boxes: available, attempt-bound claim. Arrows: available → claim (“TryClaim”); claim → available (“exact-attempt release”).
- Dashed link: MATCHED/IN_PROGRESS ↔ claim (“reciprocal ownership”). Terminal label: “no live claim”.
- Caption: Historical operation success records a past command; it does not establish current ownership.

### F3 — Commit/publish/claim/ack sequence; 0.40 page

- Direction: left to right, with a lower retry branch.
- Boxes: `commit` Request + outbox commit; `publish` Mandatory routed publish; `confirm` Broker confirm; `mark` Publication marking.
- Main publication arrows: commit → publish → confirm → mark. Label “retain outbox work” on the publish failure loop.
- Matching boxes: `intent` Durable intent; `claim` Resource TryClaim; `reciprocal` Reciprocal request transition; `handoff` Durable handoff; `ack` Delivery ack.
- Main matching arrows: broker delivery → intent → claim → reciprocal → handoff → ack.
- Decision diamond: `unknown` “Mutation outcome unknown?”. Retry arrow → operation lookup/same operationId → matching stage being resolved.
- Retarget boxes: `close` Committed CloseAttempt; `retarget` New attempt. Arrow: unknown/recovery → close → retarget → intent.
- Labels: “never infer rejection from timeout”; “delayed claims cannot reopen a closed attempt”; “ack only after durable handoff”.
- Caption: #168 keeps an earlier unknown outcome through a later 4xx without a committed Result.

### F4 — Placement and persistence; 0.35 page

- Direction: top to bottom. Outer subgraph: “One local host”; inner subgraph: “Five-node kind cluster”.
- Node boxes: one control plane, data worker 1, data worker 2, data worker 3, infra worker.
- Each data-worker box contains one voter from each of shards 0, 1 and 2. Label “three replicas per shard on distinct workers”.
- Infra boxes identify coordinator and infrastructure services. Show API/matcher Deployments as a separate workload group with chart-controlled placement.
- Storage detail inset: `pvc` PVC (“Raft log, stable store, snapshots”) → `fsm` emptyDir SQLite (“rebuild at startup”).
- Admission decision: intact history → rebuild; empty disk → unadmitted → guarded replacement → admitted data/Raft listeners.
- Interface labels: Raft, data HTTP, operator management, restricted metrics. Connect optional Collector/Jaeger through a dashed “traces” arrow.
- Caption: Worker separation supports local node-fault experiments; all workers still share one physical host.

