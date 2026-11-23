# ReliefGrid: replicated storage and asynchronous emergency-resource matching

Parth Malhotra and Hrishabh Sharrma

Source at submission: 1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9. Historical runs keep their own recorded revisions.

## 1. Abstract

In an emergency, a request for help (an ambulance, food, shelter) often cannot be matched straight away, because no suitable resource is free yet. ReliefGrid is a distributed system that accepts such requests safely and keeps trying to match them as resources become available.

It has two application services, an HTTP API and an asynchronous matcher, on top of a storage layer of three shards. Each shard is copied on three replicas that agree on every change using the Raft consensus protocol. Changes are handed to RabbitMQ through a transactional outbox, so a committed change is never lost between the database and the queue.

We deployed it on a local Kubernetes cluster (kind) and tested it with scripted demos, fault injection and load tests. In the final gate, all 9 required demos passed twice, and the chaos tests lost no acknowledged writes. Load testing found that every read was adding an entry to the Raft log. Fixing this raised the load the system handles before slowing down from 50 to over 80 requests per second, and made writes about 7 times faster (p99 684 ms to 94 ms).

## Introduction

The problem statement asks for a system that matches emergency requests to available resources, built from microservices, a coordinator, a message queue and sharded storage with replicas. The hard part is not the matching itself. It is what happens when things fail: a server crashes in the middle of a write, the network splits, or a message is delivered twice.

Our main goal was that the system never gives a wrong answer, even during failures. In practice that means:

- a write that the system confirmed is never lost;
- the same resource is never given to two requests;
- if the system is not sure whether a write happened, it says "unknown" and retries the same operation, instead of guessing.

The rest of this report explains how the system is built (sections 3 to 5), how we worked and tested it (methodology and section 6), what the load tests showed (section 7), and the decisions we made and why (section 8).

## 2. Requirement, service and API mapping

The API and matcher are the two application microservices; coordinator/storage provide the distributed data service. They never write SQLite directly. Go/net/http serves public and internal HTTP; Raft orders commands, SQLite implements the FSM, RabbitMQ schedules work, and Helm/kind provide deployment. The UI presents live or explicitly saved observations.

T3. PS Table 2 client operations. The canonical base is /api/v1; method/path-specific Traefik aliases reach the same handlers and preserve query strings and the idempotency namespace. Named UI routes are not intercepted. [1-3]

| Operation | Method and alias | Normal response |
|---|---|---|
| Submit request | POST /requests | 201 and canonical Location |
| Request details | GET /requests/{id} | 200 |
| Request status | PUT /requests/{id}/status | 200 |
| Register resource | POST /resources | 201 and canonical Location |
| List resources | GET /resources | 200 |
| Resource status | PUT /resources/{id}/status | 200 |
| Match request | POST /match/{requestId} | 200; 202 while deferred/running |

PS id fields map to requestId/resourceId, types to requestType/resourceType, creation time to server-persisted createdAt, and availability to windowStart/windowEnd plus status and absence of an active claim. A match 202 is not proof of assignment: clients read current request state. Lists/SSE extend the seven required operations; no bare /events alias is added.

## Internal mapping and client semantics

T4/T2. Internal functions and technology roles. The coordinator selects a fixed logical owner and discovers its leader; committed apply decides legality. [3-4]

| PS internal function | Interface | Authority / role |
|---|---|---|
| Registration; heartbeat | POST /internal/nodes/register; /internal/nodes/heartbeat | Discovery/health; Go HTTP |
| Data PUT / GET / DELETE | /internal/data/{key} | Coordinator routes; Raft/FSM commit |
| Workflow extensions | Claim, transition, close-attempt, operation lookup | Domain rules and durable dedupe |
| Enumeration/publication | Scan; outbox read and published marking | Complete-shard reads; durable handoff |

Keyed creates freeze canonical payload, identifiers, timestamps and original response across retries; changed-payload key reuse is rejected. Unknown outcomes retain the same identity. ETags/If-Match protect conditional updates. Cursors bind query/ring state; pages are not a global snapshot. A failed/partial shard scan cannot establish absence. Current ownership always requires authoritative state, not a historical successful operation result.

## Methodology

We split the design into named goals and built each goal on its own branch, merged through a pull request. Every pull request was reviewed by the other person before merging, and the reviews often found real bugs.

For most changes we wrote a failing test first, then the fix. For important checks we also did mutation testing: we broke the code on purpose and made sure a test failed. This showed that our tests actually catch the bugs they are meant to catch.

Bigger claims were checked by scripted runs on a fresh cluster: a "gate" that runs all 9 required demos twice, plus replacement, chaos, audit and auth checks. Every run keeps its logs and results with checksums in the repository, so each result in this report can be traced to a run. When a run failed, we kept it in the evidence, found the cause, fixed it and ran it again.

## 3. Architecture, sharding and consistency

Placement uses xxhash64 and 128 virtual nodes per shard, 384 tokens overall. The fixed ring carries ringVersion/checksum. Replica failure does not remap keys; leader discovery changes the destination within the owning shard. The coordinator prefers replicas with fresher heartbeats and returns 503 immediately when all registered replicas for a shard are marked DOWN. It can try a replica with an expired heartbeat during failover, while Raft still determines whether the replica can serve the operation. The PS gives hash mod 3 as an example; the ring reduces remapping when adding a shard. Simulation studies placement, not online data migration. The single coordinator is a routing dependency, not another consensus group. [3-4]

![F1. System architecture](figures/F1-print.svg)

F1. The broker schedules work; shard consensus authorizes changes. Every shard has one illustrative leader and two followers, each with its own deterministic FSM/outbox. Management and read-only observability use separate interfaces. Roles change on election; ownership does not. Broker confirms and delivery acknowledgements return opposite to publication/delivery arrows.

## Durable state, schema and reads

Raft log/stable state and completed snapshots live on PVCs. SQLite is derived, uses WAL/synchronous=OFF and is rebuilt in emptyDir from durable Raft history. SQLite commit is the local visibility boundary, not the independent durability source. The guarantee depends on retained quorum history and the disk/filesystem failure model; local volumes are not off-machine backup.

Since #166, application reads default to linearizable via the read fence. Explicit strong reads still use a Raft barrier. The fence establishes an own-term commit, captures a read index, requires fresh majority evidence, rechecks term/leadership and waits for SQLite completion. Queued Raft apply and old acknowledgements are insufficient. The first term gate may still append a barrier. Stale reads are separate diagnostics. Application callers refuse missing/unknown authoritative served labels. The earlier Phase 1 and Phase 2 runs retain their own measured revisions; their results do not certify this assembled source as a newly load-tested deployment. [8]

Each command checks operationId deduplication, expected version, state and attempt rules in one committed SQLite transaction. Entity change, historical result, outbox event and completed-command index are recorded together. Proposers supply identifiers and integer timestamps; apply samples no wall clock/randomness and makes no unordered decisions. Resource and request claims are unique; live assignments require reciprocity and terminal requests hold no live claim. [4]

![F2. Physical schema and logical states](figures/F2-print.svg)

F2. Five physical tables participate in committed apply; their layout does not imply foreign keys. Logical request/resource states live in records JSON. The abbreviated state path includes MATCHING before MATCHED, and RETRYING is reached from MATCHING, not MATCHED/IN_PROGRESS. Cleanup reaches the corresponding terminal state only after closure. Some legal edges/dispositions are omitted; the committed FSM transition table is authoritative.


## 4. Workflow, claims, messaging and recovery

Creation commits state and its outbox event together. Relays read bounded per-shard batches, publish persistent messages with mandatory routing and confirms, then mark only routed confirmations. A crash after confirm and before marking can duplicate publication, so processing is idempotent. This avoids a database/broker dual-write gap without exactly-once transport. [4]

![F3. Commit, publish, claim and acknowledge](figures/F3-print.svg)

F3. Publication and matching are separate asynchronous paths. Confirmation precedes publication marking; ack follows the durable handoff. A timeout is not rejection. Only committed closure authorizes retargeting; delayed claims cannot reopen a closed attempt.

The matcher authoritatively reads the request and uses type/location/window filters for candidate hints. It records a MATCHING intent binding request, resource, attempt and fingerprint before TryClaim. The resource shard checks eligibility and closure in committed apply; reciprocal request confirmation checks current version/attempt. A fresh read never authorizes an unchecked future write.

Unknown results retain intent and operationId. A lookup/retry resolves the historical command, not present ownership. RETRYING fences confirmation until the old attempt closes. Cancellation and fulfilment remain CANCELLING/FULFILLING until closure and cleanup complete. Reusable resources may return AVAILABLE; consumed resources remain CONSUMED and expired windows become OFFLINE. CloseAttempt releases only the bound claim.

Consumers use manual ack and prefetch 8, acknowledging only after the required durable transition/retry handoff. Retry queues have explicit dead-letter routing back to work; transient outages do not justify immediate nack loops. Reconcilers page bounded scans and resume intermediate states through the same rules. Recovery assumes eventual quorum/broker/worker availability; it is not a cross-shard transaction.

#168 preserves an earlier unknown through a later result-less routing 4xx. A verified committed result can resolve it; a pre-handler or malformed authentication refusal cannot. Durable callers preserve intent across process replacement and retry the same command. The deployed broker is a singleton even though its queues use the quorum type, so broker outage remains an availability limit. [4,6]

## 5. Docker/Kubernetes deployment and reproducibility

The supported target is local kind: one control plane, three data workers and one infra worker. The standalone storage Helm release owns coordinator, nine voters, PVCs, policies and bootstrap/admission tooling. The app release installs API/matcher/UI, RabbitMQ and ingress over it with storage.enabled=false, ensuring one storage owner. Multi-stage images run nonroot binaries. [4,9]

T8. Implementation and deployment pins at the declared report source. Optional observability tools are separate from the measured load profile. modernc names the Go driver, not the embedded SQLite engine release. See go.mod, tools.mk, UI package/lock files and Helm values. [2]

| Component | Technology and version | Role |
|---|---|---|
| Services | Go 1.27.2 | API, matcher, coordinator and storage |
| Consensus/log | hashicorp/raft 1.8.0; raft-boltdb/v2 2.4.2 | Replicated order and durable Raft state |
| State machine | modernc.org/sqlite 1.60.1 | Transactional, replayable derived state |
| Messaging | RabbitMQ 4.3.7 | Durable queues, routed confirmed publication |
| Local cluster | Kubernetes 1.37.0; kind 0.33.0 | Five-node local deployment |
| Packaging | Helm 3.22.1 | Installation and configuration |
| Ingress | Traefik 3.7.16; chart 42.1.2 | Public HTTP routing |
| Metrics | Prometheus 3.16.0; Grafana 13.3.0 | Scraping, dashboards and alerts |
| Tracing | OpenTelemetry Go 1.47.0; Collector 0.164.0; Jaeger 2.22.0 | Optional distributed traces |
| Load | k6 2.3.0 | Scripted HTTP load campaigns |
| Fault injection | Chaos Mesh 2.8.7 | Controlled Kubernetes faults |
| UI | React/React DOM 19.3.1; Leaflet 1.9.4; esbuild 0.28.2; Node.js 24.16.0 | Interface, maps and build tooling |

![F4. Placement and persistence](figures/F4-print.svg)

F4. Each data worker hosts one voter per shard. All five Kubernetes nodes share one physical host: this is not zone/host independence. PVC history survives an intact restart; an empty disk stays unadmitted until guarded replacement. Interfaces for Raft, data, management and metrics remain distinct.

Install pinned prerequisites and run make doctor. make dev creates/selects kind but is not a complete application install; make demo-reset installs the clean storage/app profile. make app-deploy upgrades the app over existing storage. Demos 6 and 7 are a coupled recovery sequence; run 7 immediately after 6. Follow the recovery runbook if interrupted. make gate recreates the selected disposable cluster twice by default. make cluster-down removes it. Never self-bootstrap an empty voter to make a demo pass.

Readiness drains before listener shutdown; consumers finish admitted durable handoffs. Metrics use bounded labels/private scrape access, with Grafana dashboards. Optional tracing propagates context across HTTP, outbox/AMQP and apply via Collector/Jaeger. Fixed replicas are the default; API HPA and matcher KEDA are separately verified options. Internal auth defaults off; accepting code does not establish live enablement/rotation.

Reproducibility requires cached dependencies/images, installation manifests and source/image binding, not just an archive command. Retained offline 9/9 reports certify their original kit, not a new final cohort. Cloud is parked. Final clean-clone/offline execution, swapped-presenter rehearsals and backup recording/playback retain their own acceptance records. [9]

## 6. Validation: course and scoped gate

The 12 November scoped gate used source 1623c23, fixed API2/matcher2, aging 0s, autoscaling off and auth excluded. It records course 18/18, replacement 63/63 with 1,794 acknowledged writes and none lost, app-quiesced chaos 12/12 runs (22 checks), Merkle 7/7 with 216 acknowledged writes and none lost, and clean/quiescent strict invcheck. These are recorded observations, not universal losslessness. [5]

T1. Nine PS demonstration outcomes. Each row passed in both clean rounds; acceptance is its retained assertions, not the title alone. Reset wrappers took 107/116 s on the recorded host.

| Demo / command | Required observation | R1 / R2 |
|---|---|---|
| 1 / make demo-1 | Create/read request | PASS / PASS |
| 2 / make demo-2 | Resource availability | PASS / PASS |
| 3 / make demo-3 | Type/location/window match | PASS / PASS |
| 4 / make demo-4 | Three-shard distribution | PASS / PASS |
| 5 / make demo-5 | Asynchronous burst | PASS / PASS |
| 6 / make demo-6 | Leader failure/write read-back | PASS / PASS |
| 7 / make demo-7 | Restart/replay/catch-up | PASS / PASS |
| 8 / make demo-8 | App death/interrupted calls | PASS / PASS |
| 9 / make demo-9 | Manual API scaling | PASS / PASS |

T5. Fault behavior and evidence boundaries. Ten chaos faults and two deliberate judge controls are not twelve ordinary production faults. [5-6]

| Fault | Required safe behavior | Evidence |
|---|---|---|
| Leader/partition/quorum loss | Fresh authority or unavailable | Gate; later #167 |
| Timeout / routing refusal | Same identity; uncertainty/closure retained | #168; ADR004 |
| Partial scan | Error/incomplete, never empty | Contract; scan tests |
| Broker return/redelivery/app death | Retain work; confirm/handoff before ack | ADR005; gate |
| Restart/empty disk | PVC rebuild / explicit M7 admission | Replacement/gate |
| Lag/stale projection | Raft progress plus completed-FSM/fresh reads | #167; UI tests |

The scoped gate's collector records fixed 2/2 replicas with no HPA/ScaledObject, but runtime build binding is NOT_ESTABLISHED for dev images. Some raw logs/screenshots remain outside the repository, listed in the report. Alias routes were deployed but not exercised by that gate; later forms were not part of its source. Final aliases/forms and excluded enhancements need their own accepted run.

## Later campaigns and application checks

The later #167 campaign kept API2/matcher2/UI1 active and passed 22 checks, distinct from the app-quiesced gate. Replication lag is max(0, leaderCommitIndex - followerRaftAppliedIndex), not commit minus last FSM command: barriers/no-ops/configuration entries do not advance that SQL counter. Its idle sample advanced commit 86174 to 86237 with FSM 85904, producing 333 false entries under the old formula and zero Raft-level lag. A real delay reached 51 then settled to zero; the stale-reader control detected 514 stale reads. Neither run establishes chaos safety under measured high load. [6]

Raft progress is not SQLite completion. Settle requires caught-up groups, equal completed-FSM indexes, acknowledged-ledger read-back and all nine members UP. Unreadable status is no sample, never zero. Merkle compares complete replicas at equal command indexes; it diagnoses divergence without proving the read protocol or repairing live rows.

Application evidence includes real-FSM adversarial schedules and retained kind/broker/browser tests. A historical scaling run records 637 aggregate CANCELLED/no-assigned-resource checks, not a per-request response ledger. Dirty source/missing immutable build binding limits it to recorded images; its final fixed-profile snapshot was unverified. Newer scoped-gate snapshots do not retroactively fill that gap. UI forms preserve key/body across reentry/unknown replies and require a current read: v3 cannot be confirmed by v2. Saved/offline state is labelled and offline writes disabled. [4-5,9]

## Final gate

Run gate-20261115T192826Z, code at 636b265, evidence #186 [11]. This is the run on the frozen code, with internal authentication switched on, aging at 0 s, autoscaling off, and fixed replicas (API 2, matcher 2).

| Check | Result |
|---|---|
| Course demos 1 to 9, two rounds | 18/18 passed (resets 106 s and 109 s) |
| Replica replacement | 65/65 checks passed |
| Chaos (10 faults plus 2 control experiments) | 22/22 checks passed: 0 lost writes, 0 unresolved unknowns, 0 stale reads |
| Auth probes | 401 without a token or with a wrong one, 200 with the right one |
| Auth log checks | All passed and complete: 71/71 pod logs read, no token found in any log |
| Merkle audit and strict invariant check | 8/8 passed; clean, with no undelivered outbox events |
| UI forms, PS alias routes (7), traced request | Passed |
| Fixed-replica check | API 2, matcher 2, no autoscalers |

Change after the gate. After the final gate, PR #189 (merge 5974ff2) fixed the UI’s Needs-attention counter so that it counts requests by their current workflow state. The change is UI-only, adds a browser regression test, and does not affect the storage, workflow or messaging code that the gate tested, so the gate results above still apply. After the gate, PRs #194-#199 (merged to main in #200, merge 1bc2b7b) improved the dispatcher UI: map pins by urgency and type, a details panel, offline place search, the 50 km matching radius with reasons why resources are not eligible, request sorting with waiting and window badges, an activity feed, a queue sparkline and dark mode. They are UI-only, with browser tests (97) and unit tests (44), and change no storage, workflow, messaging, API or routing code, so the gate results still apply.

The final gate passed on its third attempt. The first two stopped on problems in our test scripts, not in the system: a check that looked for a new leader only once instead of waiting for the election, and a metrics check that was not given its auth token. Both failed runs are kept in the evidence with their causes.

## Internal authentication

Official run, evidence #181 [12]. Internal calls between services now need a token. In the official run on a fresh deployment, all 9 demos passed with auth on, all 18 checks for refused and logged requests passed, and a full token rotation passed 40/40 checks: first only the old token worked, then both worked during the switch, then only the new one.

## Not certified

- Positive-interval aging and autoscaling were not verified in the final gate.
- Token rotation was verified in the official auth run but not repeated on the final code.
- Switching auth on for an already-running deployment was not tested.

## 7. Performance: Phase 1 and Phase 2

Seven retained reports use clean recorded source 7dd1940: three baselines, three step runs and one soak, seed 52, one 8-logical-CPU Core Ultra 7 258V host with 30.4 GiB RAM. kind used CPUs 0-5 and k6 6-7, Guaranteed resources, load Raft profile, fixed 2/2, tracing/autoscaling off. Source pins, recorded image IDs and a fence setting alone are not proof of immutable image binding or actual read-mode use. Phase 1 callers used strong/Barrier. [7]

T6a. Client HTTP p99 milliseconds; repeated-row values are medians of run percentiles, not pooled percentiles. HTTP completion is not sustained matching capacity.

| Run | Offered / achieved req/s | Write / ID / list p99 ms | Error fraction |
|---|---|---|---|
| Baseline, n=3 | 25 / 25 | 684 / 79.94 / 291.22 | 0.001 (0.100%) |
| Step, n=3 | 10-80 / 43.86 aggregate | 1049.34 / 102.43 / 367.06 | 0.00095 (0.095%) |
| Soak, n=1 | 35 / 35 | 838.84 / 93.75 / 330.37 | 0.001275 (0.1275%) |

## Load method and selected M6 study

Baselines measure 600 s and soak 1800 s after excluding 120 s HTTP warm-up; step runs measure 960 s without that warm-up. The mix is 15% creates, 10% resources, 10% match, 55% ID reads and 10% lists. All three step runs bracket the HTTP threshold at 50 req/s; write p99 crosses 1 s at 60. Other bounds are ID p99 <=250 ms, list p99 <=500 ms, errors <=1% and no dropped iterations. Baseline write p99 spans 665.75-690.92 ms. [7]

Requests intentionally outnumber resources. Baseline matched/WAITING counts are 1483/780; step counts 4276/2253. Matched-only latency excludes WAITING and includes resource availability. Backpressure remains visible: the soak records 416 unconfirmed publishes during load; one step records 5968 and 5209 in its after-run window. Those observations do not by themselves establish acknowledged data loss. Audits stopped retry admission, waited under the original headroom rule, drained handoffs, checked invariants and restored workers; this is not busy-system equilibrium. Off-repo raw artifacts retain hashes but were not re-read here.

T6b. Joint M6 experiment, derived from S4.1's existing study. Five samples/mode, 2000 iterations/sample, one process with three loopback replicas. Serial/parallel figures are benchmark ns/op; parallel throughput cost is not per-request tail latency. [8]

| Mode | Serial median ns/op | Parallel median ns/op | Steady-state entries/read |
|---|---|---|---|
| Barrier | 449566 | 112730 | 1 |
| Fence | 126084 | 23335 | 0 |

Ratios of medians are 3.566 and 4.831, not paired-trial speedups/confidence bounds. The raw benchmark omits runtime Go version; its source pin and separate safety run do not independently fill that omission. The term gate can still need a barrier. Shared-host noise and ordering constrain interpretation; this microbenchmark cannot establish whole-system load causality.

T6c. Placement simulation moves 24.93% of 10000 keys when extending three to four shards; it is synthetic, not live migration. Direct mod 3 to mod 4 would move an expected 75% under a uniform fixed hash; that is an analytical comparison, not a measured modulo deployment. [3]

## Phase 2 comparison

T6d. Same host and load profile; medians of three runs per phase; baselines offer 25 req/s. Historical revisions are 7dd1940 and 514bb33. Barrier counts cover the step window across all shards; refusal counts are global broker nacks, not lost acknowledged writes. [10]

| Measurement | Phase 1: strong default | Phase 2: linearizable default |
|---|---|---|
| HTTP saturation | 50 req/s; write p99 fails at 60 | >80 req/s; no tested step fails |
| Baseline write p99, ms | 684 | 93.57 |
| Baseline ID-read p99, ms | 79.94 | 56.43 |
| Baseline list p99, ms | 291.22 | 90.31 |
| Write p99 at 80 req/s, ms | 1180.37 | 388.50 |
| Barriers per step run | 270191 | 0 |
| Broker refusals per step run | 6324 | 8 |

Phase 1 issued one Raft barrier for each strong read; #166 made the linearizable fence the application default, removing steady-state read entries while retaining committed mutation checks. All three Phase 2 step runs passed through 80 req/s with clean invariant checks, audits and restoration. This establishes an HTTP lower bound above 80 req/s (n=3), not sustained matching capacity. Error rates remain nonzero: baseline median 0.000167 and step median 0.001093. The 29 residual counter differences are applied commands in flight at the end scrape, not barriers. [10]

Two exploratory runs (n=1 each) suggest a request-path limit near 100-110 req/s as list p99 exceeds 500 ms, but neither audit completed: one drain was refused and the other retained too much WAITING retry backlog. Generator memory or the 400-VU cap also limited results above 110 req/s. The coordinator is the next observed pressure point, reaching about 98% of its 250 mCPU allowance from 50 req/s; the request queue remains near capacity at upper steps. These observations do not prove a sole bottleneck or audited capacity at 110. [10]

Phase 2 adds no soak. The slim archive lists omitted raw files with hashes/locations; those off-repo files were not re-read here. The first step run's acceptance tail was completed by hand after the harness returned the documented above-range inconclusive result. The current #176 classifier labels the same threshold-clean range as a lower bound without hiding safety failures. The comparison was independently regenerated from retained request CSVs/report/extras inputs without drift; measurements remain bound to their historical source, not this final assembled artifact. [10]

## 8. Scope, decisions and conclusion

T7. PS section 12 decision matrix; rationales are not performance proofs. Other service/language, type/location matching and observability choices are covered above.

| Decision | Alternative | Why chosen / cost | Evidence |
|---|---|---|---|
| Raft | Primary-backup | Quorum order/elections; majority required | [4] |
| xxhash, 128 vnodes/shard | Modulo/dynamic placement | Stable versioned owner; no online migration | [3] |
| Derived SQLite FSM | Primary DB / memory | Transactional replay; rebuild cost | [4] |
| Transactional outbox | DB/broker dual-write | Atomic state/event; dedupe cost | ADR005 |
| Quorum queues | Classic/transient | Durable type; deployed broker still singleton | [4] |
| Fence versus Barrier | Barrier each read | No steady-state read entries; proof complexity | [8] |
| Linearizable default | Stale / strong only | Fresh reads; committed CAS/TryClaim decide writes; quorum dependency | [3,8] |
| kind | Managed cloud | Required repeatable local scope; one host | [9] |
| Open-model load | Closed-loop VUs | Offered/achieved gap and dropped work visible | [7] |
| Merkle audit | Full dumps/row copying | Equal-index localization; diagnostic only | [9] |

ReliefGrid shows that a sharded, replicated matching system can stay correct when parts of it fail. Across the final gate and the chaos tests, no acknowledged write was lost, no resource was given out twice, and uncertain outcomes were retried safely instead of guessed.

The most useful thing we learned came from load testing. Our reads were each adding an entry to the Raft log, which made the coordinator the bottleneck. Switching to a read fence that confirms leadership without writing to the log raised the validated limit from 50 to over 80 requests per second and cut write latency by about 7 times. We would not have found this without measuring.

The system still has clear limits. It runs on one machine, the coordinator and the message broker are single instances, and the cluster size is fixed. Aging and autoscaling were built but not certified. These would be the next steps.

## 9. References and repository evidence index

[1] Course Cloud Computing problem statement: Tables 2/3/8; sections 9, 10 and 12. Local deployment and live demo requirements are authoritative.

[2] [Implementation source at submission](https://github.com/ParthMalhotra07/ReliefGrid/tree/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9). Historical measurements retain their own revisions.

[3] [Public/internal/ring contracts and simulation](https://github.com/ParthMalhotra07/ReliefGrid/tree/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/contracts). [4] [Architecture, workflow and ADR004/005](https://github.com/ParthMalhotra07/ReliefGrid/blob/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/report/architecture.md). [5] [12 November scoped gate](https://github.com/ParthMalhotra07/ReliefGrid/blob/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/evidence/gate-20261112T100532Z/report.md). [6] [Later #167 campaign](https://github.com/ParthMalhotra07/ReliefGrid/tree/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/evidence/S4.3/suite-20261112T155808Z-hrishabh-sharrma-ThinkPad-E14-Gen-7). [#168 unknown-outcome fix](https://github.com/ParthMalhotra07/ReliefGrid/pull/168). [7] [S5.2 Phase 1 reports/aggregates and methodology](https://github.com/ParthMalhotra07/ReliefGrid/tree/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/evidence/S5.2). [8] [ADR006 and retained M6 raw replay](https://github.com/ParthMalhotra07/ReliefGrid/blob/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/adr/006-read-fence-study.md). [9] [Deployment, Merkle audit and qualified validation/rehearsal records](https://github.com/ParthMalhotra07/ReliefGrid/blob/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/report/deployment-and-demo.md).

[10] [Phase 2 comparison, reviewed #176](https://github.com/ParthMalhotra07/ReliefGrid/blob/25ea8bc5675ab935d0b4852f8adbacb4cf14b9b3/docs/evidence/S5.2/phase2-linearizable/comparison.md) and [method/deviations/limits](https://github.com/ParthMalhotra07/ReliefGrid/blob/25ea8bc5675ab935d0b4852f8adbacb4cf14b9b3/docs/evidence/S5.2/phase2-linearizable/README.md). Raw measurement source 514bb33; evidence publication source 25ea8bc5675ab935d0b4852f8adbacb4cf14b9b3.

[11] [Final gate gate-20261115T192826Z evidence](https://github.com/ParthMalhotra07/ReliefGrid/tree/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/evidence/gate-20261115T192826Z) and [PR #186](https://github.com/ParthMalhotra07/ReliefGrid/pull/186).

[12] [Internal auth official enable and rotation run](https://github.com/ParthMalhotra07/ReliefGrid/tree/1bc2b7b8c58294e7e49abb526b8f98e7b9c80ba9/docs/evidence/S-29-auth-enable) and [PR #181](https://github.com/ParthMalhotra07/ReliefGrid/pull/181).
