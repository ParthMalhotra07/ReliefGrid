# A-owned submission text: integration draft

Editing metadata, excluded from submission: Parth's contribution to the agreed
[nine-page outline](submission-outline.md), targeting the 3.45-page A share,
including allocated tables/references. Hrishabh supplies the complementary 5.55
pages. These fragments use the existing section numbers; they are not additional
sections or an exported/page-counted report. Source baseline `7fa4a7d0093d92477aa90a9a23fe1d3e942083b6`.
Preserve each measurement's historical revision; update implementation claims
against the accepted final source before export. S's supplied text and F1-F4 drawings are preserved in
[contributions](contributions/README.md); the [assembled draft](submission-draft.md)
combines these sources without treating unpublished Phase 2 numbers as accepted.

## 1. Title, abstract and problem statement — joint opening

**ReliefGrid: recoverable emergency-resource matching on local Kubernetes.**
An emergency request must find a resource of the right type, near its location
and within its availability window, while failures and duplicate messages may
interrupt assignment. ReliefGrid separates a Go HTTP API from asynchronous
matching workers, backed by three replicated storage shards. Request intent,
conditional claims and durable attempt closure coordinate assignment across
shards; a transactional outbox bridges committed changes to RabbitMQ. Evaluation
combines deterministic state-machine schedules, process faults and recorded
local kind demonstrations. Results describe named executions and configurations,
not universal availability or exactly-once message delivery.

## 2. Requirement/service/API mapping — A text and T3

The API and matcher provide two application microservices; coordinator/storage
provide the distributed data service, and the UI is its live operator view.
The canonical public base is `/api/v1`. Traefik's method/path-specific aliases
serve all seven PS Table 2 operations through the same handlers. They preserve
the idempotency namespace and queries, without intercepting unrelated UI routes.

| PS client operation | Method and alias | Normal response |
|---|---|---|
| Submit request | POST /requests | 201, canonical Location |
| Request details | GET /requests/{id} | 200 |
| Request status | PUT /requests/{id}/status | 200 |
| Register resource | POST /resources | 201, canonical Location |
| List resources | GET /resources | 200 |
| Resource status | PUT /resources/{id}/status | 200 |
| Match request | POST /match/{requestId} | 200, or 202 while work continues |

PS identifiers map to `requestId`/`resourceId`; entity types to
`requestType`/`resourceType`, with `type` as a list filter. The server persists
creation time as `createdAt`. Resource availability combines its window, status
and absence of an active claim. A match 202 is acceptance/deferred work, not
proof of assignment; clients poll current request state. Full mappings and
contracts are in [PS compliance](ps-compliance.md) and
[public.yaml](../../contracts/public.yaml). S contributes the internal API/PS
mapping and stack-role columns within this section's existing budget.

Keyed creates freeze the canonical payload, identifier, timestamps and original
response across retries. Reusing a key with a changed payload is rejected.
Unknown outcomes retain that identity. ETags/If-Match protect conditional
updates; cursors bind pagination to query/ring state, not a global snapshot.
Partial or failed authoritative reads cannot establish absence. Lists and SSE
extend the seven required operations; no bare `/events` alias is added.

## 4. Workflow, claims, messaging and recovery — A prose for F3

Creation commits domain state and its outbox event together. A relay reads
bounded, per-shard batches, publishes persistent messages with mandatory routing
and publisher confirms, then marks only routed confirmations. A crash between
confirmation and marking can duplicate a publish, so consumers remain idempotent.
This avoids a database/broker dual-write gap without claiming exactly-once
transport. Retry queues use explicit dead-letter routing back to request work;
manual acknowledgement and bounded prefetch separate durable handoff from
broker delivery.

The matcher reads current request state and uses type/location/window filters
to identify candidate hints. It commits a MATCHING intent binding request,
resource, attempt and fingerprint before TryClaim. The resource shard evaluates
eligibility and closure within committed apply; request confirmation then checks
the current version/attempt. Ownership must be reciprocal. A historical success
cannot prove that a resource is still owned after later cleanup.

An uncertain response preserves the intent and operationId. Retargeting is
allowed only after committed closure, so a delayed old claim is rejected.
Cancellation/fulfilment remain CANCELLING/FULFILLING while closure and resource
cleanup are outstanding. Reusable resources can return to AVAILABLE; consumed
resources remain CONSUMED and expired windows become OFFLINE. Terminal requests
hold no live claim. Consumers acknowledge only after the durable transition or
retry handoff they depend on; outages do not justify an immediate nack loop.

Reconcilers page bounded scans and resume stranded intermediate states through
the same workflow rules. Failed/partial scans are unknown. Recovery depends on
eventual quorum, broker and worker availability; it is not a cross-shard SQL
transaction. F3 should show the commit/publish/mark and intent/claim/confirm/ack
boundaries, including the unknown-outcome/closure branch. See
[workflow](workflow-and-messaging.md), [ADR004](../adr/004-attempt-closure.md)
and [ADR005](../adr/005-outbox-handoff.md).

## 5. Docker/Kubernetes deployment — A contribution

Multi-stage images run nonroot application binaries. The app Helm release
deploys API/matcher replicas, RabbitMQ, UI and ingress over the standalone
storage release (`storage.enabled=false`), avoiding duplicate storage ownership.
Coordinator-client labels admit the relevant callers through policy. Readiness
drains before listener shutdown; consumers finish durable handoffs before exit.
Metrics use bounded labels/private scrape access. Fixed replicas are the default;
API HPA and matcher KEDA are optional, separately verified controllers.
[Deployment details](deployment-and-demo.md).

## 6. Validation — A contribution and evidence qualification

The application test campaign combines real-FSM adversarial schedules with
retained kind/broker/browser checks. The historical scaling run records 637
aggregate CANCELLED/no-assigned-resource checks, not a per-request response
ledger. Its dirty source and missing immutable build binding limit acceptance
to recorded images; its final fixed API2/matcher2 restoration was unverified.
A later scoped gate separately records fixed 2/2 observations, not a retroactive
repair of that historical run. [Validation limits](validation-addendum.md).

The merged create forms retain one key/body through double-clicks, ambiguous
responses and same-tab reloads, then require a current versioned read and view
refresh. A v3 receipt cannot be confirmed by a v2 read. Saved/offline observations
are labelled, and offline writes are disabled. Their local/browser regressions
do not replace final deployed alias/form checks. The scoped 12 November gate
excludes auth, positive aging and autoscaling; later acceptance stays separate.
[Forms implementation](../../ui/src/create.js),
[gate scope/results](../evidence/gate-20261112T100532Z/report.md).

## 7. Performance and M6 — A integration sentence

Both owners select S4.1's existing Barrier/fence study as M6: historical serial
and parallel ratios of medians are 3.566 and 4.831, with measured steady-state
log entries/read 1 versus 0. Three replicas share one test process over loopback;
this is not application-load causality or tail-latency evidence. The benchmark
omits runtime Go version; its source pin and separate safety record do not fill
that omission. S supplies the separately qualified system-load results.
[ADR006](../adr/006-read-fence-study.md).

## 8. Scope, limitations and conclusion — A contribution

The result is a recoverable matching workflow with explicit uncertainty and
durable handoffs. Singleton coordinator/broker and one-host kind placement limit
availability realism. List pages are not global snapshots. Auth/aging and optional
autoscaling require their own final-cohort evidence before being called accepted.
Cloud remains parked. S/A decision rows remain in T7, within the shared budget.

## 9. References and evidence index — A assembly block

Use compact numbered references in the final layout, with immutable source/run
links; these repository links retain the longer material:

1. Course problem statement: Tables 2/3/8; deployment section 9; demonstrations
   section 10; design decisions section 12. ReliefGrid Final Plan v3.2: Part 1
   decisions M1-M8 override Part 2. Cite the supplied documents in the submission;
   do not commit local-only plan/playbook files.
2. [Accepted source baseline](https://github.com/ParthMalhotra07/ReliefGrid/tree/7fa4a7d0093d92477aa90a9a23fe1d3e942083b6),
   [contracts](../../contracts) and [PS mapping](ps-compliance.md).
3. ADR004/005/006 above; [architecture](architecture.md), workflow, deployment
   and [requirements/results](requirements-and-results.md).
4. [4 November gate](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/gate.txt),
   [12 November scoped gate](../evidence/gate-20261112T100532Z/README.md),
   [schedule evidence](../evidence/A2.3/README.md), and qualified validation above.
5. [S5.2 Phase 1 aggregates](../evidence/S5.2/aggregate-baseline.json),
   [step aggregate](../evidence/S5.2/aggregate-step.json) and
   [M6 raw replay](../evidence/A5.3-read-fence/summary.json).

Editorial closeout: integrate S's text/figures without double-counting joint
material, bind final claims to accepted evidence, export/render and count every
page. The complete report must be under ten pages; this Markdown draft does not
establish that layout acceptance or completion of the live demo/recording.
