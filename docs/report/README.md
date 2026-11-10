# ReliefGrid — report core

ReliefGrid matches emergency requests to geographically suitable resources using
Raft-backed sharded storage and asynchronous workflows. This report describes the
implemented core, refreshed against main
`42741b1306ba97013637c826ccb78aced5957b02`. It distinguishes measured evidence
from planned enhancements; owner review and the instructor's required final report length are
still pending.

## Reading order

1. [Architecture, services, APIs and schemas](architecture.md)
2. [Workflow and messaging methodology](workflow-and-messaging.md)
3. [Requirements and measured results](requirements-and-results.md)
4. [Deployment and live demonstration](deployment-and-demo.md)
5. [ADR004: attempt closure](../adr/004-attempt-closure.md) and
   [ADR005: outbox handoff](../adr/005-outbox-handoff.md)

6. [Additional engineering validation and limits](validation-addendum.md)

## Scope and attribution

Parth owns the application, matching workflows, messaging, UI, app chart and
dashboards. Hrishabh owns storage, coordinator, Raft integration, chaos, tracing
and CI. Hrishabh's supplied storage/reliability contribution is integrated across
architecture,
consistency/schema, workflow storage boundaries, evidence and deployment chapters.
Source references were refreshed from his e50685d contribution to integrated
main 42741b1, while historical evidence keeps its original qualifications. Both
owners still review the integrated report; #126 measurements and final enhanced
gate results remain PENDING, with no benchmark invented here.

The problem statement requires **2–3 microservices**, one coordinator, three
shards, RabbitMQ, Docker and Kubernetes; it does not require 23 microservices.
API and matcher are separate application services, with coordinator and storage
providing the distributed data service. kind is explicitly permitted by PS9.2.
Cloud is optional and parked following the instructor's direction.

## Reproducibility and claim boundary

The [recorded course gate](../evidence/gate-20261104T144814Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/gate.txt)
reports18/18 demo passes across two clean deployments on4November. That evidence
belongs to the recorded code tree, not automatically to every later commit.
See the [UI evidence](../evidence/A4.1) for the merged dispatch interface.
Metrics, autoscaling and API refinements have their own PR/evidence lifecycle;
they are not retrospectively claimed as part of that gate.

Run `python3 scripts/verify-report.py` to check local source/evidence links. The
checker verifies references, not scientific correctness; both owners still review
the narrative and final live demonstration. The repository must be public before
submission, the live demo is mandatory, and a recording is a backup. A separate
final-package check must confirm these external deliverables actually exist.
