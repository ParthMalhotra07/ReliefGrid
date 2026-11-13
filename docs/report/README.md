# ReliefGrid — report core

ReliefGrid matches emergency requests to geographically suitable resources using
Raft-backed sharded storage and asynchronous workflows. The detailed report core
was reviewed and merged through #137. This update reconciles its publication
state against main `7fa4a7d0093d92477aa90a9a23fe1d3e942083b6`; each historical run keeps its own source/tree/
image qualifications rather than becoming acceptance of that baseline.

The professor requires **under 10 pages**. The agreed
[nine-page outline](submission-outline.md) allocates S 5.5 and A 3.5 pages,
including figures/captions/shared tables. The
[A-owned submission fragments](submission-A-draft.md) are ready for editorial
integration with S's text and F1-F4. They are not an exported/page-counted report.
Both owners still need to review the assembled submission and rendered layout.

## Reading order

1. [Architecture, services, APIs and schemas](architecture.md)
2. [Workflow and messaging](workflow-and-messaging.md)
3. [Requirements and measured results](requirements-and-results.md)
4. [Deployment and live demonstration](deployment-and-demo.md)
5. [ADR004](../adr/004-attempt-closure.md),
   [ADR005](../adr/005-outbox-handoff.md) and [ADR006](../adr/006-read-fence-study.md)
6. [Additional validation and limits](validation-addendum.md)

## Ownership and current evidence

Parth owns application/workflow/messaging/UI/app chart/dashboards; Hrishabh owns
storage/coordinator/Raft/chaos/tracing/CI. Storage/reliability material is integrated
through the shared chapters. The PS requires **2–3 microservices**, not 23:
API and matcher are separate application services, with coordinator/storage as
the distributed data service. kind is allowed by PS9.2; cloud remains parked.

Accepted #126 Phase 1 results are retained under [S5.2](../evidence/S5.2).
The [12 November scoped gate](../evidence/gate-20261112T100532Z/README.md), merged
through #165, records 18/18 course reports, chaos 12/12 runs (10 faults plus two
controls), Merkle 7/7 and clean invcheck/fixed-profile collection. Its source is
1623c23, aging is 0s, autoscaling off and auth excluded. The report retains the
app-quiesced chaos, omitted raw artifacts and NOT_ESTABLISHED runtime build binding.
It does not certify later forms/auth/read changes or every enhanced goal.

Later comparison measurements need their own accepted run links. The final gate
with alias/form checks, enabled auth/aging evidence, any final autoscaling option,
rehearsals, recording and final-package checks remain separately identified work.

Run `python3 scripts/verify-report.py` for source/evidence link checks. Passing
links cannot prove scientific claims or page count. The public repository link,
mandatory live demo and deployment-from-scratch backup recording are required;
public visibility alone does not establish actual submission or playback.

## Assembled review draft

The [assembled Markdown](submission-draft.md) and
[nine-page PDF](reliefgrid-submission-draft.pdf) integrate the A text with
[Hrishabh's supplied contribution](contributions/README.md). See
[assembly/build instructions](ASSEMBLY.md) and [editable figures](figures/README.md).
The rendered draft is under ten pages including references. It remains a
review draft with Phase 2 publication, final source pin and final evidence
acceptance outstanding; archival editorial placeholders are not accepted
submission references.
