# Proposed submission outline: fewer than ten pages

The professor has confirmed the report must be **under 10 pages**. Target **nine
pages total**, including title, figures and references, so pagination has a buffer.
This is a proposal for both owners to agree now; it does not wait for13November
and does not claim that a rendered submission already meets the limit.

- Title, abstract and problem statement:0.5page; joint editing.
- Requirement/service/API mapping:1page; A, with S's internal/failure mapping.
- Architecture, sharding, consistency and schemas:1.5pages; S.
- Workflow, claims, messaging and recovery method:1page; A.
- Docker/Kubernetes deployment and reproducibility:1page; joint.
- Validation, gate and fault results with limitations:1.5pages; joint.
- Performance and the selected S4.1/M6 experiment:1page; S supplies #126 results,
  A integrates the existing joint experiment record. Measurements stay pending.
- Scope, limitations and conclusion:0.75page; joint.
- References and repository evidence index:0.75page; A assembles, both verify.

The budgets sum to9pages. Keep detailed schema/API descriptions, full scenario
lists, raw reports, operational procedures and extended discussion in the linked
repository chapters: [architecture](architecture.md),
[workflow/messaging](workflow-and-messaging.md),
[results](requirements-and-results.md), [deployment](deployment-and-demo.md),
[validation](validation-addendum.md), [ADR004](../adr/004-attempt-closure.md),
[ADR005](../adr/005-outbox-handoff.md) and [ADR006](../adr/006-read-fence-study.md).
The submission should summarize and cite them, preserving historical source/image
qualifications rather than copying the whole repository document set into a PDF.

Before final delivery: agree the outline, integrate actual #126 measurements,
record the final gate scope/results, assemble the concise report, export/render
it, and count **all** pages. Fix layout/content if the result is ten pages or more.
No report export/page-count acceptance or professor-approved detailed outline is
claimed by this Markdown proposal.
