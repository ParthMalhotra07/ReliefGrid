# Application aging — A4.4

Depends on merged storage seam150. The application adds the bounded reconciler
policy, deterministic proposals, immediate wake envelopes and later effective
priority preservation. Original urgency stays unchanged. No contract or S-owned
implementation changes are part of this application PR.

Red regression: reconciler has no aging interval. Trace red: an unknown operation
changes envelope bytes when its retry carries a new tracing context. Green uses
a fixed operation-derived root for the persisted aging envelope; no remote call
uses a background context. Exact operation ID, timestamp, interval, version and
payload now remain stable across clock/tracing changes and worker restarts.

Unit controls (-race -count=3) cover future backoff, exact threshold, original
urgency/cap, subsequent work priority, unknown retries, partial scans, disabled
mode and cancellation winning after a proposal. The cancellation test uses a
barrier around the real M8 mutation, not timing sleeps. Default mode emits no
aging commands; incomplete scans emit none either.

The pinned4.3.7 RabbitMQ integration observed the immediate priority2 wake through
the existing matching.requests binding in77ms, before the5s retry tier. The real
relay marked two durable events only after mandatory/routed confirms. A dedicated
disposable broker was removed afterwards. `make verify-A4.4-aging` reproduces the
real-FSM and broker checks and is invoked from CI's make check.

Full make check, gen-check, tagged vet/lint and TestApp Helm cases passed. The
chart regression verifies aging is matcher-only and defaults0s. The source-parent
rebase onto merged150 has an empty tree diff relative to the tested seam head.

Pre-PR review: READY. Conditional changes remain inside FSM apply; no intent or
claim is cleared by aging. A strong reread follows an OK or version conflict.
Unknown remains an error; scanning is bounded by existing pages/batches and calls
retain the workflow/reconciler deadlines. Labels add only the constant `age`.

Limits: zero is the default until all storage replicas support the seam. Policy
advances one due boundary per pass (or catches up to a later persisted UpdatedAt),
so it is not an instantaneous global priority scheduler. No new physical kind
aging acceptance is claimed by these M8/broker checks. Internal-call auth still
needs the owner interface on29; this does not complete that independent item.
