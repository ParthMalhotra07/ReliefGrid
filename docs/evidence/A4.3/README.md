# A4.3 real-workload acceptance — PASS, owner review pending

Run 20261107T085200Z:709 actual create/handoff/cancel/terminal lifecycles,
34315/34315 checks,47559 HTTP calls,0 unexpected HTTP failures. Five known
VERSION_CONFLICT cancellation rejections were logged, reread and retried by the
client (maximum3 attempts). Both unchanged rate==1 and rate==0 thresholds passed.
409 is an expected HTTP status only on cancellation; wrong error codes, unknown
outcomes, failed rereads and exhausted retries remain functional failures.

Matcher Ready1→8→1 and API Ready2→6→2. No matcher decrease during the90s bulk
window. Eleven removed pods had final clean exit0 observed before their grace
deadlines, including both components during real low-rate tail traffic. All709
created workflows reached CANCELLED without ownership, queues drained and no app
restarts occurred. The verifier restored manual API2/matcher2 settings afterward.

Runtime used main's existing app images plus the exact #129 probe and #133 native
boot chart merge, recorded in runtime-sources.txt. Neither dependency is silently
claimed merged. Main must incorporate/rebase these before the final merge chain;
CI must test the current combined main. No storage images/PVCs were replaced.

The earlier two unexplained cancellations were diagnosed in a bounded45s run:
409 VERSION_CONFLICT, outcomeUnknown=false. See diagnosed-cancel-conflict.json.
API/FSM semantics were correct; the original load client omitted the documented
reread/retry. No production API change was made to hide a correct rejection.

Red-first regressions cover the known-rejection retry (3red then5green cases)
and retained terminal history in preflight (red then10green acceptance cases).
Full make check, make gen-check and make verify-A4.3-static PASS. Inputs are hashed
in source-sha256.json. One full live round is retained; old failed full-run output
was archived locally. Prior red-case evidence remains for review.

PRE-PR REVIEW: READY for re-review. One controller per workload; no ack/claim/CAS
invariant change; final exits and observations fail closed; no S-owned or contract
edits; no credentials or local-only files. This acceptance is not a throughput
benchmark and does not close the A4.2 storage-metrics handoff. Closes #127 on merge.
