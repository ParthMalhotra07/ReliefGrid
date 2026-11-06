# A4.3 review checkpoint — NOT READY

This is a draft checkpoint, not a completed goal. Issue127 records the remaining
strict acceptance failure. The former cancelled-request no-op evidence has
been archived locally; it is not offered as proof for this real-workload goal.

Current code: independent API CPU HPA2–6 and HTTP queue KEDA1–8, including
unacknowledged work;120s stabilization; bounded real lifecycle clients; final
pod-status watch; API scale observations; values-file image pins with unused
duplicated tools digests removed. k6 pin names match approved S PR125.

Local make check, generation and static checks passed (9 acceptance tests).
Trace-only rejudging of the final real run passes: matcher1→8→1 and API2→6→2,
no matcher decrease during the heavy arrival window, no observed app restarts.
The broker continuously ran with0restarts throughout this run.

OVERALL LIVE RESULT IS FAIL:35938/35940 checks passed,2 cancellation responses
were non-200;2/48882 HTTP calls failed. Raw status/body were not retained by
that version of the harness, so their cause is not guessed. Diagnostic logging
and failed-run pod-watch persistence are now present for follow-up. The strict
load thresholds and terminal/reciprocal postconditions have not been relaxed.
Complete clean-exit acceptance was not reached and remains unverified.

Three review correction attempts were used; physical reruns stopped. All668
owned request fixtures were subsequently cancelled and4resources made OFFLINE
outside acceptance, without resetting storage or touching unrelated claims.
Normal fixed API2/matcher2 settings are restored. Seven resources from an earlier
interrupted MEDICINE fixture remain assigned to pre-existing demo requests and
were deliberately left intact; that development record is archived locally.
