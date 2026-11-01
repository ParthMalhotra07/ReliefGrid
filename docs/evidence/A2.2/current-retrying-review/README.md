# A2.2 review — current RETRYING confirmation and full SCH08

The added test proposes confirmation using the current RETRYING version,
status and attempt and requires DOMAIN_CONFLICT with the complete request row
unchanged. It then resumes closure through Abandon and drives SCH08 through
its exact final state: request MATCHED v6 on attempt B; old resource AVAILABLE
v3 and unclaimed; second resource ASSIGNED v2 with the reciprocal B claim.
A late claim against the old attempt remains CLOSED.

A Go build overlay supplied a deliberately unsafe copy of the transition rule
that allowed RETRYING confirmation. The new test failed (mutation-red.log).
The original S-owned source was never changed, and no overlay or unsafe source
is committed. The normal real-FSM regression passed (green.log), followed by
full ordinary/chaos/live checks and clean generation. No existing assertion,
fixture, timeout, contract or invariant was changed.

Production currently does not call Abandon: it is a tested helper for the
future reconciler/retarget policy. A2.4/#79 must supply that policy and the
wakeup/cleanup recovery path before the gate claims full liveness coverage.

Pre-PR review: READY. This adds regression coverage and states the actual
production limitation without changing workflow/FSM behavior.
