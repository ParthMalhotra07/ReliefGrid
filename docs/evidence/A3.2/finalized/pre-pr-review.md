# A3.2 integration review

The diff against #86 is limited to A3.2 API health/shutdown, PDB, request and
wakeup drain, supervisor connection lifetime, regressions and evidence. No
storage/contract code or A3.1 implementation commits are included.

Both consumers preserve commit-before-ack; failed attempts keep the same
unacked delivery. Only the active bounded attempt survives admission shutdown.
Connection cleanup and API shutdown/Serve joining remain bounded. Existing
five-worker joins and readiness state from A2.4 are retained. Abrupt crashes
still rely on the existing unknown-outcome/idempotent retry path.

Full check/gen and affected race count3 passed. Both real-broker admitted
handoff tests pass. The A2.4 25-hop verifier is required before push to pin
delivery retention. Earlier combined demo5/8 evidence retains its actual source
identity and is not represented as a new run of this branch.

READY for review: current source combined with #84 passed clean-deployment
demo8, including an actual interruption and strict audit. See ../acceptance/.
Parent merge/rebase and main post-merge checks remain. The combined default
check exposed unchanged S-owned #91; a sequential-package full check passed
without modifying tests, and standard CI on production head3b7acb6 passed.
