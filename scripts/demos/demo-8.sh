#!/usr/bin/env bash
# demo-8 (owner A, goal A3.1): v3.1 §28 row 8.
#
# What to show:   Delete one API or matcher pod while traffic/work continues
# Acceptance:     Kubernetes creates replacement; progress continues; interrupted calls counted/retried
#
# Assertions to write (plan A3.2: graceful shutdown, PDB minAvailable=1 for the API):
#   - Start seeded traffic through the API Service and record every call's outcome.
#   - Delete one API pod (then, in a second part, one matcher pod) mid-traffic.
#   - A replacement pod (new UID) becomes Ready within a bounded time.
#   - Progress continues: completed calls and matches keep rising during and after the delete.
#   - Interrupted calls are counted and retried with the same Idempotency-Key; none is lost or
#     duplicated; expected transient errors are recorded, not hidden (§28 demo hygiene).
#   - invcheck check exits 0 after settling.
#
# Seed/reset: write it with demo_reset; start from the gate's clean deployment (reset.sh).
# Fill in the body below with lib.sh (run, check, pass, fail, demo_end); see demo-4.sh for a
# worked example. Until then this demo reports NOT IMPLEMENTED, which the gate counts as a
# failure.
. "$(dirname "$0")/lib.sh"
demo_begin 8 "Delete one API or matcher pod while traffic and work continue"
demo_not_implemented "owner A, goal A3.1"
