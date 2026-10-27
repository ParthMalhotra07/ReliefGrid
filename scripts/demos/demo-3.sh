#!/usr/bin/env bash
# demo-3 (owner A, goal A3.1): v3.1 §28 row 3.
#
# What to show:   Async and explicit API matching using compatible/incompatible resources
# Acceptance:     Correct type/radius/window; reciprocal assignment; no double claim
#
# Assertions to write:
#   - Seed resources: one compatible (type, inside the radius, window covers now) and
#     incompatible ones (wrong type, outside the radius, window closed).
#   - Async: a new request is MATCHED to the compatible resource only, through the
#     matcher (RabbitMQ), within a bounded wait.
#   - Explicit: POST /api/v1/match/{requestId} gives the same rules.
#   - Reciprocal assignment: request.assignedResourceId == resource ID and the resource's
#     claim names the request (the strong read of both records).
#   - No double claim: two requests competing for one resource; exactly one is MATCHED, the
#     other WAITING or RETRYING; invcheck check exits 0 (at most one live claim).
#
# Seed/reset: write it with demo_reset; start from the gate's clean deployment (reset.sh).
# Fill in the body below with lib.sh (run, check, pass, fail, demo_end); see demo-4.sh for a
# worked example. Until then this demo reports NOT IMPLEMENTED, which the gate counts as a
# failure.
. "$(dirname "$0")/lib.sh"
demo_begin 3 "Async and explicit API matching with compatible and incompatible resources"
demo_not_implemented "owner A, goal A3.1"
