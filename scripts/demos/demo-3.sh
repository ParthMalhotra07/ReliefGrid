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
# Executed through app.sh and app_demo.py. The shared report retains the seed,
# assertions, inputs, HTTP outcomes and bounded recovery observations.
. "$(dirname "$0")/lib.sh"
demo_begin 3 "Async and explicit API matching with compatible and incompatible resources"
# Shared preflight and durable evidence wrap the executable application assertions.
. "$(dirname "$0")/app.sh"
app_demo 3
