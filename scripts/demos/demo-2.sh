#!/usr/bin/env bash
# demo-2 (owner A, goal A3.1): v3.1 §28 row 2.
#
# What to show:   Register resource, list it, update availability/status
# Acceptance:     Resource persists with validated state and location
#
# Assertions to write (public API, contracts/public.yaml, through Traefik or a port-forward):
#   - POST /api/v1/resources with a seeded body and an Idempotency-Key -> 201 and a Location;
#     the body echoes type, location and window, the registered status, version 1, UTC timestamps.
#   - GET /api/v1/resources lists it (filter by type/status) and GET by ID returns the same body.
#   - An invalid body (lat/lng out of range, unknown enum, window ending before it starts) is
#     400 with the error envelope, and nothing is created.
#   - Updating availability/status (PUT /api/v1/resources/{id}/status, a partial update:
#     status AVAILABLE|OFFLINE and/or a new UTC window) -> 200 with the version incremented and
#     the change persisted; ASSIGNED or CONSUMED, or windowStart >= windowEnd, is 400 and
#     changes nothing.
#   - The stored record (coordinator /admin/locate/res:<id>, then a strong read of
#     /internal/data/res:<id>) has the validated state and location.
#
# Seed/reset: write it with demo_reset; start from the gate's clean deployment (reset.sh).
# Fill in the body below with lib.sh (run, check, pass, fail, demo_end); see demo-4.sh for a
# worked example. Until then this demo reports NOT IMPLEMENTED, which the gate counts as a
# failure.
. "$(dirname "$0")/lib.sh"
demo_begin 2 "Register a resource, list it, update its availability and status"
demo_not_implemented "owner A, goal A3.1"
