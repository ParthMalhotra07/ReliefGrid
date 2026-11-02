#!/usr/bin/env bash
# demo-2 (owner A, goal A3.1): v3.1 §28 row 2.
#
# What to show:   Register resource, list it, update availability/status
# Acceptance:     Resource persists with validated state and location
#
# Assertions to write (public API, contracts/public.yaml, through Traefik or a port-forward):
#   - POST /api/v1/resources with a seeded body and an Idempotency-Key -> 201 and a Location;
#     the body echoes type, location and window, the registered status, version 1, UTC timestamps.
#   - GET /api/v1/resources lists it (filter by type/status) and a strong internal read by key returns the persisted state.
#   - An invalid body (lat/lng out of range, unknown enum, window ending before it starts) is
#     400 with the error envelope, and nothing is created.
#   - Updating availability/status (PUT /api/v1/resources/{id}/status, a partial update:
#     status AVAILABLE|OFFLINE and/or a new UTC window) -> 200 with the version incremented and
#     the change persisted; ASSIGNED or CONSUMED is 409 (claim-managed); windowStart >= windowEnd is 400; each
#     changes nothing.
#   - The stored record (coordinator /admin/locate/res:<id>, then a strong read of
#     /internal/data/res:<id>) has the validated state and location.
#
# Seed/reset: write it with demo_reset; start from the gate's clean deployment (reset.sh).
# Executed through app.sh and app_demo.py. The shared report retains the seed,
# assertions, inputs, HTTP outcomes and bounded recovery observations.
. "$(dirname "$0")/lib.sh"
demo_begin 2 "Register a resource, list it, update its availability and status"
# Shared preflight and durable evidence wrap the executable application assertions.
. "$(dirname "$0")/app.sh"
app_demo 2
