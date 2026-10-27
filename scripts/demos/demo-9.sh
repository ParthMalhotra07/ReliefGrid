#!/usr/bin/env bash
# demo-9 (owner A, goal A3.1): v3.1 §28 row 9.
#
# What to show:   Scale API 2 to 6 replicas under traffic
# Acceptance:     New pods become ready and receive requests through the Service
#
# Assertions to write (plan A3.1: manual scaling with the HPA off, 30 s tolerations on app pods):
#   - Under seeded traffic through the Service, scale the API Deployment from 2 to 6.
#   - All 6 pods become Ready within a bounded time.
#   - Each new pod serves requests through the Service (per-pod request counts or a response
#     header naming the pod), not only the original two.
#   - No error rate beyond the recorded expected window; scale back to 2 at the end.
#
# Seed/reset: write it with demo_reset; start from the gate's clean deployment (reset.sh).
# Fill in the body below with lib.sh (run, check, pass, fail, demo_end); see demo-4.sh for a
# worked example. Until then this demo reports NOT IMPLEMENTED, which the gate counts as a
# failure.
. "$(dirname "$0")/lib.sh"
demo_begin 9 "Scale the API from 2 to 6 replicas under traffic"
demo_not_implemented "owner A, goal A3.1"
