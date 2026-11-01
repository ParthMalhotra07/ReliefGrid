#!/usr/bin/env bash
# demo-5 (owner A, goal A3.1): v3.1 §28 row 5.
#
# What to show:   Submit a burst; show RabbitMQ ready/unacked counts, matching and waiting results
# Acceptance:     Events processed asynchronously; after bounded quiescence, outbox and invariants pass
#
# Assertions to write (plan A3.1: the 500/50 burst fixture):
#   - Seed 50 resources, then submit the 500-request burst; record the API's 201s.
#   - While it drains, record RabbitMQ ready/unacked counts per queue (management API) at a
#     fixed interval; ready > 0 at some point shows the processing was asynchronous.
#   - Within a bounded quiescence wait, every request is in a stable state (MATCHED or
#     WAITING), ready and unacked return to 0.
#   - invcheck check --max-backlog 0 exits 0: no invariant violations, outbox drained.
#
# Seed/reset: write it with demo_reset; start from the gate's clean deployment (reset.sh).
# Executed through app.sh and app_demo.py. The shared report retains the seed,
# assertions, inputs, HTTP outcomes and bounded recovery observations.
. "$(dirname "$0")/lib.sh"
demo_begin 5 "Submit a burst; show RabbitMQ queue counts, matching and waiting results"
# Shared preflight and durable evidence wrap the executable application assertions.
. "$(dirname "$0")/app.sh"
app_demo 5
