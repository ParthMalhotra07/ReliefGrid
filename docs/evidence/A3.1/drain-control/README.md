# Controlled pending-handoff check

Separate from the 500/50 burst fixture. Executed after the two complete clean
rounds. Workers are stopped; one synthetic WATER resource is created to commit
one known outbox event; the actual production relay-only helper must publish
and mark exactly one event, leave every strong outbox page empty, and pass
invcheck. Worker replicas are restored by finally. No consumer hint is purged
or acknowledged. No cancellation/fulfillment is used.

Actual helper output: {"Read":1,"Published":1,"Marked":1}. The raw before/after
pages, original inputs, helper output, invariant report and executed scripts
are retained here. The shared-framework report is at
../../A3.1-20261101-drain-control/demo-5.txt.

Reproduce on a fresh/quiescent deployed kind-reliefgrid cluster, from repo root:
`bash docs/evidence/A3.1/drain-control/verify.sh`. It uses a fresh run directory.
The verifier fails if there is existing undrained work before the seeded event.
The original executed.sh retains this machine's exact paths; verify.sh uses
repository-relative artifact paths. Credentials are read into environment only.
