# Local autoscaling (A4.3)

Real-workload acceptance passed; owner review/merge is pending. See docs/evidence/A4.3.

The opt-in kind profile installs one API CPU HPA(2–6,60%) and one KEDA-owned
matcher HPA(1–8,4messages/replica). HTTP queue depth includes ready and unacked
messages. Both use120s downscale stabilization. Helm omits their replica fields
while enabled; disabled/default profiles retain fixed API2/matcher2 for demo9.
Metrics-server/KEDA charts and Linux/amd64 images remain checksum/digest pinned.
KEDA watches reliefgrid and reads only the existing broker Secret in that
namespace. The scaler uses private15672, with username/password Secret refs.

Commands: make autoscaling-install; make autoscaling-up;
make verify-A4.3-static; make verify-A4.3; make autoscaling-down.
Every mutation explicitly selects kind-${CLUSTER:-reliefgrid}. An app-owned
storage release is refused. No reset, PVC deletion or cloud operation runs.

The revised k6 workload bounds concurrent lifecycles:32VU heavy arrival for90s,
then1VU real tail traffic from120–420s. Each request must observe a committed
handoff, verify a reciprocal match where applicable, cancel, and observe a
claim-free terminal state. A separate48VU API read phase runs120–210s. The
documented125s reconciler walk is allowed180s for terminal observation. The
verifier's780s post-start bound includes k6 grace and HPA stabilization.

Acceptance requires both Ready scale-ups/down, no matcher downscale during the
heavy window, complete load success, no observed app restarts, drained queues
and every removed pod's clean exit before its grace deadline. Both API and
matcher exits must be observed during real tail traffic. Missing statistics or
missing final pod statuses fail. No manual replica change creates the result.

The latest accepted run includes709 real lifecycles, full scale-up/down and11
observed clean exits. Cancellation retries only a documented known VERSION_CONFLICT
after an authoritative reread (maximum3 attempts); every conflict is retained in
the k6 log. Domain conflicts, unknown outcomes, malformed responses and exhausted
retries fail. Transport thresholds remain strict for unexpected errors.

This is not a throughput benchmark and does not certify A4.2. Runtime chart
dependencies #129/#133 are recorded explicitly in the evidence. Restore manual
settings with autoscaling-down before a course gate.
