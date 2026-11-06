# A4.2 — application acceptance and pending storage handoff

**Application acceptance: PASS. Full A4.2: INCOMPLETE (S-owned #113).**

Base main: `dd1ea8e` (#100). Application code commit: `090066f`.
The exact final source is identified by `source-sha256.json`; deployment image
IDs and the preserved broker PVC are recorded alongside it. Monitoring used the
private observability Helm release on the local five-node kind cluster.

## Executed checks

| Check | Observed result | File |
| --- | --- | --- |
| `make check` | PASS: lint 0 issues, default and chaos-tagged race tests green | `check.txt` |
| `make gen-check` | PASS: no generated contract changes | `gen.txt` |
| `make verify-A4.2-static` | PASS: race tests, private chart shape, 3 dashboards and exact Prometheus promtool syntax validation | `static-verify.txt` |
| Metrics tests repeated with `-race -count=3` | PASS | `metrics-race-repeat.txt` |
| API, matcher, public API and workflow integration packages under `-race` | PASS | `integrated-race.txt` |
| Production local-kind application probe and Grafana capture | APPLICATION_PASS | final `live-*/results.json`, `live-application.txt` |
| Storage/coordinator families and leader-kill health/lag/age acceptance | INCOMPLETE, not claimed | `full-goal-status.json` |

The live probe discovered all four API/matcher exporters individually and the
seven canonical broker queues. All three dashboards and every PromQL expression
evaluated. It created a synthetic resource/request, observed real asynchronous
REST → outbox → RabbitMQ → matcher completion and histogram growth, then observed
durable cancellation and an increment from the previous cancellation counter.
A second no-candidate request reached WAITING; its age grew by at least two
seconds across complete observations before screenshot capture. Requests were
cancelled on exit. Metric labels never include their entity identifiers.

The screenshots show the actual private Grafana service. Missing S-owned storage
metrics are labelled **No observation**. They are not zero placeholders or a claim
that full storage recovery has been verified.

## Red-first cases

- `red-private-listener.txt`: main had no configured private metrics listener.
- `red-parent-cancellation.txt`: an initial private server outlived parent
  cancellation and could consume the public API's pod-drain budget.
- `red-chart-ports.txt`: application workloads had no metrics port/environment.
- `red-commit-latency.txt`: an initial histogram ended at the proposal timestamp,
  losing a measured two-second commit/response interval (30 versus 32 seconds).

The final tests retain rejection/replay/unknown-outcome distinctions, exercise a
real 101-record paged FSM scan, fail a later page/partial shard observation, and
verify that failed scans preserve the previous complete count/age and timestamp.
High-cardinality input collapses into fixed label buckets. The API process test
uses bound port-zero listeners and checks public `/metrics` remains 404.

## Deployment findings resolved

The monitoring pins use the verified Linux/amd64 manifests for Prometheus
v3.16.0 and Grafana 13.3.0. Digest-only Docker exports created unnamed kind import
references; the loader now archives a named tag made from the verified immutable
digest, while pod images remain digest-pinned. Only the two new broken Prometheus
import metadata references were removed on the dev infra worker; no application
or storage volumes were reset.

Grafana's default plugin preinstall attempted unpinned downloads and filled its
bounded emptyDir. Preinstall and its auto-update are disabled. The final release
became Ready without raising a disk limit or weakening a test.

## Pre-PR review

**Draft: application portion ready for review; NOT READY for full-goal merge.**

1. No FSM/ownership/handoff rules change. Store wrappers delegate commands,
   identities, timestamps, errors and results unchanged. Metric observations
   cannot grant ownership or acknowledge a delivery.
2. Match latency/conflict samples exclude replayed results. Invocation counters
   explicitly count observations, not a durable unique-request ledger.
3. Remote observation walks are bounded, outside scrape/mutation paths. Parent
   cancellation closes and joins the private listener/sampler independently of
   public API readiness propagation and drain. Race checks pass.
4. Unknown outcomes remain distinct. Partial/failed reads mark cached data
   invalid, preserve last complete values and do not become a fresh empty scan.
5. Real red outputs and final local/live results are retained. The full-goal
   verifier explicitly refuses to pass without the storage/fault handoff.
6. Only A-owned application/Helm/dashboard code, shared build files and goal
   documentation change. Frozen contracts, S-owned source and .github are not
   modified. #113 tracks the missing S-owned exporters/policy.
7. Commits are Parth-authored; generation, formatting and lint pass. No real
   secrets, kubeconfigs, local progress files, plan PDFs or attribution trailers
   are staged. Keep only one compact final screenshot round; prior development
   evidence is archived locally outside the repository.

Next for full completion: confirm the S metric names/labels and private scrape
access from #113, wire the observed leader-kill fault stage, verify health/lag/age
changes and recovery, then re-run full acceptance before ticking A4.2.
