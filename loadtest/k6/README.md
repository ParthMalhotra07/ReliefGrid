# S5.2 load scenarios

k6 scenarios for the S5.2 performance report, run from the host against the
public API on kind (Traefik, `http://127.0.0.1:8080`). Each iteration replays
the next operation of a seeded dataset, so two runs with the same seed offer
the same work in the same order; only the arrival rate comes from the
scenario.

| File | Scenario |
| --- | --- |
| `lib.js` | dataset replay, tags, error counting, the S5.2 thresholds |
| `smoke.js` | 0: one VU, 2 ops/s, 1 min. A correctness check and calibration for the later scenarios, not a result. |
| `step.js` | 2: the arrival rate climbs in steps (`START_RPS`, `STEP_RPS`, `STEPS`, `RAMP_S`, `HOLD_S`); each hold is one measured step, and the saturation point is the last step inside every threshold |
| `baseline.js` | 1: a constant `RATE` (about half the saturation point) for `DURATION_S` (600 s), the first `WARMUP_SECONDS` (120 s) not measured; the reference for every before/after |
| `soak.js` | 3: a constant `RATE` (about 70% of saturation) for 1800 s: log growth, snapshots and their cost under the load Raft profile (#33) |

## Dataset

```sh
go run ./loadtest/cmd/gendata -seed 52 -run "$RUN" -ops 200 -out "$DIR/dataset.json"
```

`loadtest/dataset` fixes the mix: 15% request creates, 10% resource
registrations, 10% `POST /match/{id}`, 65% reads (55% `GET /requests/{id}`,
5% request listings, 5% resource listings). Locations fall in a 20 km disc,
inside the matcher's 50 km radius. The run name changes only the
Idempotency-Keys: `payloadSha256` is equal across runs of one seed.

## Deploy and run

```sh
make cluster-down && loadtest/cluster.sh      # fresh kind, load overlays, every pod Guaranteed
loadtest/run.sh smoke
START_RPS=10 STEP_RPS=10 STEPS=8 loadtest/run.sh step
RATE=30 loadtest/run.sh baseline
go run ./loadtest/cmd/loadreport aggregate -out agg.json docs/evidence/S5.2/<run>...
```

`loadtest/cluster.sh` installs both charts with their `values-load.yaml`
overlays (never the defaults): the load Raft profile, and requests = limits on
every container, about 5.35 CPU in total. For each run, `run.sh` confines the
kind node containers to `KIND_CPUS` (0-5, `docker update --cpuset-cpus`), runs
k6 on `K6_CPUS` (6-7) and sets the power profile to `performance`. All three go
in the report, and one EXIT trap restores the cpusets and the previous power
profile whether the run passes, fails or is interrupted.

`loadtest/run.sh` records the host, versions and configuration; generates the
dataset; scrapes every storage node's and the coordinator's `/metrics` before
and after (`kubectl port-forward`); samples every pod's cgroup on its kind
node every 5 s; runs k6 under `taskset`; lets matching settle; reads back every
request the run created; audits without admission; then builds and checks
`docs/evidence/S5.2/<runId>/summary.json` (`go run ./loadtest/cmd/loadreport`).

The audit is demo 5's `audit_without_admission`. A WAITING request (the
dataset has fewer resources than requests) goes round the retry tiers for as
long as the matcher runs, each round a `RetryWorkflow` and an outbox mark, so
`invcheck` could never see the applied indexes still. A paused matcher
empties no queue, and the drain publishes into the same reject-publish queues,
so `run.sh` first lets the matcher keep consuming until every queue is at most
80% of its max-length (`HEADROOM_PCT`, limits read from the live broker),
for at most 600 s (`HEADROOM_WAIT_S`); past that bound the audit fails. It
then scales the matcher (which also runs the reconciler and the outbox relay)
to 0, publishes the handoffs already committed with
`scripts/demos/drain-outbox` (a refused publish is retried for up to 20 s),
runs `invcheck check` as a Job, and scales the matcher back. Hints left in
RabbitMQ stay there. The report's `audit` states the replicas before and
after, the headroom wait (`headroom`: threshold, bound, seconds waited, each
queue's limit and its depth when the pause began) and what was drained, with
the retries; a matcher that cannot be put back is a safety failure (exit 4).

`loadtest/background.py` samples the load k6 cannot see every 5 s, from before
k6 until the audit's pause (`raw/background.jsonl`): the broker's confirm
counters (publishes nacked by a full queue under `reject-publish`), every
queue's depth, and WAITING from the API's workflow walk. The report's
`background` gives, for the run, for after it (k6 stopped, retries still
running) and for each step: unconfirmed publishes, peak depth per queue and
WAITING, plus the sample taken at the pause.
Raw files over 5 MiB move to `SPILL_DIR/<runId>/` (default `~/.cache/reliefgrid/S5.2`; an existing file there is never replaced)
and are recorded by size and sha256. `raw/requests.csv.gz`, one line per
request, is always committed.

Report figures:

- **Classes:** write, read and list p50/p95/p99, from k6's raw points in the measured window.
- **Errors:** by HTTP status and JSON code.
- **Match:** matched and waiting counts. Latency covers matched requests only:
  server-side `updatedAt - createdAt`, which includes any time spent waiting
  for a compatible resource.
- **Raft:** commit p50/p99 per shard, from the histogram deltas, plus snapshots
  taken and peak log and snapshot bytes.
- **Pods:** peak CPU (a 5 s average) and mean CPU, peak memory and CPU
  throttling per pod.
- **Ledger:** every acknowledged create reads back 200.
- **Steps:** each step lists the pods running at their CPU limit (at least 90% of
  it, or throttled in at least 20% of CFS periods). The saturation point names
  the first step at which any pod reached its limit, so the curve says which
  component saturated first.

Repeated runs of one scenario and seed are reported as median plus min/max
(`loadreport aggregate`). Matching is asynchronous, so matched and waiting
counts vary between runs even though each run offers the same work.

k6 is pinned in `tools.mk` (`K6_VERSION`). Run it with `taskset` on cores the
cluster is not using, and record them (`host.k6Cpus` in the report).

## Tags and outputs

- Every request has `class` (`write`, `read` for one key by ID, or `list`
  for a listing, which scans every shard), `op` (the dataset kind) and a
  fixed `name` per route. The summary has no per-URL series.
- `api_errors{status,code,op}` counts every non-2xx answer by its JSON error
  `code` (`TRANSPORT` when there was no answer).
- `created_request{request_id}` is in the raw output only. It is never used
  as a threshold or summary key. The post-run collector reads each request's
  final status from it, to count matched and waiting requests separately.
- `skipped_ops` counts matches and reads before the VU had created a request.

Thresholds: writes p99 <= 1 s, reads by ID p99 <= 250 ms, listings p99 <= 500 ms
(reported separately from reads), failed requests <= 1%,
no dropped iterations. The report (`docs/evidence/S5.2/<runId>/summary.json`)
is checked by `scripts/load-report-check.py`; `scripts/verify-S5.2.sh` runs
it over every run.
