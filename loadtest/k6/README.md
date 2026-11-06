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

## Dataset

```sh
go run ./loadtest/cmd/gendata -seed 52 -run "$RUN" -ops 200 -out "$DIR/dataset.json"
```

`loadtest/dataset` fixes the mix: 15% request creates, 10% resource
registrations, 10% `POST /match/{id}`, 65% reads (55% `GET /requests/{id}`,
5% request listings, 5% resource listings). Locations fall in a 20 km disc,
inside the matcher's 50 km radius. The run name changes only the
Idempotency-Keys: `payloadSha256` is equal across runs of one seed.

## Run

```sh
loadtest/run.sh smoke          # SEED=52 OPS=200 K6_CPUS=7 K6=k6 (kind only)
```

`loadtest/run.sh` records the host, versions and configuration; generates the
dataset; scrapes every storage node's and the coordinator's `/metrics` before
and after (`kubectl port-forward`); samples every pod's cgroup on its kind
node every 5 s; runs k6 under `taskset`; lets matching settle; reads back every
request the run created; runs `invcheck check` as a Job; then builds and checks
`docs/evidence/S5.2/<runId>/summary.json` (`go run ./loadtest/cmd/loadreport`).
Raw files over 5 MiB move to `SPILL_DIR` (default `~/.cache/reliefgrid/S5.2`)
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
