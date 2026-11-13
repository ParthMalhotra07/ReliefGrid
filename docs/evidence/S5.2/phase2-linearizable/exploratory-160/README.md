# Exploratory step run to 160 req/s (n=1), audit refused

**Exploratory, n=1. Unvalidated: no harness report and no safety verdict.** The
audit refused at its headroom step, so invcheck, `facts.json` and `summary.json`
were never produced. Nothing here is checked by `load-report-check.py` or
`verify-S5.2.sh`.

## What ran

- Commit fa8c872 (tag `s5.2-phase2-explore-measured`): 514bb33 (#166) with fefbea4
  (k6 dataset parsed once, in its SharedArray) and b99c77d (headroom counts parked
  retries) cherry-picked on top. Both are from PR #170.
- `loadtest/run.sh step` was run under the Phase 2 driver, on a fresh kind cluster,
  with Phase 2's method: CPU isolation, power profile `performance`, seed 52, MAX_VUS 400.
- 16 steps of 10 s ramp + 110 s hold, from 10 to 160 req/s by 10 (`params.txt`).
- The run-stage deadline was 4020 s (`run-timeout-seconds.txt`): the usual formula plus
  600 s for the longer headroom bound.
- **The headroom bound was 1200 s for this run only.** The bound is recorded in
  `raw/headroom.json` (`boundSeconds`). `facts.json` was never written, because the
  run stopped before the report stage.

## Outcome

- k6 exited 99 because thresholds were crossed. The settle and read-back completed:
  24183 created, 0 unreadable (`raw/statuses-summary.json`).
- The audit waited 1213 s for headroom and refused (`raw/headroom-check.txt`):
  `matching.requests 23936 > 8000 (80% of 10000; 7783 queued + 16153 returning from retry.120s,retry.30s,retry.5s)`.
- `run.runner-status.json` shows `runnerExit 1`, `k6Exit 99`, `reportExit null`,
  `safetyFailure false`. CPU pinning and the power profile were restored, and the
  cluster was kept.
- `diag-broker-queues.txt` is a broker snapshot taken 40 s after the stop. It shows
  the same backlog.
- k6 peak RSS was 438 MB, and host available memory never fell below 19319 MB
  (`hostmem-summary.json`, 10 s samples).

## Per-step figures (`explore-raw.json`)

These were reconstructed with `../tools/explore_raw.py` from the raw k6 points, the
cgroup and the background samples. The raw files are off-repo, and their sha256 is in
`SHA256SUMS.harness`:

```
explore_raw.py run 10 10 16 10 110 pod-cpu-limits.json
```

`pod-cpu-limits.json` holds the limits read from the kept cluster after the run.

Dropped iterations at 120–160 req/s came from the MAX_VUS cap, according to k6's log
("Insufficient VUs, reached 400 active VUs"). Under the agreed rule, those steps are
limited by the load generator, not system saturation.

| | |
|---|---|
| Saturation point | 100 req/s |
| Broken at 110 by | list p99 570 ms (> 500); write p99 867, read p99 145, errors 0.16%, 0 dropped |
| First component at its CPU limit | coordinator, 92% at 30 req/s, 97-98% from 40 |
| First queue at its limit | `matching.requests`, 9983 at 80 req/s; full, with 3306 nacks, at 110 |
| From 110-120 req/s | storage shards 2 then 0 at 93-100% of their CPU limit |

## Files

- Harness outputs, copied unchanged:
  - `dataset-info.json`, `k6-summary.json`
  - `raw/headroom.json`, `raw/headroom-check.txt`, `raw/queue-limits.json`,
    `raw/queue-routes.json`, `raw/statuses-summary.json`, `raw/k6.log`
  - `run.runner-status.json`
  - `run.runner.log`, with `$HOME` replaced by `~`
- Off-repo, under `~/.cache/reliefgrid/S5.2-explore2-20261113T052633Z/explore-160/run/`,
  with checksums in `SHA256SUMS.harness`:
  - `dataset.json`
  - `raw/k6-points.jsonl.gz`, `raw/cgroup.jsonl`, `raw/background.jsonl`,
    `raw/statuses.jsonl`
  - the metrics scrapes
