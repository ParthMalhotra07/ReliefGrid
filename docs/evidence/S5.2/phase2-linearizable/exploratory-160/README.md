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

## Why the backlog cannot drain

**Diagnosis (Hrishabh):**
- Requests outnumber resources, so about a third of requests stay WAITING.
- Every WAITING request keeps a retry message cycling. `loadtest/background.py`
  documents the same behaviour: "every WAITING request goes round the retry tiers".
- So after a large run, the messages returning to `matching.requests` stay above
  the 8000 threshold (80% of 10000). The audit cannot find headroom, however long it
  waits.

**Checked against the numbers:**

| | This run | Phase 2 step runs (each) |
|---|---|---|
| Dataset requests / resources | 25797 / 16949 | 6865 / 4495 |
| Excess requests, 1 − resources/requests | 34.3% | 34.5% |
| Requests created / WAITING at read-back | 24183 / 10895 | 6528-6529 / 2252-2253 |
| WAITING at the headroom decision | 8242 (34.1% of created), at the refusal, 06:25Z | 1976-2190, at the pause, headroom found at once |
| `matching.requests` depth at that point | 23936 = 7783 queued + 16153 returning | 2915-3662 (ready + unacked only, the old rule) |
| Messages per WAITING request | 2.9 (returning alone: 2.0) | 1.3-1.9 (ready + unacked only) |

- **The numbers support the diagnosis.** The share left WAITING (34.1%) matches the
  resource deficit the dataset sets (34.3%). With 8242 WAITING, even one cycling
  message per WAITING request puts the queue above 8000. A longer wait cannot help:
  WAITING fell from 10895 at the read-back (45% of created) as retries
  matched, then levelled off at 8219-8242 for the last 6 min of the wait
  (06:19:31-06:25:22Z). That is the size of the resource deficit, where matching
  stops. Meanwhile the queue total stayed near 24k. The background `waiting` gauge
  is stale (`waitingValid: false`) before about 06:13Z, so only the read-back and
  the later valid samples are used. The Phase 2 runs passed
  because their WAITING count (about 2.2k) keeps the floor far below 8000.
- **One refinement.** At the refusal there were 2.9 messages per WAITING request
  (16153 returning alone is 2.0 per request), not 1. So the backlog is about 3 times
  what one-per-request predicts. The surplus has not been diagnosed: it could be
  duplicates from at-least-once dead-lettering, or wakeup re-publishes.
- **What it implies.** By the floor alone, any run that leaves more than about 8000
  WAITING cannot pass this audit; at this dataset's ratio, that is about 23.5k
  created requests. At the observed 2.9 per WAITING, the limit is about 2.8k
  WAITING, or about 8k created. The 16-step run created 24183.

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
