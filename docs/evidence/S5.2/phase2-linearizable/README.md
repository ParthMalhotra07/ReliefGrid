# S5.2 Phase 2: linearizable reads by default (#166), before/after

Phase 1 (`../S5.2-*`) measured the system at 7dd1940, where a read with no mode was a
strong read: one Raft barrier per read. Phase 2 measures the #166 head, 514bb33 (tag
`s5.2-phase2-measured`), where the same reads take the S4.1 read fence: no log entry
per read. `comparison.md` holds the full tables, and `comparison.json` the same data.

## Method (same as Phase 1)

- `loadtest/run.sh` at 514bb33, seed 52. Each run starts on a fresh kind cluster,
  created and torn down by the run.
- kind on CPUs 0-5, k6 on CPUs 6-7. Power profile `performance` during the runs,
  restored to `balanced` after. Laptop on AC, with sleep inhibited.
- Guaranteed pods, Raft profile `load`, api 2 and matcher 2 replicas.
- Phase 1's rates, not re-derived (`rates.json`):
  - 3 step runs, 10 to 80 req/s in 8 steps (10 s ramp, 110 s hold each).
  - 3 baselines at 25 req/s.
  - No soak and no pilot.
- Stop conditions: a failed audit, invcheck or restore stops the sequence. None
  occurred. Every run's invcheck is clean, and its audit and restore pass.
- Slim layout as in Phase 1 (01feb5b): k6 points, datasets, statuses, cgroup samples
  and metrics scrapes stay off-repo. `summary.json` gives each one's bytes, sha256 and
  location, and `SHA256SUMS` still lists them. `tools/slim.py` applies this.
- `check.txt` is `scripts/load-report-check.py` on every run here.
- `aggregate-*.json` come from `loadreport aggregate`.

## Additions to the usual report

- **Raft entries vs FSM commands.** Per shard, the growth of the shard's highest
  `storage_raft_commit_index` over the k6 window, minus the growth of
  `storage_command_latency_seconds_count` summed over its replicas. The remainder
  ("non-command") is barriers, no-ops and configuration entries. Reads come from
  `storage_read_latency_seconds_count{consistency}`.
- **Idle.** Every Phase 2 run takes two scrapes 60 s apart after deploy and before
  load (`extras/phase2/*.json` `.idle`). The Phase 1 commit 7dd1940 got one
  deploy-only idle sample (`extras/phase1/p1-idle.json`) as its control. Phase 1's own
  runs had no idle sample.
- **Coordinator CPU per step.** From the run's `raw/cgroup.jsonl`, over each step's
  hold window. For baselines, the window starts after warm-up.
- Tools: `tools/extras.py`, `tools/compare.py` and `tools/compare-config.json`.
  `compare.py` recomputes the per-step percentiles from `raw/requests.csv.gz`, using
  linear interpolation, and asserts that every p99 equals the report's.

## Results (medians of 3; ranges in `comparison.md`)

| | Phase 1 (strong) | Phase 2 (linearizable) |
|---|---|---|
| Saturation point | 50 req/s (60 broken by write p99, 3/3) | above 80 req/s (no step broken, 3/3) |
| Non-command Raft entries, step run (all shards) | 270196 (79% of the log) | 29 |
| Non-command entries at idle, per shard | 6.2/s (≈ 1 per strong read) | 0 |
| Baseline write p50/p95/p99 (ms) | 136 / 426 / 684 | 32 / 75 / 94 |
| Baseline read p50/p95/p99 (ms) | 41 / 68 / 80 | 2.5 / 31 / 56 |
| Baseline list p50/p95/p99 (ms) | 130 / 234 / 291 | 18 / 72 / 90 |
| Step 80 req/s write p99 (ms) | 1180 | 389 |
| Baseline error rate | 0.001 | 0.00017 |
| Step-run broker nacks | 6324 | 8 |
| Step-run `matching.wakeups` peak (limit 10000) | 10016 | 159 |
| Step-run `matching.requests` peak (limit 10000) | 10020 | 9951 |
| First pod at its CPU limit | coordinator @ 20 req/s | coordinator @ 20 req/s |
| Coordinator CPU at baseline (m, limit 250) | 120 | 217 |

Notes:

- **Saturation is above the measured range.** `load-report-check.py` exits 3 on the
  step runs: "no step breaks a threshold up to 80 req/s; raise the top step". This is
  the expected outcome of the change, not a failure. Every other check passes. Each
  step run's acceptance records `saturation-above-range`
  (`extras/phase2/*.acceptance.txt`).
- **The coordinator is the next limit.** It sits at 98% of its 250 m limit, throttled
  87-95% of periods, from 50 req/s upwards. It forwards every storage call, and in
  Phase 2 it forwards about 2.1x as many per baseline run (about 407k against 194k).
  Its CPU per forwarded call is unchanged or slightly lower. The extra calls are
  background work that now completes inside the window: in Phase 1 the matcher fell
  behind (`matching.requests` peak 3.6k at baseline, against 60 in Phase 2).
- **`matching.requests` is near its limit at the top steps.** It still rose to
  9922-9960 in every step run. It only drops 6-33 publishes at 80 req/s, against
  about 5.5k in Phase 1, but it is the queue to watch once the step range is raised.
- **Refusals are global.** The reports count broker nacks (`publishesNotConfirmed`)
  across all queues, not per queue. The wakeups and requests peaks are given alongside
  so the two queues can be told apart.
- **Per-step counts can be slightly negative.** A per-step `publishesNotConfirmed` of
  -1 to -4 appears in both phases. It is the harness's own per-step difference, taken
  between samples, and is reported as it is.

## Deviations, in order

1. **Attempt 1 stopped before load** (preserved off-repo as `step-1-failed-1`). The
   idle-scrape check counted 10 metrics files, not 9, because the storage label
   selector also matches the coordinator pod. The count was restricted to
   `storage-shard-*.prom`, the cluster was torn down, and the sequence was restarted.
2. **Step-1 was finished by hand.** It exited 3 (saturation above range) with every
   other gate passing. The driver's acceptance was revised to accept exactly that
   case. Step-1 was finished with the revised tail: checks, then teardown. The
   remaining runs used the revised driver unattended.
3. **The Phase 1 idle control failed to start once** (`stage_timeout` rejects a 0 s
   stage). No cluster had been created yet. The idle stage now takes its 60 s idle
   window as its stage length. The rerun was accepted.

## Verification scope

`scripts/verify-S5.2.sh` reads only `docs/evidence/S5.2/*/summary.json`, one level
deep, so it still judges Phase 1. This directory is checked by `check.txt`.
