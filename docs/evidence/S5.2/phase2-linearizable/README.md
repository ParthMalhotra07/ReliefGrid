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
- `check.txt` is `scripts/load-report-check.py`, as changed in this branch, run on
  every run here.
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
- **Barriers vs uncounted commands.** The non-command entries are split using
  `storage_raft_fsm_apply_seconds_count`: entries that never reached `FSM.Apply` are
  barriers, no-ops or configuration entries; the rest are applied commands.
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

- **Saturation is above the measured range.** At the measured commit, the check exited
  3 on every step run ("INCONCLUSIVE saturation: no step breaks a threshold up to 80
  req/s; raise the top step"), with every other check passing. The run driver accepted
  exactly that case as `saturation-above-range` (`extras/phase2/*.acceptance.txt`).
  This branch moves that rule into the harness. `load-report-check.py` now passes a
  step run that breaks no threshold up to its top step as `saturation: > <top> req/s`,
  provided the report states no saturation point. A run whose first step already
  breaks a threshold is still INCONCLUSIVE (exit 3). `check.txt` is the updated check,
  and every run passes, including `saturation: > 80 req/s` on all three step runs.
- **The 29 non-command entries are not barriers.** In every shard of every Phase 2
  run, Raft entries equal FSM applies (growth of the highest
  `storage_raft_fsm_apply_seconds_count`), so 0 entries committed without reaching
  `FSM.Apply`. The term never changed (2 throughout), and storage reads in the window
  were 0 strong and 0 stale.
  - The 0-27 per shard are commands that had applied but were not yet counted by the
    receiving replica's command-latency counter. `metrics-end` is scraped the moment
    k6 stops, while the matcher and outbox are still writing.
  - No caller issued an explicit strong read inside the measured window.
  - The only caller in a load run that still asks for strong reads is `cmd/invcheck`
    (the audit, default `-consistency strong`), and it runs after `metrics-end`. The
    statuses read-back also runs after the window, through the API, whose default is
    linearizable since #166. `scripts/verify-reconcile.py` also reads strong, but it
    is verification only and is never part of a load run.
  - Phase 1, for scale: 270191 barriers per step run.
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
2. **Step-1's acceptance tail was run by hand.** Every harness stage of step-1 ran
   unattended under the driver:
   - `loadtest/run.sh step` (k6 load)
   - `loadreport build` (report, SHA256SUMS and spill)
   - the read-back, audit and invcheck
   - `load-report-check.py`
   - host restoration

   `run.sh` exited 3 (`runnerExit 3`, `reportExit 3`, `k6Exit 0`,
   `safetyFailure false`) on the saturation INCONCLUSIVE alone, and the driver
   stopped with the cluster kept. Only the driver's own acceptance tail then ran by
   hand, at 17:26:54Z, about 1 min after the report was built (17:25:54Z). It
   wrote nothing inside the run directory:
   ```
   grep -vE '^(PASS  |== )' run.require-saturation.log   # only the two saturation/report INCONCLUSIVE lines
   echo SATURATION_ABOVE_RANGE ... > saturation-above-range.txt
   jq -e '.safetyFailure == false and .teeExit == 0 and .k6Exit == 0 and .runnerExit == 3 and .reportExit == 3' run.runner-status.json
   jq -e '.ok == true and .verdict == "clean" and (has("incomplete") | not) and .report.quiescent == true' run/raw/invcheck.json
   jq -e '.invariants.ledger == "PASS" and .invariants.invcheck == "PASS" and .invariants.unreadable == 0
          and .audit.mode == "without-admission" and .audit.matcherReplicasAfter == .audit.matcherReplicas' run/summary.json
   tar ... docs/evidence/A1.5 | sha256sum   # equal to a15-before.sha256
   docker inspect <kind nodes> | cmp - owned-nodes.json   # the run's own cluster
   make cluster-down CLUSTER=reliefgrid
   tar ... docs/evidence/A1.5 | sha256sum   # still equal
   echo "PHASE2_RUN_ACCEPTED step-1 ..." > acceptance.txt
   ```
   Checksums (13 Nov):
   - In every run's original directory, `sha256sum -c SHA256SUMS` passes for every
     file the harness listed. No file in any run directory is newer than its
     SHA256SUMS.
   - The step runs' k6 points, which the harness spilled to
     `~/.cache/reliefgrid/S5.2/<runId>/`, match the sha256 in their `summary.json`.
   - Every committed file is byte-identical to the harness's, except `summary.json`
     (only its `raw` list, where the slim step marks off-repo files with their
     sha256) and that file's line in `SHA256SUMS`.
   - The remaining runs used the revised driver unattended.
3. **The Phase 1 idle control failed to start once** (`stage_timeout` rejects a 0 s
   stage). No cluster had been created yet. The idle stage now takes its 60 s idle
   window as its stage length. The rerun was accepted.

## Verification scope

`scripts/verify-S5.2.sh` reads only `docs/evidence/S5.2/*/summary.json`, one level
deep, so it still judges Phase 1. This directory is checked by `check.txt`.

## Exploratory step run to 160 req/s (n=1, audit refused)

`exploratory-160/` holds one exploratory run at 514bb33 plus the PR #170 harness
fixes (fa8c872). It is n=1 and has no safety verdict: the audit refused on queue
headroom after its 1200 s bound, so invcheck never ran. Reconstructed from raw k6
points, it puts saturation at 100 req/s, broken at 110 by list p99 (570 ms). See its
README. It does not change the results above.
