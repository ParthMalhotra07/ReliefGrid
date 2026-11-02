# S4.3 re-judge of the saved suite under the coverage rules (#85 review)

**The 11 passing results below are re-judged from rebuilt inputs, not a new chaos run.**
Only restart-coordinator was re-run on kind (see "restart-coordinator re-run"). The judge now
needs post-grace coverage and every targeted replica probed. This directory applies it
to the 12 reports in `../suite-20261101T112942Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/reports`.
The raw ledgers those runs wrote are on the cluster's ledger PVC and were not read again.
So each judge input is rebuilt from its saved report, and the three new counters are
filled with lower bounds that the report proves:

| counter (faulted shard, in [from+grace, to]) | lower bound used | why it holds |
|---|---|---|
| `writeAttemptsInsideFault` | `acksInsideFault`, plus the load's failed attempts in `errorsObserved` inside the interval when this is the only shard with `errorsDuring > 0` | each ack that started after the grace and came by the heal is an attempt that ended inside; each failed attempt is logged with its time |
| `readsInsideFault` | `readsDuring - 41 × (readers + probes)`, at least `readsServedInsideFault` | every read loop sleeps 50 ms between reads, so it starts at most 2000/50 + 1 = 41 reads in the 2 s grace |
| `probeReadsInsideFault[p]` | `probeReadsDuring[p] - 41`, at least `probeServedInsideFault[p]` | the same bound for one probe loop |

The other fields are copied from the report (`build.jq`). The rebuild was checked
first: the judge before this change, run on the rebuilt inputs, gives each run its
saved verdict (column "old judge").

## Result: 11 of 12 saved reports pass; restart-coordinator re-run on kind and passes (see below)

| run | saved | old judge, rebuilt | new judge | post-grace ms | writes ≥ | reads ≥ | probes ≥ |
|---|---|---|---|---|---|---|---|
| kill-leader | pass | pass | pass | 2341 | 23 | 32 | 1-0: 19 |
| kill-follower | pass | pass | pass | 9657 | 178 | 127 | 2-2: 127 |
| restart-shard | pass | pass | pass | 3162 | 32 | 48 | (not judged) |
| isolate-replica | pass | pass | pass | 18962 | 534 | 637 | 0-1: 355 |
| isolate-leader | pass | pass | pass | 18537 | 435 | 522 | 1-2: 349 |
| delay-isolate-leader | pass | pass | pass | 18602 | 109 | 320 | 1-0: 351 |
| lose-quorum | pass | pass | pass | 18037 | 16 | 993 | 2-0: 348, 2-1: 339, 2-2: 348 |
| delay-follower | pass | pass | pass | 24330 | 682 | 819 | (not judged) |
| stop-worker | pass | pass | pass | 28967 | 642 | 318 | (not judged) |
| control-lost-write | pass | pass | pass | 9666 | 252 | 208 | (control) |
| control-stale-reader | pass | pass | pass | 24131 | 350 | 519 | (control) |
| **restart-coordinator** | pass | pass | **FAIL** | **-1141** | 0 | 0 | — |

Probe names are shortened: `2-0` is `storage-shard-2-0`. Writes and reads are lower
bounds on the faulted shard. stop-worker and restart-coordinator also need coverage on
every shard; for stop-worker those bounds pass too (`results.jsonl` holds only the
faulted shard's). Negative controls are judged on whether they caught their fault, not
on coverage.

**restart-coordinator:** `coverage: no observation window after the grace: the fault
lasted 859ms, the grace is 2000ms`. Deleting the pod healed in 859 ms, inside the 2 s
grace, so the saved PASS judged an empty interval. It was re-run on kind, below. The
other 11 have not been re-run; their counters remain the lower bounds above.

## restart-coordinator re-run (seed 440, the saved run's seed)

The harness now scales the coordinator to 0 and holds it for the grace plus
`COORDINATOR_DOWN_SECONDS` before scaling back. These runs have their own ledgers, so
their counters are real values, not lower bounds.

| run | hold | after the grace | write attempts per shard | reads per shard | judged |
|---|---|---|---|---|---|
| `../suite-20261101T144808Z-…` | 10 s | 10.2 s | 5 / 4 / 5 | 4 / **2** / **1** | FAIL (reads) |
| `../suite-20261101T163336Z-…` | 30 s | 30.3 s | **2** / 18 / 21 | 5 / 13 / 5 | FAIL per shard; **PASS** across shards (41 ≥ 9) |

Every other criterion passed in both runs: same ring before and after, leader terms
unchanged (2/2/2), no new bootstrap Job, every shard acknowledged writes again after the
restart, 0 lost, 0 stale, and every unknown outcome resolved by operation ID as never
applied (28 and 13).

Why the counts are low: every data call goes through the coordinator
(`coordclient` → `storage-coordinator/internal/data`), so while it is down each call
waits out `coordclient.DefaultTimeout` (5 s; the slowest read in both runs took 5000.7–5001.2 ms).
Two readers make about 0.7 reads/s across all shards, so 10 s was too short for 3 per
shard and 30 s is enough. Each write worker retries one operation up to four times, so
the four workers moved through only one or two keys each during the outage. Which shards
those keys hash to is chance: shard 0 got 2 attempts.

So the judge counts restart-coordinator's **write attempts across shards**, needing at
least `MinCoverage × shards` = 9 in total. The fault is the shared routing path, not a
shard. Reads are still held to 3 per shard, and every shard must still acknowledge
writes again after the restart. The other experiments keep the per-shard write minimum.
`restart-coordinator-30s.json` is the 30 s run re-judged under this rule from its own
counters (input rebuilt with `build.jq`, availability taken from the report as is): the
judge before this rule reproduces the run's saved failure, and the new judge passes it.
The cluster was not re-run for this.

## Measured: a coordinator outage means zero availability and no loss (input for #31)

| | 10 s hold | 30 s hold |
|---|---|---|
| outage (scale to 0 → scale back) | 12.2 s | 32.3 s |
| acks before the fault, all shards | 69.3/s | 69.5/s |
| acks after the grace, per shard | 0 / 0 / 0 | 0 / 0 / 0 |
| reads served after the grace, per shard | 0 / 0 / 0 | 0 / 0 / 0 |
| slowest client call | 5.0 s (timeout) | 5.0 s (timeout) |
| first ack after the scale-up | 3.8–3.9 s | 2.6–2.7 s |
| lost / stale | 0 / 0 | 0 / 0 |
| unknown outcomes (all never applied) | 28 | 13 |

With a single coordinator, every client call waits out the 5 s timeout while it is down,
so availability is zero for the whole outage plus about 3 s, while nothing acknowledged is
lost. The Raft groups never noticed: terms did not change.

## Files

- `build.jq`: saved report to judge input, with the bounds above.
- `restart-coordinator-30s.json`: the 30 s re-run, re-judged with writes counted across shards.
- `results.jsonl`: one line per run: saved verdict, old judge on the rebuilt input, new
  verdict and failures, and the faulted shard's rebuilt counters.

Reproduce from the repo root:

```sh
R=docs/evidence/S4.3/suite-20261101T112942Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/reports
go build -o /tmp/invcheck ./cmd/invcheck
for f in $R/*.json; do jq -f docs/evidence/S4.3/rejudge-20261101/build.jq "$f" |
  /tmp/invcheck judge --input - | jq -c --arg run "$(basename "$f" .json)" '{run: $run, pass, failures}'; done
```
