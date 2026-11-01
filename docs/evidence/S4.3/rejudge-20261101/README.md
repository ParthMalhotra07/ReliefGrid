# S4.3 re-judge of the saved suite under the coverage rules (#85 review)

**These are re-judged results from rebuilt inputs, not a new chaos run.** The judge now
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

## Result: 11 of 12 pass; restart-coordinator fails (pending a re-run)

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
grace, so the saved PASS judged an empty interval. The harness now scales the
coordinator to 0 and holds it down for the grace plus `COORDINATOR_DOWN_SECONDS` (10 s)
before scaling back. **Its re-run on kind is pending**, and so is a fresh suite whose
ledgers carry the real counters instead of these bounds.

## Files

- `build.jq`: saved report to judge input, with the bounds above.
- `results.jsonl`: one line per run: saved verdict, old judge on the rebuilt input, new
  verdict and failures, and the faulted shard's rebuilt counters.

Reproduce from the repo root:

```sh
R=docs/evidence/S4.3/suite-20261101T112942Z-hrishabh-sharrma-ThinkPad-E14-Gen-7/reports
go build -o /tmp/invcheck ./cmd/invcheck
for f in $R/*.json; do jq -f docs/evidence/S4.3/rejudge-20261101/build.jq "$f" |
  /tmp/invcheck judge --input - | jq -c --arg run "$(basename "$f" .json)" '{run: $run, pass, failures}'; done
```
