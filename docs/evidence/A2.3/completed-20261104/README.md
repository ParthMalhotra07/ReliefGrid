# A2.3 completed schedule acceptance

Run ID: `full-seams-isolated-20261104`. All 50 seeds passed under the race detector:

| Backend | Physical cases per seed | Passed executions | Go test duration |
| --- | ---: | ---: | ---: |
| Real-FSM M8 | 17 | 850 | 103.648s |
| kind through the production coordinator client | 18 | 900 | 5913.169s |

SCH13a is transfer-only and has no M8 form. The remaining 17 physical cases
run on both backends. `matrix.csv` records every case's counts, and the two
seed-1 logs retain one complete round of fault observations plus the final
command result. `summary.json` records hashes and sizes of the full raw logs
retained locally, the backend source digest, and the 150 real SIGKILLs and 150
successful post-assertion fixture recycles. `source-sha256.json` identifies
the harness files used for this run. No missing boundary is treated as success.

## Failure found and corrected

The first bulk kind attempt hit a context deadline at seed5/SCH13b. Repeated
container kills on one pod had accumulated Kubernetes CrashLoopBackOff
(observed 2m40s with restartCount5). The first failure is in `backoff-red.log`.
Each fault now has to recover with the same pod UID and satisfy its original
index, row and fixture assertions. Only afterwards does normal fixture teardown
replace that pod, retain its PVC and require a new Ready UID with restartCount0.
A failed assertion remains a failure. The unchanged test-work limits apply;
cleanup has its own bounded rollback context. The runner stops at the first failure.

The corrected three-fault probe passed in87.606s including cleanup before
this full50-seed rerun. SCH01 uses the committed-log/before-apply barrier from
#94; the separate proposer-only probe is not counted as SCH01.

## Reproduce

On the disposable local kind-reliefgrid deployment:

```sh
SCHEDULE_RUN_ID=full-seams-isolated-20261104 make test-schedules SEEDS=50
```

The runner resets the deployment, uses a pinned isolated broker, pauses matcher
admission, installs the seams image for the fault cases, then restores the
captured image tag, isolation labels, replicas and admission on exit.
