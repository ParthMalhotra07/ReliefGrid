# A2.3 schedule evidence

Full local acceptance passed: **850/850 M8 and 900/900 kind executions**,
all 50 seeds under the race detector. The kind run observed 150 real container
SIGKILLs followed by same-pod recovery and 150 post-assertion fixture recycles.
See [completed-20261104](completed-20261104/README.md) for the matrix, real red
output, command results and source hashes. SCH13a is cluster-only; SCH13b runs
on both backends. The verified harness was rebased onto main dba92c5 without
changing any file in its source manifest.

Owner review, merge and post-merge verification remain required. This evidence
is independent of the official joint demo gate and does not claim a gate result.

## Historical checkpoint (superseded by the completed run above)

Full acceptance is **RED / NOT READY**. `make test-schedules SEEDS=50` still
fails SCH01 (both commands in the FSM log before apply) and SCH12/SCH13
(interrupted snapshot creation/restore). These need S-owned test seams in #88.
No missing case is skipped or counted as a pass.

`checkpoint/batches.json` records the measured subset counts and run IDs;
the companion logs retain command summaries and real Go results. Fourteen
drivers were exercised for seeds 1..50 on M8 and kind. The separate proposer
probe is not counted as SCH01. M8's later command-fault batch strengthens
SCH02/SCH03 with explicit unknown-outcome assertions and a pre-proposal drop;
the other driver semantics are unchanged. SCH14's 50 physical coordinator
replacements are recorded separately from its M8 partial-read test.

Reproduce the final subsets with the commands in tests/workflow/README.md.
The main cluster batch uses IDs 2..11,15,16,17 after a clean deployment;
SCH14 uses IDs=14, SCHEDULE_HOLD_SHARD=1 on the isolated dev fixture. Each
replacement requires a new Ready coordinator UID, the same ring version and
checksum, explicit unavailable shard1 and a reciprocal match on shard0.

Original missing-target red output and the current three missing-boundary
failures are retained. A full run cannot be inferred from subset success.
Source hashes identify the packaged harness. Normal make check, generation,
tagged lint and affected race count3 results are saved with the checkpoint.

During harness development the Kubernetes Service proxy timed out under the
coordinator NetworkPolicy. The final runner uses a reconnecting loopback
port-forward supported by that policy, with child join/cleanup regressions;
no policy was removed or relaxed. One earlier successful case batch ended
with a shell error after the script was edited while executing; its raw log
was retained locally and the final file was rerun from a fresh deployment.
Neither failed target invocation is counted as a successful harness run.

Large raw deployment/HTTP logs remain local and unstaged. Compact summaries
are generated from actual Go output and assert every listed tag has all 50
seeds; they are not invented green reports. This is not A3.3 gate evidence.
