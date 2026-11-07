# Additional engineering validation

These results extend the report core. They do not relabel the historical 18/18
course gate or certify an unexecuted enhanced gate. The implementation PRs still
require their owner reviews and merges.

| Check | Observed result | Immutable evidence |
|---|---|---|
| A4.2 leader-failure observability | Actual DOWN member and replacement leader, 6,484 committed synthetic writes, 41 samples, unfinished workflow age +47.227s, all nine members authoritatively caught up | [Fault proof and three screenshots](https://github.com/ParthMalhotra07/ReliefGrid/tree/0c37a654db580dea73b7abc7a22ae3f8fd3d5bb8/docs/evidence/A4.2/live-20261107T133542Z) |
| A4.3 scaling and graceful exits | 637 complete real lifecycles; matcher Ready 1→8→1, API Ready 2→6→2; 12 clean exits before their first deletion deadlines; strict invcheck clean and quiescent | [Scaling round and audit](https://github.com/ParthMalhotra07/ReliefGrid/tree/7264d3cbfdddf43b184cdbec3eac0ff4485348dd/docs/evidence/A4.3/20261107T131600Z) |
| Public source availability | Repository changed to public on 7 November; unauthenticated API access confirmed visibility=public | [Repository](https://github.com/ParthMalhotra07/ReliefGrid) |

## Interpretation and limits

Storage's raw Raft/FSM index gap includes barriers and no-op entries. A positive
gap alone does not establish unapplied business data. The recovery verifier uses
the authoritative caughtUp status on all nine members in addition to dashboard
movement. Missing and stale outbox observations are absent, rather than zero.

The scaling workload exercises durable handoffs, reciprocal matches, cancellation
and terminal cleanup. Kubernetes rewrites deletion timestamps during final
grace-0 deletion, so the verifier records the first positive-grace deadline and
checks finishedAt against that original bound. Its final invariant audit occurs
after measured scaling: admission is paused, committed handoffs drained with
routed confirms, then invcheck must report complete, clean and quiescent.

These are functional and recovery observations, not throughput or saturation
benchmarks. The storage/load chapter still needs Hrishabh's final repeated k6
measurements. The offline kit is not certified until all nine demos pass with
external access blocked. Three joint presenter rehearsals and the final recording
and submission receipt remain external deliverables.
