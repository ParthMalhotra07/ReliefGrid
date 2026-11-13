# Additional engineering validation

These results extend the report core and preserve each run's scope. The
[12 November scoped gate](../evidence/gate-20261112T100532Z/report.md) is now merged
through #165: course 18/18, app-quiesced chaos 12/12 runs, Merkle 7/7 and strict
invcheck/fixed-profile collection. Aging was 0s, autoscaling off and auth excluded.
It does not certify those open enhancements, later forms/read/auth changes or a
final integrated gate. #134/#136 are merged; implementation and execution are
separate evidence claims.

| Check | Observed result | Immutable evidence |
|---|---|---|
| A4.2 leader-failure observability | Actual DOWN member and replacement leader, 6,484 committed synthetic writes, 41 samples, unfinished workflow age +47.227s, all nine members authoritatively caught up | [Fault proof and three screenshots](https://github.com/ParthMalhotra07/ReliefGrid/tree/0c37a654db580dea73b7abc7a22ae3f8fd3d5bb8/docs/evidence/A4.2/live-20261107T133542Z) |
| A4.3 scaling and graceful exits | 637 client workflows passed CANCELLED/no-assigned-resource checks (aggregate results only); matcher Ready 1→8→1, API Ready 2→6→2; 12 clean exits before their first deletion deadlines; strict invcheck clean and quiescent | [Scaling round and audit](https://github.com/ParthMalhotra07/ReliefGrid/tree/7264d3cbfdddf43b184cdbec3eac0ff4485348dd/docs/evidence/A4.3/20261107T131600Z) |
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

The scaling evidence is historical and limited to the recorded image identities.
Its source record is dirty; an immutable image-to-source/overlay mapping and the
latest tools image identity were not established. This does not establish a
runtime mismatch. Aggregate k6 results are retained, but per-request final
responses are not. The last measured autoscaling state was API2/matcher1.
Existing Helm restoration output records a deployed revision, but no final
replica/controller snapshot was retained: final fixed API2/matcher2 restoration
is unverified. The [review corrections and qualified evidence](https://github.com/ParthMalhotra07/ReliefGrid/tree/468de44ba8b217298d9d003e24c81b10d953348c/docs/evidence/A4.3)
retain the four manifest differences and these limits. They do not certify
deployment of the reviewed source or the present cluster.

These are functional and recovery observations, not throughput or saturation
benchmarks. The storage/load chapter now records accepted #126 Phase 1 measurements;
later comparisons need their own accepted raw run references. The [retained offline reports](https://github.com/ParthMalhotra07/ReliefGrid/tree/b4ae2487df19c18d0cc44d2f6e1dd5225fba4701/docs/evidence/A5.4/accepted-20261107)
support nine passes for the recorded artifact. #139's reviewed host-preparation and timeout-cleanup follow-up is merged;
those historical reports still do not certify a new integrated kit or arbitrary
offline host. Three joint
presenter rehearsals and the final recording and submission receipt remain external deliverables.

## Physical evidence is bounded by its recorded source

The storage/reliability evidence and failed chaos-coverage observations are
catalogued in [requirements and results](requirements-and-results.md). Physical
crictl schedule kills support named boundaries on the recorded seams image;
same-index digests support sampled convergence; the aging-seam evidence explicitly
does not include a physical cluster run. None is a fresh integrated gate on
ce643ee or a blanket proof of every failure timing. Reported empty violation
lists require complete, clean and quiescent inputs to be meaningful.

Accepted #126 Phase 1 and the scoped #165 gate are recorded above. Later load
comparisons and the final integrated gate remain pending. The scoped collector
retains its own fixed API2/matcher2 snapshots with runtime build binding
NOT_ESTABLISHED; it does not retroactively verify #123's missing final snapshot.
Application aging stays disabled until rollout verification and policy agreement;
auth/gate scope is not silently changed by this report integration. The final
enhanced gate must retain explicit fixed API2/matcher2 restoration evidence,
rather than inferring it from Helm completion or the earlier autoscaling samples.
