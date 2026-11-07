# A4.3 latest acceptance — PASS

Latest retained round: 20261107T131600Z. 637 real request lifecycles, matcher Ready 1→8→1, API Ready 2→6→2, no bulk matcher downscale, zero application restarts during the round, 12 clean pod exits. Exit timestamps are compared with the first observed positive-grace deadline, retained per UID; the final kubelet grace-0 rewrite cannot extend it. A missing first positive-grace observation fails closed.

The final audit pauses KEDA, uses the demos' pause-and-drain path and runs strict invcheck. Actual result: ok=true, verdict=clean, quiescent=true, no pending outbox. Workers, KEDA annotation and process environment are restored in finally, including on failure. The normal fixed API2/matcher2 profile was restored after the round.

Red regressions caught late exit accepted under rewritten deadline, wrong deadline retained, missing grace accepted and duplicate k6 definitions. All 15 verifier regressions and five client tests pass. make check, generation and static chart verification passed before the live round. Current-main verification is repeated before push. Runtime uses branch-built api/matcher/tools plus the native broker fix from approved #133; storage PVCs and images were not replaced. Previous rounds and full verbose logs are archived locally; one full round is retained here.

PRE-PR REVIEW: READY for re-review. No contract/S-owned changes, no relaxed thresholds or skipped tests. The invariant audit occurs after measured scaling and cannot contribute to its scale/downscale evidence. This remains scaling/drain acceptance rather than a throughput benchmark or A4.2 verification.
