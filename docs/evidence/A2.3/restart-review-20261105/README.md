# A2.3 restart-count review fix

The real kill helper now requires restartCount == before+1 after the killed
container is Ready, before checking the original pod UID and returning to the
snapshot assertions. An extra startup crash is a failure before fixture recycling.
The obsolete pod-delete fallback comment was removed; SIGKILL remains mandatory.

A subprocess regression drives the actual helper through fake kubectl/crictl
commands. One injected restart passes; an extra crash after the initial restart
must fail specifically on the count. red.log proves the original helper accepted
that extra crash. race3.log records the corrected positive/negative controls.

As requested in Hrishabh's review, the corrected kind subset ran all five seeds
of SCH12/SCH13a/SCH13b: 15/15 PASS under -race, 15 real SIGKILLs and same-pod
recoveries, then 15 successful post-assertion fixture recycles. cluster.log retains
the complete small subset. verify.log records the runner result; its outer exit
was also checked for successful restoration. Missing boundaries and failed cleanup
remain failures. The original assertions and time budgets are unchanged.

This is the requested delta verification, not a new full 50-seed acceptance run.
The earlier 850 M8/900 kind run retains its original source manifest under
completed-20261104. This subset was run after rebasing onto main 415fa2c; exact
review-fix source hashes are in source-sha256.json. Full make check, generation,
schedules-tag vet/lint and focused race-count3 regression checks passed.

Non-blocking SCH04 closure and broader outbox observations are tracked in #109.

After the completed run and rollback, the branch was rebased onto main
8145ae0 (the gate-evidence merge). All tested source hashes remained unchanged;
post-rebase-check.log, post-rebase-gen.log and post-rebase-tagged.log record
successful final publication checks on that base.
