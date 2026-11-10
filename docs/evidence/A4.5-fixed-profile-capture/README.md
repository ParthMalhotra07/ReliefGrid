# A4.5 fixed-profile collector verification

Baseline: main 764ced55c742e2c8d8907a738376397897f58dad. This is offline tooling
verification on 10 November 2026, not a new physical run or current gate verdict.
Only the root session worked; owner review remains required.

`red.txt` records the missing-module failure before implementation. `focused.txt`
records the Python fixture suite, including failure paths. `go-focused.txt`
records its race-enabled Go wrapper, which also runs in ordinary `make check`.
`check.txt` and `gen-check.txt` record repository checks. No Docker/kind cluster
was created, reset, scaled, paused, drained or otherwise changed for this work.

Fixtures cover controller generation/replicas, pod readiness/ownership/deletion,
requested images and immutable IDs (including mixed-image replicas), foreign-
labelled autoscalers, current ReplicaSet revision, storage ownership, sanitized
output, source changes, two-pass consistency, command timeout/failure, completed
tools Job ownership/image/exit status and refusing artifact overwrite.

Two disposable mutations fail the regression suite: accepting a remaining
autoscaler and ignoring changes between read passes. The actual assertion
failures are retained in mutation logs. These demonstrate relevant negative
coverage, not universal correctness of Kubernetes or the enhanced gate.

Pre-PR self-review: READY for **read-only collector scope**. No S-owned files,
contracts, acceptance thresholds or historical artifacts change. A PASS records
the selected fixed API2/matcher2 state and collector checkout only. Runtime image
build/source binding remains explicitly NOT_ESTABLISHED; historical #123 final
restoration is still unverified. The future integrated gate must execute the
collector against retained tools Job artifacts and provide separate workload,
strict audit, source/build and gate evidence.
