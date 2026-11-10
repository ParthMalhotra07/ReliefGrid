# Aging rollout checker verification

Baseline main764ced55c742e2c8d8907a738376397897f58dad. red.txt records the missing
implementation before tests. Focused tests cover synthetic K8s/management state,
the CLI with stubbed Git/cluster reads, controller/pod ownership and revisions,
exact nine-node coverage, image/source record approval, committed voters,
admission/catch-up/leader terms, disabled producers and repeatable sanitized
artifacts. Applied-index movement is allowed without comparing it with Raft
indexes. Collect command tests permit only GETs and /storagectl status execs.

Removing immutable-image matching or the admission requirement makes the
permanent regressions fail in disposable source copies. Actual mutation output
is retained, with only trailing whitespace normalized. Go embeds Python inputs
so Python-only changes invalidate cached Go test results.

focused.txt, go-focused.txt, check.txt and gen-check.txt retain real check output.
Pre-PR author self-review: READY for read-only prerequisite-checker scope;
co-owner approval/capability review remains required. No S-owned paths/contracts
were edited. No cluster was read or mutated during these fixture checks, no
producer was enabled and no live rollout/functional aging acceptance is claimed.
The owner approval file is a supplied declaration, not a signature or proof of
feature capability; Git ancestry alone cannot rule out a later revert. Production
rollout, ordinary routing preflight and functional aging Verify remain separate.
