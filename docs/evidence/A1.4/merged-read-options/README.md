# Final merged read-options refresh — 31 October 2026

Integrates merged main abfaede (#62 and #68), plus #61's bf5b8ab review fixes.
The relay retains explicit strong, one-second bounded pending-outbox queries
and same-shard batches of at most 100. No S-owned source or contract was
hand-edited. The current #61 consumer test uses the merged query signature.

Fresh GITHUB_ACTIONS=true make check passed repository/chaos race tests, both
lint runs (0 issues), pinned-broker A1.3 Verify and all A1.4 live mandatory-return,
mid-batch broker restart and duplicate-handoff checks. Integration vet/lint,
generation and module checks passed. See make-check.log.

#61's S re-review/merge and this goal's S review are still required. Retarget
the same PR to main after #61 lands; main-targeted smoke and post-merge goal
verification remain required. No Phase 2 gate is waived by this stacked pass.
