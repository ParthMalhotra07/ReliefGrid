# Joint M6 selection and provenance clarification

A proposed the bounded S4.1 study in PR152 at1ea60a9; S's9November review
confirmed the raw medians/ratios and agreed to select it as the single M6 study.
ADR006 now records acceptance. This is a scope decision for that study, not
new benchmark/gate acceptance or a change to aging/authentication scope.

The benchmark-source-go.mod.txt is actual `git show 5ad15ac:go.mod` output:
Go1.27.2 is pinned in source. The separate safety run explicitly records that
runtime version. The raw benchmark output itself omits its runtime version;
neither source pin nor safety runtime establishes the benchmark runtime.
Original raw records are unchanged. Current check/generation/summary replay
outputs accompany this clarification. Earlier red/proposed evidence remains
historical. Independent exact-head review is required before push.
