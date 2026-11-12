# Build provenance verification

Baseline main764ced55c742e2c8d8907a738376397897f58dad. Test-first missing-module
failure is retained in red.txt. Focused Python regressions use actual temporary
Git repositories/archives and stub Docker operations; no delivery images or
cluster were built here. Installed Docker CLI help confirmed --iidfile/--label.
Docker Desktop's WSL daemon was unavailable; no physical-build claim is made.

Coverage: clean root/immutable export, ignored/untracked input exclusion, sampled
source-change detection, correct labels/IID/nonroot entrypoint, partial failure
without a success manifest, existing/inside-source output refusal, distinct
runtime pods, exact baseline and immutable-image binding. Negative controls
removing dirty-source rejection and runtime-ID matching fail the intended tests
in disposable copies. Actual output is retained in mutation logs; trailing
whitespace alone is normalized. Go embeds Python inputs for test-cache tracking.

focused.txt, go-focused.txt, check.txt and gen-check.txt contain verification
output. Pre-PR author self-review: READY for recorder/comparison scope; co-owner
review remains required. No production contracts or S-owned paths changed.
The records are unsigned trusted-builder metadata, not attestations or runtime/
protocol/gate certification. Actual clean delivery builds and observed runtime
binding still must be performed on the final approved source cohort.
