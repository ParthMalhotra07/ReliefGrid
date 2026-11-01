# A1.5 owner review — standalone storage ownership

Demo reset/gate keep the standalone storage release as the sole owner, wait for
nine UP replicas, then call app-deploy for app images, chart dependencies and the
app install. app-deploy defaults to storage.enabled=false and the existing
storage-coordinator Service. It does not build or load storage images in this
mode. A missing Service fails before deployment. An app release that previously
owned storage is refused rather than silently deleting its StatefulSets.

CI explicitly selects APP_WITH_STORAGE=true in its isolated namespace, retaining
the tested full-subchart path. Existing 30-second integration budgets, smoke job
timeouts, storage images and bootstrap procedures remain unchanged.

test-first.log reproduces an unpackaged checkout failing its direct umbrella
install. regressions.log/static.log cover the replacement reset path, both
ownership modes, refusal cases, full/broker-only/app-only Helm renders, and empty
Helm-home dependency preparation. check.log is a fresh GITHUB_ACTIONS=true make
check with both live broker verifiers; gen-check.log proves generated code drift
is absent. Tagged script vet/lint and go mod tidy -diff also passed.

The current local namespace uses the former app-owned storage release. Its PVCs
were preserved; it was not migrated or reset for this review. Real fresh-runner
deployment/Traefik smoke must pass on this corrected PR head before merge.
The merge includes A1.4's separately verified owner-review fix e20e6dd.
