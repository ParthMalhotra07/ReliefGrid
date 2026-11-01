# Application CI entry point

The earlier proposed S-owned workflow addition is superseded. The existing
workflow calls make ci-smoke with all required tools and an always-uploaded
evidence root. The shared Makefile now runs scripts/app-ci-smoke.sh after the
unchanged prebuilt storage smoke. No .github file or timeout was changed.

The additional app step uses reliefgrid-app-smoke by default so it neither
adopts nor overwrites the storage release. Evidence goes to
EVIDENCE_DIR/app-SMOKE_RUN_ID. A failed deploy or e2e fails the existing smoke
job; diagnostics and logs survive for its existing always() upload.

Unit failure-path tests and the real local app entry-point run passed. The
ready main-targeted PR still needs its actual fresh-runner smoke to pass.
S review remains required for the PR, but no separate workflow patch is needed.
