# Merged dependency and CI entry point — 31 October 2026

Source combines merged main abfaede (#62/#68), #61 bf5b8ab and #65 e548086.
This is verified application integration; #61/#65 still require ownership
reviews and merges before goal completion. No S-owned source was hand-edited.

Fresh full/default manifest tests, GITHUB_ACTIONS=true make check (ordinary and
chaos race tests, 0 lint issues, real A1.3/A1.4 broker Verifies), integration
vet/lint, generation and module checks passed. See static.log and make-check.log.

The previous workflow handoff is resolved through the shared Makefile: existing
CI make ci-smoke first tests the exact prebuilt storage archives, then calls
scripts/app-ci-smoke.sh. The additional app stage uses a separate namespace,
builds/loads the actual app images, checks placement/leaders and runs public
create/register/asynchronous-match/replay through Traefik. Evidence stays under
the existing always-upload root. No workflow file or job timeout was edited.

The new tests first failed because the CI app entry point was absent (ci-red.log).
They then passed, including injected deployment/smoke failures: a failed deploy
prevents smoke; either failure propagates while diagnostics/logs remain.

The exact new app CI script was exercised locally against the existing local
app namespace (APP_CI_NAMESPACE=reliefgrid), preserving its broker PVC. It passed
full image build/deployment, three RF3 groups with distinct data-worker replicas,
one agreed leader per shard, reciprocal assignment and original-create replay:

```
A1.5 APP DEPLOY: PASS
A1.5 E2E SMOKE: PASS
A1.5 CI APP SMOKE: PASS
```

See ci-app-smoke.log and app-local-closeout/. This local run does not claim the
separate fresh CI namespace has run. The ready main-targeted #66 CI run must
prove that path. Dependencies/ownership review, main Verify and Phase 2's gate
remain required; no later-phase work is declared complete.
