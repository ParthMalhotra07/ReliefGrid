# Fresh-runner Helm correction — 31 October 2026

CI run 41179957589 on 37b998d failed its actual make ci-smoke entry point:

```
app_deploy_test.go:104: helm dependency: exit status 1: Error: no repository definition for https://traefik.github.io/charts. Please add the missing repos via 'helm repo add'
```

The local Helm configuration already contained the upstream repository. The
production app chart preparation must establish it explicitly on a new runner.
scripts/app-chart-deps.sh now registers the project alias reliefgrid-traefik,
then builds the locked dependencies with bounded calls. Both app-deploy.sh and
the actual manifest tests use that entry point. Chart.yaml/Chart.lock pins
remain unchanged; no assertions or job budgets were relaxed.

A new integration regression first failed with the absent preparation script
(red.log). It now passes with all three Helm configuration/cache/data homes
empty. The unchanged full/default manifests also pass. Fresh make check passed
ordinary/chaos race tests, both lint runs (0 issues), and both pinned-broker
Verifies. Integration vet/lint, gen-check and module checks passed; see static.log
and make-check.log. The next exact-head CI run must repeat make ci-smoke on its
fresh runner; this local result is not a CI pass claim. Self-correction attempt 1.
