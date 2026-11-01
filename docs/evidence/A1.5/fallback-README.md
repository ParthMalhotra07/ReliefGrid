# A1.5 fallback — images and application chart

31 October 2026. Branch a/A1.5-app-on-kind, stacked on #65 at b27f7f1 with
Parth's explicit authorization. Test-first checkpoint approved with "ok, continue".

## Completed and checked

- Static CGO_ENABLED=0 API, matcher and placeholder UI images using the existing
  pinned Go 1.27.2 and distroless digests. Actual image IDs and users/entrypoints
  are recorded in image-api.json, image-matcher.json and image-ui.json.
- All three copied runtime binaries are actual 64-bit ELF executables without
  PT_INTERP or PT_DYNAMIC. The UI image served readiness and its placeholder
  with a read-only root filesystem, dropped capabilities and no-new-privileges;
  /internal/ring returned 404. See image-runtime.log and images-build.log.
- Application templates: API x2, matcher x2, UI x1, non-root/read-only security,
  resource settings, Services, initialized-process health probes, coordinator
  configuration and secret-backed AMQP URL. Explicit kind profile enables the
  local S-owned storage chart dependency and Traefik NodePorts/routes.
- Default broker-only profile remains independent. The broker's existing pinned
  image, PVC and credentials are preserved. No cluster install or PVC operation
  was performed by this fallback work.
- Matcher health exposes initialization, relay-loop and consumer connection
  state. Liveness is independent of a temporary broker outage; readiness means
  initialized process/active relay loop, not all-shard or matching availability.
- GITHUB_ACTIONS=true make check passed, including race/chaos tests and live
  A1.3/A1.4 acceptance. Additional integration-tag vet/lint passed (0 issues),
  make gen-check and go mod tidy -diff were clean. See make-check.log.

## Blocked goal verification — S handoff

The original full-application manifest test remains unchanged and failing:

```
TestAppHelmFullDeployment: yaml: unmarshal errors:
mapping key "app.kubernetes.io/name" already defined
TestAppBrokerOnlyProfileRemainsIndependent: PASS
```

S's deploy/helm/storage/templates/statefulset.yaml emits both storage.labels and
storage.shardSelector into the pod labels (lines 28-29). Both helpers include
app.kubernetes.io/name: storage. The duplicate exists in the dependency source;
it is not an application-label collision introduced by this branch.

Request for S: keep the existing storage.labels include and emit only component
and shard labels alongside it in this pod-label block. Keep the full selector
helper unchanged elsewhere. This removes the duplicate without changing labels
or the Raft/claim protocol. No S-owned file was edited here.

An additional diagnostic rendered only owned app resources with storage disabled
and parsed them strictly: API=2, matcher=2, UI=1. See owned-chart-diagnostic.log.
This does not replace the failing full test or establish RF3/app e2e success.

Per the playbook's A1.5 fallback and STOP rule, work stops after images/charts.
Real storage installation, app deploy/e2e commands, create/register/match smoke,
CI integration for these deployment checks and full goal completion remain
pending the S handoff correction. This PR must remain draft and must not merge
until the full manifest check, real cluster Verify and required reviews pass.
A1.5 remains unticked pending human merge and fresh main verification.

## Self-correction history

The initial health cancellation test reported "health listener still accepting
after shutdown". Cancellation could win before Serve registered its listener.
The code now explicitly closes its owned listener and joins Serve; the same test
passes. Applied the same cleanup discipline to the new UI server.

Lint then caught missing request contexts and an unchecked shutdown result in
new tests; supplied contexts and asserted the returned cancellation error.
The live A1.3 verifier also stopped because its existing first-image lookup saw
the new app image values before the RabbitMQ pin. Kept the existing broker block
first and reran full checks successfully. The Docker context includes loadtest
because the existing S tools image copies it. No tests were weakened or skipped,
no timeout raised, and no contract/invariant or S-owned source changed.
