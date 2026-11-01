# Current closeout status

The latest integration and verification supersede the historical snapshot below;
see [merged dependency closeout](merged-dependency-closeout/README.md).
#62/#68 are merged. The app CI entry point is wired through the shared Makefile;
no separate workflow-owner edit is needed. #61/#65 review/merge, this PR's
S review, actual main-targeted CI and post-merge Verify remain required.

# A1.5 — application images, kind deployment and smoke

31 October 2026. PR #66 remains draft, stacked on #65 with Parth's explicit
authorization. Human dependency merges, S review, CI and fresh main verification
remain required before completion. Nothing below is a main completion claim.

## Behaviour

Static CGO_ENABLED=0, non-root API/matcher/UI images use the pinned Go 1.27.2 and
distroless digests. Actual IDs, users and entrypoints are recorded in image-*.json.
Previously copied binaries have no PT_INTERP/PT_DYNAMIC; image-runtime.log shows
the UI running with a read-only filesystem, dropped capabilities and readiness.

The explicit app kind profile installs API x2, matcher x2, UI x1, the S storage
subchart and Traefik routes. App calls use storage-coordinator:8080. App pods are
non-root/read-only on data workers; the broker and coordinator use infra.
The default broker-only profile stays independent. Matcher readiness describes
initialization and its relay loop, separately reporting broker connection state.

`make app-deploy` builds and loads uniquely tagged images, upgrades the same Helm
release without deleting data, preserves broker PVC identity, checks RF3 placement
and records one agreed leader per shard. `make e2e-smoke` uses only public Traefik
routes, frozen create payloads/keys, bounded retries and a 90-second overall budget.
It verifies MATCHED, a reciprocal ASSIGNED resource claim from a complete strong
resource list, and byte-identical original create replay after matching.

## Actual local integration result

See local-integration-20261031/. The isolated deployment tree combined published
#66 at 4b14a32, main #60 at 2f85542 and the proposed #67 label correction at
2515870, plus the own script/ingress changes in this update. This run is not
verification against #68's actual source.

```
placement-check: every shard has 3 replicas on distinct data workers
shard 0: three replicas, one agreed leader
shard 1: three replicas, one agreed leader
shard 2: three replicas, one agreed leader
A1.5 APP DEPLOY: PASS
A1.5 E2E SMOKE: PASS {"result": "PASS", "requestId": "ed64a9eb-0dee-5722-a27b-d97e57832a5f", "resourceId": "65a62d8b-20ea-5adf-a79b-2a374db31b20", "reciprocalClaim": true, "originalCreateReplay": true}
```

The unchanged broker PVC UID was 3d982505-ac00-4eb2-b6ec-8b8b0cafb9b2.
Recorded attempts retain the initial unsupported detail-route failure and the
successful rerun with the exact same immutable create inputs.

Fresh GITHUB_ACTIONS=true make check passed repository/chaos race tests, lint
(0 issues) and live A1.3/A1.4 acceptance. Integration-tag vet/lint, gen-check and
tidy were clean. The combined proposed-label tree passed full/broker-only chart
tests. Source tests are retained, with an additional IngressClass assertion.

## Corrections and remaining gates

The first live UI call returned 404 because the app selected ingress class
traefik while the chart created reliefgrid-traefik. A new assertion reproduced
`missing IngressClass traefik`; the app profile now sets that class explicitly.
Original route/security/replica assertions were preserved.

The next smoke reached MATCHED but the new script called GET /resources/{id},
which is absent from the frozen contract. After Parth authorized the correction,
it uses GET /resources, asserts strong/complete/all-shard results, selects the
exact resource ID and preserves the reciprocal-claim and replay assertions.
No contract changed, assertion weakened, timeout raised or test skipped.

#67 was closed in favour of the storage owner's #68 at 347ed665. A fresh isolated
combination of #66 at 0371cbb and the exact #68 head passed full/default manifest
checks, strict storage labels, GITHUB_ACTIONS=true make check, generation/tidy,
app deployment and reciprocal/replay smoke; see owner68-integration/. The same
broker PVC was preserved. #68 is approved but still requires human merge and
final merged-head verification. #61 has requested MQ review fixes; #62 needs its owner
rebase; #65 and this draft must integrate those final heads and be rechecked.

The shared smoke prerequisite now runs the app manifest tests where Helm is
installed. The current S-owned CI image job builds storage images only; request
S's app-image/end-to-end CI wiring before claiming the full goal Verify in CI.
The local pass does not replace that gate. Resource wakeup execution, complete
business retry recovery, full UI and reconcilers remain their later goals.

The earlier fallback and its initial corrections remain in fallback-README.md
and the existing red/runtime/build logs.
