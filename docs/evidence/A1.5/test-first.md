# A1.5 — application on kind

Parth explicitly authorized starting A1.5 stacked on verified PR #65 before the
dependency merges, while keeping verification, owner review and human merge gates.
Branch: a/A1.5-app-on-kind; dependency head b27f7f1. Main remains d28a1da.

The native Linux checkout preserves the storage chart's committed ring symlink.
Original dirty Windows checkouts and existing broker data remain preserved.

## Plan

1. Reverify A1.1, A1.3 and A1.4 and the full baseline check.
2. Write/run deployment acceptance tests first, then the playbook checkpoint.
3. Build static non-root API, matcher and placeholder UI images using the existing
   pinned Go/distroless bases. No fake storage runtime.
4. Extend the app umbrella with S's local storage chart dependency, app templates,
   an explicit kind app profile and Traefik NodePorts/routes. Preserve the current
   broker-only profile and broker PVC. No storage or CI source edits.
5. Install with --wait --wait-for-jobs. Verify three real RF3 groups and placement.
6. Via Traefik: create request, register resource, observe reciprocal match; replay
   keyed create and require the original body/Location. Save sanitized evidence.
7. Run full checks, goal verification, review and CI. Open a separate stacked PR;
   retarget only after dependencies merge. No main push or automatic merge.

## Acceptance

Rendered manifests must contain API x2, matcher x2, UI x1, storage 3x3 and the
coordinator. Verify initialized-process health probes, non-root security/resource
settings, coordinator and secret-backed broker configuration, exact RabbitMQ pin,
broker infra placement and localhost NodePort mapping. Expose only the API and UI
through Traefik; do not strip /api/v1. The broker-only render must remain independent.

The live smoke test must check committed business state through Traefik, not just
pod readiness. Image/runtime checks and the real cluster run follow implementation.
No goal-completion or real-cluster pass is claimed by the static tests alone.
