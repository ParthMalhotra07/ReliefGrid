# Coordinator NetworkPolicy admission label — A1.5

API and matcher pod templates now carry the string label
`reliefgrid.io/coordinator-client: "true"` for coordinator NetworkPolicy #40.
The relay is hosted inside the matcher process, so its calls originate from the
labelled matcher pods. There is currently no separate relay or reconciler
Deployment; a future separate worker must carry the same pod label.

The UI remains outside the allowed client set, and immutable Deployment
selectors are unchanged. Both standalone-storage and full-subchart app profiles
are checked by decoding actual rendered manifests. The regression failed on
all four API/matcher combinations before the fix (red.log), then passed
(green.log). Full make check (including the live A1.3/A1.4 broker verifiers),
A1.5 static Verify, tagged script vet/lint and generation checks passed.

No NetworkPolicy, storage chart, frozen contract or S-owned implementation was
changed. This tests label compatibility; it does not claim that the pending
coordinator NetworkPolicy itself was deployed or its enforcement verified.

Pre-PR review: READY. Scope is limited to pod admission labels and regression
coverage; no workflow, ownership, ack boundary, retry rule or deadline changes.
