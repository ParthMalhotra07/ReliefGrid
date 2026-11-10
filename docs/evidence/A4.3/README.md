# A4.3 historical evidence — recorded images, with verification limits

Retained round:20261107T131600Z. 637 client workflows passed
CANCELLED/no-assigned-resource checks; aggregate k6 results are retained, but
per-request final responses are not. The measured observations show matcher Ready
1→8→1, API Ready2→6→2, no bulk matcher downscale, zero observed application
restarts during the round, and12 clean pod exits. Exits use the first observed
positive-grace deadline; the final grace0 rewrite cannot extend it.

The retained strict audit reports ok=true, verdict=clean, quiescent=true and no
pending outbox. Its KEDA pause/drain runs after measured load and termination
validation. Regression coverage now runs the actual main orchestration with
external I/O mocked: audit exactly once after load/scaling/termination checks,
and never after a failed measured verdict. Early and duplicate-audit mutations
are rejected. These unit fixtures are not another physical acceptance run.

## Provenance scope

Acceptance is limited to image identities observed in the retained pod watch:
[extracted identities](review-20261109/recorded-image-identity.json). Both API and
matcher were tagged a43-review-nov7; older top-level runtime manifests describe
other images and are not provenance for this latest round. source.json records
a dirty revision. Existing build/restore logs were inspected, but no sufficient
immutable mapping from the latest observed image identities to the exact source
tree and broker/chart overlays was recovered. The latest deployed tools image ID
is not established. These limitations do not establish a runtime mismatch.

The original manifest is retained unchanged. Its four differences against reviewed
head7264d3c are [recorded separately](review-20261109/historical-manifest-differences.json):

- Makefile: independent ci-schedules target; tooling only.
- tools.mk: identical k6 version/checksum moved to their canonical location;
  equivalent tooling metadata.
- scripts/verify-A4.3-test.py: deadline/pin/audit regressions; test/evidence only.
- scripts/verify-A4.3.py: KEDA pause, drain, strict audit and restoration changes;
  acceptance orchestration with runtime effects. Do not treat this difference
  as proof that the measured run executed the current reviewed script.

Current branch check/gen/static results validate the reviewed source separately;
they do not retroactively supply a build-to-source map for the historical run.

## Restoration scope

The last measured state is API2/matcher1 under autoscaling. Audit cleanup restores
saved workers and removes its pause annotation. A retained
[Helm restore log](review-20261109/existing-restore-command.log) reports deployed
revision22 after a command selecting the fixed profile with --wait;
[command intent](review-20261109/existing-restore-command.txt) is retained too.
Neither contains a final Deployment/HPA/ScaledObject replica snapshot. Final fixed
API2/matcher2 restoration is therefore **unverified** by the retained artifacts.
No present-cluster observation is substituted for that missing historical proof.

This is historical scaling/drain evidence, with aggregate workflow checks. It
does not certify throughput, per-request terminal responses or the present
cluster. No new physical kind run was required for these review corrections.
