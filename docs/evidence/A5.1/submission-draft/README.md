# Submission fragments and accepted-evidence reconciliation

Parth's A-owned submission fragments target the agreed 3.5-page share within
the nine-page plan (S5.5/A3.5, including visuals). About 1,250 whitespace-delimited
words are retained, with API mapping, workflow/handoff/closure prose, app
deployment/validation portions, M6 integration and references. This is not a
rendered 3.5-page claim; S's text/figures and final export/count remain required.

The old report called all #126 results pending and #139/#145/#128 unresolved.
Publication state is now reconciled against main 7fa4a7d. Accepted Phase 1
measurements and the merged scoped #165 gate are distinguished from later
comparison/final-gate work. Historical source/images, omitted raw artifacts,
busy-system backlog and missing build bindings remain explicit. No old run's
missing final-replica evidence is filled retroactively by a newer run.

Verification ran red on the missing A draft before writing it. It also rejects
an intentionally broken evidence link in the new draft (file restored afterward).
The generic checker now includes the draft. Ten report/ADR artifacts and 264
local links pass, as do the three existing verifier regressions, the nine-page
allocation check, retained M6 replay, full make check and make gen-check.

replay-measurements.py reads the seven retained S5.2 reports, re-runs the current
checker (with the saturation requirement for three step runs), recomputes every
aggregate median/min/max/n using Decimal inputs and checks the published units.
Error fractions 0.001/0.00095/0.001275 are 0.100%/0.095%/0.1275%, never zero.
Run it read-only from the repository root with:
python3 docs/evidence/A5.1/submission-draft/replay-measurements.py .

No live load/cluster/auth/aging/autoscaling run, final report export, new kit,
rehearsal, recording or final package acceptance occurred. The pending comparison
and exploratory 100-110 req/s claim are not promoted from chat into accepted
measurements. Owner review remains required before integration/merge.
