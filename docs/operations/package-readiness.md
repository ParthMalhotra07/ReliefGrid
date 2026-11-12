# Final-package consistency check

This read-only tool checks the final inventory and links retained artifacts to a
clean tagged source baseline. It does not run a deployment, benchmark, gate,
browser, publication check or media player. Owners must supply truthful reviewed
summary receipts; a PASS is **mechanical consistency**, not independent semantic
certification or proof of submission. Report length/format confirmation, pending
measurements, auth work and physical acceptance must not be silently waived.

Assemble a separate package directory outside the clean source checkout. Use
relative canonical POSIX paths, no symlinks and no unlisted files. Every file
except the inventory manifest itself must have one entry with path, role and
SHA-256. Large archives/recordings are hashed by streaming. The output must be a
new file outside both the checkout and package. Nothing is overwritten.

The inventory shape is:

```json
{
  "schemaVersion": 1,
  "sourceCommit": "",
  "sourceTree": "",
  "releaseTag": "",
  "repositoryURL": "https://github.com/ParthMalhotra07/ReliefGrid",
  "publicVisibilityConfirmed": false,
  "reportFormatConfirmed": false,
  "ownerSignoff": {"A": "", "S": ""},
  "unresolved": ["Populate only after actual final-delivery verification"],
  "artifacts": []
}
```

The empty example is intentionally NOT READY. The tag must already exist and
resolve to the clean checkout's HEAD; the tool never creates/moves it. Git supplies
commit/tree and source-file hashes. Source inventory must include README.md,
contracts/public.yaml, contracts/internal.yaml, contracts/ring.yaml and every
tracked JSON event schema and Markdown ADR from that baseline, with unchanged
bytes. ADR004 (attempt closure) and ADR005 (outbox handoff) are required explicitly;
the merged M6 ADR006 is included by that complete ADR inventory.

Required roles are source, report, recording, offline-kit, evidence, checks, gate,
performance, offline, build-provenance, fixed-profile and runtime-binding. Source,
report, evidence and build-provenance may contain multiple files. Summary roles,
the final offline archive, runtime binding and fixed profile are unambiguous
single artifacts. The fixture schema is executable in package_readiness_test.py;
its invented bytes/results are examples only and must never become real evidence.

The owners' checks/gate/performance/offline envelopes each declare sourceCommit
(the accepted package baseline), status=PASS and nonempty rawArtifacts pointing
to hashed evidence entries. Retain original raw evidence and its actual source/
tree/image qualifications; never rewrite old metadata to make an envelope pass.

- checks.checks requires PASS for make-check, gen-check, contract-validation,
  doctor, clean-clone and recording-playback. Run those existing checks first;
  this tool does not fabricate receipts or rerun them automatically.
- gate.checks requires PASS for ui, dashboards, trace, chaos, autoscaling,
  replacement, ci-smoke, api-refinements and m6. It also carries a separately
  qualified historicalCourseGate: its original sourceCommit plus 18 distinct
  round/demo/status entries (round1/2, demo1..9). The historical commit may differ
  from the final baseline. This does not demand a new 18-demo course gate or
  promote historical execution to current-source acceptance.
- performance declares pendingMeasurements=false and acknowledgedWriteAudit=PASS,
  backed by actual #126 outputs. The checker does not validate statistical method
  or infer numerical results from these declarations. Until those measurements
  exist, this category is incomplete.
- offline declares nine distinct passing demo entries, archiveSHA256 matching the
  packaged offline-kit, and isolation.runnerBlocked=true plus five distinct named
  nodes with externalBlocked=true. Original logs must support these declarations.
  Archive/playback/network behavior is not re-executed by this inventory check.

The six-image build inventory covers api, matcher, ui, coordinator, storagenode
and tools from the same clean baseline. Build labels and immutable IDs must agree
with the source records. The current fixed-profile source must match that
baseline; its distinct API2/matcher2 pods and successful tools exit are retained.
The separate runtime binding must reference the exact build/capture raw-file
hashes and the same observed runtime images. Missing/unbound/dirty/stale evidence
is rejected. No old #123 restoration claim is repaired by packaging it.

```sh
python3 scripts/package_readiness.py --repo . \
  --package-root /tmp/reliefgrid-final-package \
  --manifest /tmp/reliefgrid-final-package/manifest.json \
  --output /tmp/reliefgrid-package-check-UNIQUE.json
```

No blanket text search rejects words such as pending/unverified in qualified
historical report prose. Pending state is rejected in the explicit readiness
fields instead. File content/identity and inventory are rechecked before success;
access-time updates caused by reading are permitted. The manifest hash is saved.
The public-repository, format and owner declarations are supplied attestations,
not cryptographic signatures or independently queried GitHub state.

Offline regression command: `python3 scripts/package_readiness_test.py -v`.
Its Go wrapper embeds both Python files for cache invalidation. Fixture PASS is
not readiness of today's repository/package; actual final artifacts remain due.
