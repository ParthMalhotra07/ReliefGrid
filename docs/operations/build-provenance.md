# Source-bound delivery builds

Normal development builds remain available. Their image records explicitly say
sourceBinding=NOT_ESTABLISHED. To prepare reviewed delivery evidence, use strict
mode from a clean checkout (including no untracked source files):

```sh
APP_BUILD_PROVENANCE=1 APP_IMAGE_TAG=delivery-run \
  A15_EVIDENCE_DIR=/tmp/reliefgrid-build-UNIQUE make app-images
```

Strict mode exports HEAD with Git archive and builds from that isolated context,
not the changing working checkout. Ignored/untracked files are not exported.
The success manifest is written only after all builds and source rechecks pass.
An evidence directory inside the source tree, dirty source, changed HEAD/tree,
partial build, incorrect labels, malformed image ID or wrong nonroot entrypoint
fails. Existing evidence is never overwritten. Submodules need a separately
reviewed materialization recipe and are rejected here.

`build-provenance.json` records source commit/tree/clean status, archive SHA-256,
Docker client/server version, hashes of Dockerfiles/ignore files/go.mod/go.sum/
tools.mk, image configuration IDs, available repository digests and platform.
The exact captured Dockerfile fixes its base references and default build args;
this command accepts no unrecorded build args/overlays. It labels images with the
source commit/tree/archive digest, captures --iidfile, and inspects that immutable
ID rather than a potentially retagged name. No build stdout/stderr or raw image
environment is persisted. Input/output parents must already be usable.

The generic recorder can also build storage/coordinator/tools without editing
their owner scripts. Coordinate those builds with S:

```sh
python3 scripts/build_provenance.py build --source . \
  --output /tmp/reliefgrid-cohort-UNIQUE --tag delivery-run \
  --component api --component matcher --component ui \
  --component coordinator --component storagenode --component tools
```

This produces default binaries, not seams-tagged or custom-argument builds.
It creates local images; it does not load them into kind or deploy anything.

After #157's fixed-profile collector records the actual runtime cohort, compare
the retained records without rewriting the original capture:

```sh
python3 scripts/build_provenance.py bind \
  --build-record /tmp/reliefgrid-cohort-UNIQUE/build-provenance.json \
  --capture /tmp/final-profile/fixed-profile.json \
  --output /tmp/runtime-binding-UNIQUE.json
```

Multiple --build-record arguments are supported for disjoint component builds
from the same clean commit/tree. Binding requires four distinct API/matcher pods
and one tools observation, exact requested references, and runtime immutable IDs
present in the build records. Tags alone never establish a binding. Docker config
IDs and registry manifest digests are different identities: both are retained
when supplied by Docker. If a runtime presents a manifest ID absent from the
records, binding fails; retain the actual registry/export proof instead of
substituting a tag or inventing a digest mapping. Input raw-file SHA-256 values
are included in the binding so the final package can link the exact artifacts.

These are **unsigned records from a trusted local builder**, not a signed
supply-chain attestation, proof of Docker daemon honesty, functional correctness
or a gate verdict. Owner review of the source/recipe remains necessary. Historical
images without original build records remain unbound; do not backfill claims.

Offline regression command: `python3 scripts/build_provenance_test.py -v`.
The ordinary Go test wrapper embeds both Python inputs so cache invalidation
also covers Python-only edits. Docker operations in the regression fixture are
stubbed; a passing fixture does not claim an actual delivery image was built.
