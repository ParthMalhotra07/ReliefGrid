# Preserve accepted evidence — A6.1 / #89

Ordinary application build/deploy/smoke commands choose unique cache run folders
and print their location. An explicit `A15_EVIDENCE_DIR` is still honored; a
deployment exports it to the image-build child. CI's named directories are
unchanged. See [usage](../../../operations/run-evidence.md).

- [Red](red.log): three real entry-point tests overwrite accepted image/node
  evidence or create smoke files under the historical A1.5 directory.
- [Green](green.log): four sandboxed entry-point regressions pass, including two
  default builds, a failed deployment, a failed HTTP smoke call, and explicit
  selection. External Docker/Kubernetes commands are fakes; no cluster changed.
- [Race regressions](regression.log): new tests and existing storage-ownership/CI
  propagation tests passed with `-race -count=3`.
- [Full check](check.log), [generation](gen.log): PASS.

Pre-PR review: READY. No protocol, contract, storage or CI workflow changes.
The default avoids modifying historical repository evidence on both success and
failure. No directory is deleted; evidence remains available after failure.
No end-to-end cluster redeployment is claimed for this file-routing change.
