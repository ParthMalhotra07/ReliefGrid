# PR123 review corrections

The real main orchestration is exercised with external I/O and measured helper
verdicts mocked. It must audit exactly once, after successful load completion,
scaling verdict and pod-termination checks. Failed measured scaling or termination
must never invoke audit. Moving audit before load fails three new cases; invoking
audit twice fails the exact-once case. Production verifier was restored after
each mutation and is unchanged by this correction. Eighteen verifier tests pass.

make check, gen-check and verify-A4.3-static passed on the rebased current branch,
including five client tests. There was no new physical kind run.

Historical image identity and manifest comparison are separated from current
source validation. Old records remain unchanged. The four historical manifest
differences were independently recomputed against7264d3c. Retained source was
dirty; no sufficient immutable map to the latest observed images/overlays or
latest deployed tools image ID was recovered. Acceptance is limited to the
recorded images; no runtime mismatch is inferred from missing provenance.

The existing restore log and command show fixed-profile restoration was requested
and Helm reported deployed revision22. They do not provide a final replica or
controller snapshot; fixed API2/matcher2 restoration remains unverified.
637 aggregate terminal/no-assigned-resource checks are supported; individual
final responses and throughput certification are not claimed.
