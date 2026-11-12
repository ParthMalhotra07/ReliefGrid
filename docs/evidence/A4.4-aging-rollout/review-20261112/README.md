# Aging rollout review regressions

The permanent suite now separately rejects wrong committed voter IDs, Nonvoter
suffrage, zero leaders despite an agreed leader ID, a valid new leader or term
between passes, wrong tree/context labels with recomputed approval hashes, an
old Pod revision, absent admission/catch-up/state/commit fields and a failed
read command. Null server identity formerly escaped with AttributeError; the
retained red test reproduces it. The CLI now saves a sanitized FAIL result.

Sixteen focused tests, make check and generation checks passed. Twelve targeted
unsafe mutations were rejected in disposable source copies (mutations.txt).
No cluster was read, no image was built and aging was not enabled. Fixtures do
not establish functional aging or an accepted live rollout. Producers stay 0s.
