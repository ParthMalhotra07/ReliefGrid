# PS Table2 compatibility verification

Final real-kind run20261106T124636Z PASS. All seven PS operations were reached
through Traefik using bare paths and canonical /api/v1 paths, with matching
success statuses and stable keyed-create replay across prefixes. The original
response bodies and IDs are retained across keyed retries. UI root, named
non-UUID paths and bare/events return exactly the direct UI Service response;
they are not captured by API aliases. Canonical SSE remains /api/v1/events.

Two missing contract regressions pass: committed unclaimed-resource status200,
and match202 for an existing WAITING workflow whose next attempt is in future.
The fixture was cancelled and its resource made OFFLINE after acceptance.
Full make check, generation and static route isolation tests passed. Contract
approach approved by both owners on issue120; no bare/events added.

The first UI-isolation assertion incorrectly assumed named SPA routes existed
and returned200. The existing UI returns404 for those paths; the final check
compares status, content type and body against the actual UI Service instead.
This proves route preservation rather than adding new UI routes.
