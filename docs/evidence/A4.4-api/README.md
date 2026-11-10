# A4.4 ETags and cursor implementation

Stacked on #134 (contract proposal). Aging and internal authentication are not
claimed complete: S-owned seams are #135 and #29 respectively.

Red-first tests reported missing creation ETag,409 instead of conditional412,
accepted stale resource update and a cursor walk visiting only2/29records.
After implementation make check and gen-check PASS. Focused real-FSM tests pass
race/count3 (3.223s), including commit-time CAS race vs unknown outcome, original
keyed-create response ETag, weak/list/wildcard/invalid tags, all29requests visited
once, spatial filtering on every page, invalid/mismatched cursors and partial-scan
failure. Actual output is retained here.

PRE-PR REVIEW: READY for review of the implemented ETag/cursor scope. Conditional
preconditions are checked again by the committed CAS; unknown outcomes stay503.
Cursor continuation consumes merged page leftovers before advancing opaque Store
state. No record/ownership history is cached in a token. Remote reads retain
request/call deadlines and the bounded scan budget; partial/missing observations
fail503. Only app code changes; no S-owned implementation changes. The internal
cursor validator is reused without constructing new Store cursors. Generated
contract types live in parent #134. No credentials or local-only files included.

Limits: M1 fixed ring, no global snapshot during concurrent writes, no migration
snapshot claim. Owner approval/merge of #134 must precede implementation merge.
No claim of a new full demo gate or completed A4.4 aging/authentication.
