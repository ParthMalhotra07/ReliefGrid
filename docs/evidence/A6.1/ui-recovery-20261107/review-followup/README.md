# Review follow-ups for #140

Resources now carries the same LATEST READ / SAVED VIEW marker as Requests. A browser regression checks live → failed authoritative read → successful refresh recovery.

A ref guard is checked and set synchronously before submitting a write and released in finally after its authoritative reread. React busy state still drives the visible disabled state. The regression reenters the same rendered action handler in one browser task, before React can commit busy state: the old code submitted two writes; the fixed code submits one. This uses the pinned React version's DOM props attachment only in the test, with no production test hook.

Failing-first output: 2 failed (missing Resources label, Expected 1 / Received 2 writes). After the fix: 12 passed (6.2s), including all existing offline, reconnect, stale-read, unknown-outcome and cancellation cases. Node tests: 6 passed. make check and make gen-check passed; static assets rebuilt. PRE-PR REVIEW: READY. No contract or S-owned changes, no skipped or weakened tests, no credentials or local-only files staged.
