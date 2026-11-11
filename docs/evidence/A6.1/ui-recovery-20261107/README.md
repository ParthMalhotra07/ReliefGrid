# A6.1 UI recovery follow-ups

Two regression cases from the #112 review are fixed:

- A first authoritative read failure now shows unavailable lists and a retry action, rather than loading forever.
- A refresh requested while a read is already running waits for the following authoritative read. Overlapping callers coalesce, including after an error.

The saved snapshot remains visibly stale on failure; events remain hints, and cancellation still waits for committed cleanup.
There are no create buttons in this UI. Any future create form must send a stable Idempotency-Key for retries and double submits.

Real red and green output is retained beside this file. Browser verification: 10 passed (6.3s). Node verification: 6 passed, 0 failed.
The checked-in UI bundle was rebuilt with the pinned Node and npm dependencies. Full make check and generation checks passed before commit; current-main verification is added before push.

PRE-PR REVIEW: READY. No protocol, contract or S-owned source changes. Both fixes have failing-first regressions; no tests were skipped or weakened. Remote reads retain their abort deadline; unmount still aborts the active request. No secrets, machine paths, local playbooks or attribution are staged.
