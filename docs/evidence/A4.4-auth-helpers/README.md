# A4.4 internal authentication helper verification

Baseline: main 764ced55c742e2c8d8907a738376397897f58dad. Local verification on
10 November 2026, using the pinned Go toolchain. Only the root session worked;
no new independent-agent review is claimed. S's owner review remains required.

`red.txt` records the test-first failure because the proposed helper interfaces
did not exist. `focused.txt` records three race-enabled repetitions of the auth
suite. `check.txt` and `gen-check.txt` record required repository verification.
`red-bounds.txt` records an unbounded request reaching the injected transport
before the final deadline guard was added; the permanent regression also checks
delegation of idle-connection cleanup.
The first full check's test-only context/header-access lint findings are retained
in `check-attempt1.txt`; they were corrected without removing coverage.

The suite checks invalid configuration before I/O, formatting/JSON/slog redaction,
current/previous acceptance and retirement, credential-class separation, strict
single-header handling, concurrent request/client ownership, credential/header
replacement, nested wrapper rejection and all five redirect status codes with an
injected permissive client. Inherited operator/proxy credentials are stripped.

Two unsafe mutations were tested in disposable package copies, never in the
working source: allowing redirects makes the redirect regression fail for all
five status codes; removing the single-header count check makes the case-varied
duplicate regression fail. Their real nonzero output is retained in mutation logs.

Pre-PR self-review: READY for **helper scope**. No protocol/store/messaging path,
contract or S-owned file changes; no live route becomes authenticated. The
helpers cannot establish startup/route coverage, unknown-outcome behavior,
Secret rollout or authenticated end-to-end operation before owner integration.
Initial enablement, rotation deployment, operational callers and the protected
contract remain the #29 joint follow-up. Auth stays an unfinished part of A4.4.
