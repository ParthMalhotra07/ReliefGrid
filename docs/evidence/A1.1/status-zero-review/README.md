# Status-zero unknown response regression

Adds the exact requested coordinator error without Status, expecting HTTP 503
OUTCOME_UNKNOWN, retryable=true and outcomeUnknown=true for both CreateEntity
and CompareAndSet. Existing Retry-After and no-accidental-write assertions apply.
The implementation already handles this correctly; only the missing table row
was added. Temporarily gating unknown handling on nonzero Status fails both new
cases (mutation.log); the original source was restored byte-for-byte and the
same refusal suite and full make check then passed. Generation stayed unchanged.

Independent reviews of #71/#73 exposed a separate shared coordinator timing
failure tracked in #26. It is outside this API test-only change and is not
claimed fixed here. This PR still needs S re-review, CI and main verification.
