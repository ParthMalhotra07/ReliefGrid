# Final-package checker verification

Baseline main764ced55c742e2c8d8907a738376397897f58dad. red.txt records the missing
module before implementation. red-atime.txt records a permanent regression that
reproduced reading an old artifact incorrectly counting its atime update as a
content change. The fix preserves inode/mode/size/mtime/ctime checks and checksum
validation while allowing access-time changes caused by the reader itself.

Fixtures exercise a synthetic package (not real delivery media/results), actual
temporary Git tags/trees, the CLI/output guards, complete inventory/source ADR/
contract coverage, hash mismatches/path traversal/symlinks/extra files, pending
results/missing signoffs, historical course-gate qualification, baseline/runtime
binding and exact offline archive/isolation coverage. Historical report prose
containing pending qualifications remains unchanged and accepted as prose.

Unsafe disposable mutations bypassing artifact checksums and pending-measurement
rejection both fail the intended regressions. Mutation output is retained with
trailing whitespace only normalized. Go embeds both Python inputs for cache keys.
focused.txt, go-focused.txt, check.txt and gen-check.txt retain verification output.

Pre-PR author self-review: READY for mechanical consistency scope; co-owner
review remains required. No submission, real package, gate, performance run,
offline run, recording/playback or public-visibility check was executed. Supplied
summary/signoff declarations are not independently certified by this tool. It
does not create a release tag, rewrite historical evidence or waive pending
report format, #126 measurements, auth, rollout or delivery requirements.
