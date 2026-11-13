# Review follow-up: operational mount contract and disabled redirects

The original tests accepted a wrong Pod mount directory while the fixed env
path remained unreadable. The shared emitted-Pod assertion now pins the volume
name and exact read-only mount directory as well as the existing Secret/key/
projected filename/mode/group/env path. Actual demo invcheck, recovery worker
and final audit Job paths all use that assertion. Directory and name mutations
now fail, with green baselines; the earlier survivor output is retained.

An unset sender path also incorrectly installed redirect refusal. The legacy
GET redirect returned 200 before auth wiring but 302 afterward. A permanent
regression ran red, then passed after preserving the original anonymous opener
when disabled. Configured auth still rejects redirects and out-of-scope URLs;
invalid configured files still fail before I/O. Reintroducing redirect refusal
in disabled mode fails the new regression. No status codes are rewritten.

These are local HTTP/manifests tests, not a kind enablement or rotation run.
