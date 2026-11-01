# A1.4 dependency refresh — 31 October 2026

Integrates the #61 fixes at 40e48fb and merged main/#68 at e9178df. The existing
#62 read-options interface and its adapted PendingOutbox call are preserved by
the automatic merge; no shared interface or owner source was hand-edited.

Fresh GITHUB_ACTIONS=true make check passed all repository/chaos race packages,
lint (0 issues), A1.3's pinned-broker Verify and A1.4's real broker stop/restart,
mandatory-return and duplicate delivery checks. Integration vet/lint passed;
generation and tidy were clean. See make-check.log.

The first run's disposable broker failed its startup reachability check although
the broker log reported startup. That old check did not retain connection-error
details. A clean diagnostic broker was reachable after nine observations; it
did not reproduce the original failure. The verifier's one-shot host-port sample
was therefore hardened rather than raising its startup budget or suppressing a
failure: A1.3 and A1.4 now rediscover current mappings during the existing 60-step
readiness loop, reject absent/invalid loopback mappings, and preserve the last
connection error/address for a red run.

The final full run deliberately returned empty host-port mappings for the first
two port queries of each disposable broker. Both real pinned-broker Verifies
recovered and passed within their unchanged budgets. The control affects only
Docker port discovery, never storage/FSM rules, event dispositions or broker
assertions. The real mid-batch stop/restart and all exact assertions remain.
port-discovery.txt records that control; the full output includes both initial
unavailable samples for each broker.

The three domain-event patterns now route to the bounded durable matching.wakeups
queue inherited from #61. Its A2.4 handler is still a later goal, documented in
that PR. Unknown routes still remain pending on mandatory return. Human reviews,
#62's final owner rebase/merge and post-merge verification remain gates.
