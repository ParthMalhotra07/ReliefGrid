# A2.2 pre-PR review

Verdict: READY for ownership review, with dependency merges/main Verify still required.

- Invariants: exact intent/version fencing before voluntary closure; permanent
  closure proof before WAITING; unknown claim reuses immutable command; partial
  scans retain incomplete state. Historical results grant no current ownership.
- Ack boundary: domain due/event commit or routed persistent transport confirm;
  failed/unknown handoffs leave the source recoverable. Poison remains separate.
- Idempotency: lost responses, restart, duplicate before-due hints, closure replay
  and concurrent confirmation are covered by real-FSM tests and live AMQP cases.
- Concurrency/deadlines: remote Store calls and publisher setup/confirm are bounded;
  old consumer reconnect backoff remains; ordinary/chaos/tagged race tests pass.
- Errors: rejection/unknown stay distinct; wrapped causes preserve errors.Is;
  no dropped intent or optimistic success after failed handoff.
- Tests: no existing assertion weakened, test skipped or timeout increased;
  new long live test budget derives from 25 fixed TTL hops. Red checkpoints retained.
- Scope/hygiene: A-owned source/shared tooling only; frozen contracts/generated
  files unchanged; no local-only instructions, secrets, S code or attribution staged.

The parent is refreshed to b7aaff2 with the independently verified A1.4/A1.5
review fixes and merged A1.3. A final GITHUB_ACTIONS=true make check plus tagged
vet/lint/gen/tidy passed on that source stack; actual output is in this directory.
