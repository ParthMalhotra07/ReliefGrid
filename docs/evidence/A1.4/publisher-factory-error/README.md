# Failed publisher construction — A1.4

A fresh app deployment exposed a nil-pointer panic when RabbitMQ was not yet
listening. mq.NewPublisher returns a typed nil pointer plus an error; after
conversion to the Publisher interface it is non-nil. Relay.runShard retained
that failed value and called Close on it during retry/cleanup.

The worker now retains a publisher only when construction succeeds. Failed
dials retain all pending events and use the existing cancellable backoff. No
publication or mark occurs through an invalid publisher. The real-FSM worker
regression first reproduces the panic (red.log), then proves construction
retries, the event drains through a healthy publisher, and all three ready
publishers close exactly once on cancellation (green.log).

The captured kind startup stack is in startup-panic.log. Full make check,
including live A1.3/A1.4 verification, and generation checks passed. This does
not change operation identity, confirmation/mark ordering, retry deadlines,
contracts, S-owned source or broker delivery policy.

Pre-PR review: READY. The fix is confined to construction-error handling; the
new regression exercises the actual multi-worker relay and real SQLite FSM.
