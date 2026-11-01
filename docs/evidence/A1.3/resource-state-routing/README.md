# Resource update wakeup routing — A1.3 follow-up

Public status/window updates commit `resource.state_changed` events on
`relief.events`. No binding matched that exact key, so mandatory relay sends
returned NO_ROUTE and the durable outbox never drained. The clean demo-2
invcheck captured two pending events before this fix.

Adds the exact routing key to durable `matching.wakeups` in both embedded
runtime definitions and Helm's import file. Existing exchanges, queues, policy
arguments, registered-resource/request/match routes and schemas are unchanged.
The parity/binding test first failed on the missing binding (red.log), then
passed. Live pinned-broker verification now publishes that exact key with
mandatory routed confirmation and receives the persistent wakeup unchanged.

Full make check, live A1.3 Verify and generation checks passed. No failed event
is marked without publication. A2.4 still owns consuming/re-reading wakeups;
this fix only gives the event a durable destination. No frozen contract or
S-owned implementation changes.

Pre-PR review: READY. Additive binding only, with exact-key routing and
runtime/Helm parity covered; no ack/confirm/closure invariant is weakened.
