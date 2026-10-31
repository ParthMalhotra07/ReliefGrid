# A1.3 live verification checkpoints

## Transport correction

Initial unit tests passed. Full make check passed (both lint runs 0 issues,
all race packages and chaos-tag invcheck checks). The first live test verified
the fixed 5-second TTL, request.retry route, priority 3 and effective policies.
Its isolated-vhost cleanup then failed with EOF and later tests could not
connect to the shared port-forward.

A separate manual reproduction using the existing mq spike showed that deleting
an isolated vhost resets an AMQP connection and makes kubectl kill its entire
forward, including management:

```text
read tcp4 ...->127.0.0.1:5672: read: connection reset by peer
error: lost connection to pod
```

The existing kind broker remained healthy, with zero restarts. The verifier
now creates a disposable Docker broker at the exact image digest from Helm,
using direct loopback ports. The same test assertions and timeouts remain.
No cluster volumes or shared topology were removed. Tests still use isolated
vhosts, and cleanup removes only the task-created Docker container.

## Historical measurement checkpoint (approved later)

The same Verify after the transport correction passed:

```text
TTL -> relief.requests/request.retry -> matching.requests; priority=3; reason=expired
--- PASS: TestIntegrationRetryTTLDeadLetterRoutePreservesPriority (5.67s)
malformed -> reject(requeue=false) -> relief.dlx -> matching.dead
--- PASS: TestIntegrationMalformedDeliveryReachesMatchingDead (5.45s)
```

The third test then failed:

```text
integration_test.go:148: manual ack / prefetch mismatch: []
--- FAIL: TestIntegrationManualAckPrefetchAndInterruptedHandoff (0.30s)
```

The handler had received the valid broker delivery (the test awaited its entry
barrier), but the immediately queried management statistics had not yet exposed
the consumer. This is a faulty observation point, not evidence that prefetch or
manual acknowledgement is wrong. No test assertion was edited after this
failure. The exact prefetch=8, ack_required=true and interrupted-redelivery
checks remain in place.

Per the playbook SELF-CORRECTION LOOP step c, stopped and requested Parth's
approval to wait for the consumer's appearance in management statistics under
the existing 30-second context, then evaluate the same exact assertions. No
timeout increase, skipped assertion or arbitrary settling sleep is proposed.
Full make check passed again on the implementation after this failing Verify.
Raw transport and direct-broker output is preserved locally under .tools/.

## Continuation and final resolution

Parth requested continuation after the proposed measurement correction. Applied
only the bounded metadata observation change. The original exact assertions
then exposed a consumer shutdown bug: the inspection connection closed after
the cancellation path's five-second close budget. The concurrent Cancel RPC
started by ConsumeWithContext raced channel Close. Consumer now owns cancellation
using Consume plus its bounded setup/handler/close path. The same Verify passes.

Pre-PR regression tests found case-aliased retry properties reaching the durable
handler. Exact wire keys are now required before decoding; the same tests pass.
Integration-tag lint's S1038 finding was fixed by using t.Errorf with the same
format/arguments, preserving the assertion.

The first real Helm startup hook failed with exit 69 because await_startup
could not contact an Erlang node before it existed. The pinned CLI's wait
command monitors the local PID file; the template now sets a container-local
PID path and waits on it before importing definitions. The failed unready pod
blocked rolling replacement, so it was deleted in the dev cluster. No volume
was deleted. The corrected template deployed, installed both policies and
passed the existing 100-message/same-PVC restart verifier without intervention
on the second restart.

Final GITHUB_ACTIONS=true make check and embedded goal Verify both pass. All
corrections preserved the plan/invariants; no production failure exceeded one
code-correction attempt, and no passing assertion was weakened or removed.
