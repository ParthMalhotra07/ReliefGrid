# A1.3 verified messaging topology and consumer

This goal declares the four planned exchanges, five durable single-member
quorum queues, retry TTLs/explicit dead-letter routes and bounded safety
policies. The manual-ack consumer re-reads authoritative request state before
calling a durable handler. Unknown handoff/read failures are never acknowledged.
Malformed events reach matching.dead; an interrupted valid handoff redelivers.

## Test first and approved correction

The early plan was pushed as draft PR #61 before implementation. Original
normal/integration checks failed because internal/mq had no production files;
see test-first.md and raw red logs. Parth approved `ok, continue`.

The initial statistics assertion ran before RabbitMQ's management consumer
metadata was visible. Parth's later instruction to continue approved the
proposed correction: await metadata within the original 30-second context,
then apply the identical prefetch=8/manual-ack/redelivery assertions. No timeout
was increased and no assertion was skipped or loosened.

The corrected observation exposed a real shutdown bug. ConsumeWithContext's
concurrent Cancel RPC raced our channel Close; cleanup hit its deadline and
closed the shared connection. The consumer now owns cancellation and channel
closure. The same live test passes with the inspection connection preserved.

Review added regression cases for retry property case aliases. They failed
before the strict-key code fix and now pass. The embedded creation schema is
checked byte-for-byte against the unchanged frozen contract. Nine unit/topology
tests and three live integration tests run, plus their subcases.

## Actual final checks

```text
go vet -tags integration ./internal/mq/...
golangci-lint run --build-tags integration ./internal/mq/...
0 issues.

GITHUB_ACTIONS=true make check
0 issues.
0 issues.
go test -race ./...
ok github.com/ParthMalhotra07/ReliefGrid/internal/mq
go test -race -tags chaos ./cmd/invcheck/...
ok github.com/ParthMalhotra07/ReliefGrid/cmd/invcheck
make verify-A1.3
TTL -> relief.requests/request.retry -> matching.requests; priority=3; reason=expired
malformed -> reject(requeue=false) -> relief.dlx -> matching.dead
prefetch=8; manual ack=true; interrupted handoff redelivered
ok github.com/ParthMalhotra07/ReliefGrid/internal/mq 8.823s
A1.3 VERIFY: ALL PASS
```

Full repository race checks passed. The CI path above actually creates the
exact pinned broker and runs the live tests, rather than relying on compile-only
CI coverage. make gen-check and go mod tidy -diff left no changes.

## Real Helm and persistence check

```text
bash spike/mq.sh
STATUS: deployed
REVISION: 7
confirmed=100
pvc_same_uid=3d982505-ac00-4eb2-b6ec-8b8b0cafb9b2
received=100
priority=strict:[31 15 7 2 1]
priority=urgency:[3 2 1]
priority=omitted-default-4:[7 0 1]
PASS A0.3 RabbitMQ restart verification
```

Both named policies were observed on the kind broker after restart. The current
pod uses the corrected revision, is Ready and has zero container restarts.
The initial hook used await_startup before the Erlang VM existed and exited 69.
The fix uses the pinned CLI's PID-file-aware wait. The failed revision's unready
pod initially blocked StatefulSet rolling replacement; deleting that dev pod
allowed the corrected template to start, preserving the PVC. The second pod
restart in the existing A0.3 verifier then completed without manual intervention.

Raw final outputs: make-check.log (including the live Verify), helm-live.log,
envelope-red.log and envelope-green.log. Earlier correction history is in
self-correction.md; full diagnostic console output is retained locally in .tools/.

## Pre-PR review

READY for A1.3: authoritative rereads; no ownership based on event snapshots;
manual ack after the documented durable handler boundary; failed reads retain
the delivery; bounded setup/read/work/ack/close operations; no reconnect or
nack hot loop; race/lint/goal Verify clean. New code is A-owned messaging/app
Helm plus shared Makefile/module/evidence changes. No S-owned or frozen contract
file changed; no local-only files are staged. jsonschema/v6 was already pinned
at v6.0.2 and is now direct because the consumer uses it.

The single-broker failure-domain limit, A1.4 relay, A2.2 business recovery,
A1.5 process/deployment wiring and later event subscribers are explicitly
documented in internal/mq/README.md. This evidence does not claim the complete
public API/matcher cluster smoke, nor goal completion before S review/human
merge and fresh main verification.
