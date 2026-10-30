# A1.3 test-first checkpoint

Base: main 904aaee. Draft PR #61 contains only the acceptance plan and passing
baseline/dependency evidence at 6deabc6. The failing tests are local until the
implementation passes make check; no failing commit was pushed.

Tests written before implementation:

- Current committed request is loaded before the handler; ack follows a real
  committed handoff, with bounded call contexts.
- An old PENDING hint cannot act on a request already CANCELLED in the real FSM.
- A failed read or durable handler returns its error and does not ack, reject or
  immediately nack/requeue the valid work. Ack failures are surfaced.
- Malformed JSON/version/identity events are rejected without requeue.
- The current workflow's actual durable retry envelope is accepted and causes
  an authoritative reread of its WAITING schedule.
- Definitions describe all four exchanges, five quorum queues, explicit group
  size one, retry TTLs, six bindings and dead-letter safety policies; Helm and
  runtime definitions cannot drift.
- Live broker checks: fixed 5-second expiry, explicit request.retry route,
  original body and priority retained, and x-death expired provenance; malformed
  delivery reaches matching.dead with rejected provenance; manual ack/prefetch 8
  and interrupted handoff redelivery. Live tests use isolated, disposable vhosts.

Actual output from `bash scripts/verify-A1.3.sh`:

```text
github.com/ParthMalhotra07/ReliefGrid/internal/mq: build constraints exclude all Go files in /mnt/c/Users/am/Desktop/CC/.reviews/A1.3/internal/mq
FAIL github.com/ParthMalhotra07/ReliefGrid/internal/mq [build failed]
FAIL
```

Actual output with `go test -race -tags integration -count=1 ./internal/mq/...`:

```text
github.com/ParthMalhotra07/ReliefGrid/internal/mq: no non-test Go files in /mnt/c/Users/am/Desktop/CC/.reviews/A1.3/internal/mq
FAIL github.com/ParthMalhotra07/ReliefGrid/internal/mq [build failed]
FAIL
```

These are the intended missing-implementation failures. They do not prove any
runtime behavior yet. Go formatting and shell syntax checks completed cleanly.
Broker version was independently observed as 4.3.7 and stream_queue enabled.
Raw red output is stored alongside this note. Implementation awaits Parth's
test-first approval under the playbook GOAL LOOP step 5.
