# A1.4 test-harness STOP — 30 October 2026

The first full `GITHUB_ACTIONS=true make check` reached live A1.4 acceptance.
Repository vet/lint/race tests and the live A1.3 verifier passed. Nine relay tests
passed, including the production coordinator-client path and independent shard
workers. Live mandatory return followed by routed publish also passed.

Exact failure:

```
integration_test.go:99: broker restart: context deadline exceeded
integration_test.go:223: broker admin DELETE ... dial tcp 127.0.0.1:62371: connect: connection refused
--- FAIL: TestIntegrationBrokerStopsMidBatchAndPendingRowsRecover (90.00s)
```

Docker inspect showed empty HostPort values (ephemeral assignment). After restart
the same container fd46b4249982 exposed management on 62376 and AMQP on 62377,
while the test still used management port 62371. Broker logs reported successful
recovery and listeners at 14:24:53 UTC, startup 8887 ms, with matching.requests
re-elected leader and its prior log recovered. The subsequent duplicate test
could not start because it inherited the stale management address. It did not
exercise the duplicate assertion, and no pass is claimed for that test.

The disposable container and its anonymous data were removed by verifier cleanup;
no shared broker, kind pod or PVC was touched. No code or timeout changes were
made after this failed run. Full output: first-check-failed.log.

The test harness needs to rediscover the same owned container's host ports before
initial setup and after restart, share its updated address with cleanup, and
use those addresses in following tests. The exact proposed helper change is
`.tools/A1.4-port-correction.patch` outside the repository. It changes no failure,
persistence, count, priority or duplicate assertions and keeps the 90-second budget.

Per A-Playbook Part 3A(c), a test needing correction requires Parth's approval.
No A1.4 commit/push/PR was made because the required full check is not green.
Existing A1.3/#61 remains seven checks green and unmerged.

## Resolved after explicit approval

Parth approved the port-discovery correction and continuation. The helper now
rediscovers ports of the same verifier-owned container at setup and restart;
the updated pointer is shared with cleanup. All assertions and the 90-second
budget are unchanged. The full check and all live acceptance cases then passed.

The additional integration lint found `QueueInspect` deprecated (SA1019).
The pinned v1.16.0 source provides `QueueDeclarePassive`; the helper now uses
that passive inspection with the identical zero-ready-messages assertion.
No test was skipped, weakened, deleted or given a larger timeout.
Pre-PR review also corrected the new matcher entrypoint to suppress errors only
when the external signal context is cancelled: a fatal relay error joined with
its sibling worker's cancellation must still produce a nonzero process exit.
