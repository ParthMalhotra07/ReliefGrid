# Schedule runner internal authentication [A4.4]

The regression first reproduced missing app credentials at actual cluster
fixture startup and the raw restart/ring probe, invalid-file acceptance and
credentialed redirects (red.txt). The new callers load only the app sender file
for internal HTTP; management seam calls retain their operator bearer token.
Memory-only M8 does not read cluster credentials or require Helm.

Permanent runner tests exercise the actual shell with a mocked shared-helper
boundary: every readiness/outbox curl and child Go invocation sees the app
sender file; unreadable state refuses before mutation; exit cleanup runs after
test failure. The old jq fake now drains stdin before returning its chosen
verdict, preventing an unrelated producer SIGPIPE while preserving rejection.
The real shared helper is owned/tested in prerequisite #174.

Focused race checks, four unsafe mutations, tagged schedules lint, full
make check and generation checks pass. main-integration/ records the independent
validation on main b77e5b9761119a73bed8f2d3eede40c863492275 before stacking on
#174 for the shared staging implementation. Root-level outputs will record
the final parent-stack validation. No protocol assertion, deadline or safety
invariant was relaxed.

No kind, RabbitMQ campaign, 50-seed acceptance, enabled rollout or credential
rotation was executed for this auth-only change. The joint enabled run and
rotation remain required. #174's configuration-type review finding must be
fixed before the stack merges. Use SCHEDULE_RESET=0 for the joint enabled check
so the runner does not replace the deliberately enabled deployment with defaults.
