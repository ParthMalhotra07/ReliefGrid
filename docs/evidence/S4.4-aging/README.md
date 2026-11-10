# S4.4 transactional aging seam

Base: main 0fa3a63. Red tests committed first as 54536fb. No physical cluster was
used. The original red outcomes were unknown AgeRequest, missing effective urgency,
missing Store/memstore method and HTTP 404. Test setup corrections during green:
fresh operation ID after a definitive cancellation conflict, OpenImage for an
existing snapshot, explicit published:false in a route fixture, and ExecContext
for failure injection. The exhaustive coordinator route suite now includes /age.

- `focused.txt`: `go test -race -count=3 ./internal/fsm ./internal/fsm/memstore ./internal/storagenode ./internal/coordclient -run TestAgeRequest`.
- `check.txt`: complete successful `make check` output.
- `gen.txt` / `gen-check.txt`: regeneration and staged-generation drift check.
- `mutation-threshold.txt`: exact boundary shifted by 1 ms; exact-threshold test fails.
- `mutation-cap.txt`: interval cap removed; extreme-timestamp capped-result test fails.
- `mutation-noop.txt`: achieved-target no-op disabled; duplicate aging test fails.
- `mutation-rollback.txt`: commit partial record on outbox failure; unchanged-record assertion fails.

All four mutations were individually reverted. Generated code and the additive
internal contract belong to this PR and require both owners' approval. Immediate
request.retry routing on relief.requests remains pending Parth's agreement. The
seam is not enabled by a reconciler here; A owns that integration and subsequent
effective-priority event production. Public stale If-Match behavior depends on
#136; this branch establishes the entity-version increment, not its public 412
integration. SQLite image/log tests do not claim physical Raft crash acceptance.
