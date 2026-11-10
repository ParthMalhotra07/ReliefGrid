# Returned-state regression correction

Conflict now permanently asserts Age returns CANCELLED/v3, not the earlier
WAITING/v2 read. A real-FSM historical-OK wrapper commits aging(v3), commits
cancellation(v4), then replays the unchanged aging command(OK/v3). The test
requires Age's returned record to be current CANCELLED/v4.

Baseline passed; replacing the final authoritative reread with return rec,nil
failed both tests with the exact stale WAITING/v2 record. Mutation output is
retained and production restored immediately. All aging regressions passed
with race/count3, full make check and gen-check passed. No production aging
change or new physical run was needed. Rollout prerequisites now explicitly
include the coordinator and all nine storage replicas; default0s is preserved.
