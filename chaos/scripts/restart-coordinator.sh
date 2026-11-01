#!/usr/bin/env bash
# S4.3 chaos suite: restart-coordinator. See lib.sh, faults.sh and `restart-coordinator.sh --help`.
EXPERIMENT=restart-coordinator
DESCRIPTION="Scale the coordinator to zero under load and hold it down past the grace (COORDINATOR_DOWN_SECONDS, 10 s more). The new one serves the same ring, no group re-elects or re-bootstraps, and every shard acknowledges writes again (outcomes that went unknown are resolved by operation ID)."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
