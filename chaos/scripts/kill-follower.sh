#!/usr/bin/env bash
# S2.3 failure harness: kill-follower. See lib.sh and `kill-follower.sh --help`.
EXPERIMENT=kill-follower
DESCRIPTION="Kill one follower of a shard under load (delete its pod, chosen by the seed); the majority keeps acknowledging writes, and the follower returns and catches up."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
