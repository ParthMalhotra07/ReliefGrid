#!/usr/bin/env bash
# S2.3 failure harness: kill-leader. See lib.sh and `kill-leader.sh --help`.
EXPERIMENT=kill-leader
DESCRIPTION="Kill the leader of one shard under load (delete its pod); a leader at a higher term must take over within ELECTION_MAX_SECONDS and every acknowledged write must survive."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
