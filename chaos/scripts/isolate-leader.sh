#!/usr/bin/env bash
# S4.3 chaos suite: isolate-leader. See lib.sh, faults.sh and `isolate-leader.sh --help`.
EXPERIMENT=isolate-leader
DESCRIPTION="Partition the shard leader from both peers under load (Chaos Mesh NetworkChaos, both directions). The isolated node commits nothing and refuses every fenced read (a probe reads it directly); the majority elects a higher-term leader and goes on; then it rejoins."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
