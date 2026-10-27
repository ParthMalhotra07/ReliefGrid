#!/usr/bin/env bash
# S2.3 failure harness: isolate-replica. See lib.sh and `isolate-replica.sh --help`.
EXPERIMENT=isolate-replica
DESCRIPTION="Cut one replica's Raft port (7000) both ways under load, by the chart's NetworkPolicy (label reliefgrid.io/chaos-isolated=true); the isolated replica commits nothing while the majority goes on, then it rejoins."
ROLE_FLAG=1
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
