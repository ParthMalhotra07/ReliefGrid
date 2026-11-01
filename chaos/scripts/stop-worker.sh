#!/usr/bin/env bash
# S4.3 chaos suite: stop-worker. See lib.sh, faults.sh and `stop-worker.sh --help`.
EXPERIMENT=stop-worker
DESCRIPTION="Stop the kind node of a data worker under load (docker stop; Chaos Mesh can't stop a node), taking one replica of every shard; every shard keeps acknowledging; then docker start and its pods return."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
