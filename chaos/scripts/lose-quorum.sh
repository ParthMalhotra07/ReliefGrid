#!/usr/bin/env bash
# S4.3 chaos suite: lose-quorum. See lib.sh, faults.sh and `lose-quorum.sh --help`.
EXPERIMENT=lose-quorum
DESCRIPTION="Partition every replica of one shard from the other two under load (Chaos Mesh). No write is acknowledged and no linearizable read served on that shard while it holds (probes read each replica), the other shards go on, and it recovers after the heal."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
