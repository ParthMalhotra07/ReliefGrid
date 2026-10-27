#!/usr/bin/env bash
# S2.3 failure harness: restart-shard. See lib.sh and `restart-shard.sh --help`.
EXPERIMENT=restart-shard
DESCRIPTION="Restart all three replicas of a shard under load (delete every pod); the group re-forms from its disks at a higher term, with no self-bootstrap and no lost write."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
