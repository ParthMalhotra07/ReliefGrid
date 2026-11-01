#!/usr/bin/env bash
# S4.3 chaos suite: delay-isolate-leader. See lib.sh, faults.sh and `delay-isolate-leader.sh --help`.
EXPERIMENT=delay-isolate-leader
DESCRIPTION="Delay one follower, then partition the leader from both peers (Chaos Mesh), so acks from before the cut are in flight while the old leader is read directly. As isolate-leader: the old leader serves no fenced read and commits nothing."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
