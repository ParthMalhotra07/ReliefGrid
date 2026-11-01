#!/usr/bin/env bash
# S4.3 chaos suite: delay-follower. See lib.sh, faults.sh and `delay-follower.sh --help`.
EXPERIMENT=delay-follower
DESCRIPTION="Delay one follower's traffic with both peers (Chaos Mesh, 300 ms +-50 ms) under load. The majority keeps committing; the follower's lag rises and converges after the heal. With --stale-readers, the negative control: stale reads of the lagging follower must be caught."
# shellcheck source=chaos/scripts/lib.sh
. "$(dirname "$0")/lib.sh"
chaos_main "$@"
