# shellcheck shell=bash
# Kind-only guard (S5.1). Source it, then call require_kind before any tool runs:
#
#   require_kind <kube context> <script name>
#
# Exits 2 unless the context is a kind context (kind-*). The scripts that use it run kind,
# docker exec/stop/stats or conntrack against kind nodes, or delete and recreate the kind
# cluster: on GKE they would act on the wrong thing or nothing at all. GKE has its own scripts
# (scripts/gke-deploy.sh, scripts/gke-teardown.sh), and the GKE leader-kill subset
# (kill-leader, kill-follower, demos 6 and 7) uses kubectl only.
require_kind() {
	case "$1" in
	kind-*) return 0 ;;
	esac
	echo "$2 is kind-only (kind, docker or conntrack on kind nodes, or a kind cluster rebuild); refusing context $1" >&2
	exit 2
}
