# shellcheck shell=bash
# Shared by lib.sh (every demo) and gate.sh: where the repository and the evidence are,
# and what code and tools a run used.

DEMOS_DIR_SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$DEMOS_DIR_SELF" rev-parse --show-toplevel)"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$REPO_ROOT/docs/evidence}"
CLUSTER="${CLUSTER:-reliefgrid}"

# The code under test, not the commit: committing evidence (or any docs change) must not
# make it stale, so only these paths are fingerprinted.
CODE_PATHS=(go.mod go.sum Makefile tools.mk contracts cmd internal loadtest deploy scripts/demos chaos)

# code_tree: 12 hex digits identifying the committed code under CODE_PATHS
code_tree() {
	git -C "$REPO_ROOT" ls-tree -r HEAD -- "${CODE_PATHS[@]}" | git hash-object --stdin | cut -c1-12
}
# code_dirty: yes if those paths have uncommitted changes
code_dirty() {
	if git -C "$REPO_ROOT" status --porcelain -- "${CODE_PATHS[@]}" | grep -q .; then echo yes; else echo no; fi
}
utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# versions: one line per tool and image; a missing one says so, never fails
versions() {
	local ctx="${KUBE_CONTEXT:-kind-$CLUSTER}" img
	echo "git head: $(git -C "$REPO_ROOT" rev-parse --short=12 HEAD 2>/dev/null || echo unavailable)"
	echo "go: $(go version 2>/dev/null || echo unavailable)"
	echo "kubectl: $(kubectl version --client -o json 2>/dev/null | jq -r .clientVersion.gitVersion 2>/dev/null || echo unavailable)"
	echo "kubernetes ($ctx): $(timeout 5 kubectl --context "$ctx" version -o json 2>/dev/null | jq -r '.serverVersion.gitVersion // "unreachable"' 2>/dev/null || echo unreachable)"
	echo "helm: $(helm version --short 2>/dev/null || echo unavailable)"
	echo "kind: $(kind version 2>/dev/null || echo unavailable)"
	echo "docker: $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo unavailable)"
	for img in reliefgrid/storagenode:dev reliefgrid/coordinator:dev; do
		echo "image $img: $(docker image inspect --format '{{.Id}}' "$img" 2>/dev/null || echo unavailable)"
	done
}

# Cordons. A demo that stops a storage pod cordons its worker (demo 6 hands the cordon to demo
# 7). Every such cordon carries DEMO_CORDON_LABEL, so restore_demo_cordons can undo exactly
# those: each demo runs it on exit (when it does not hand the cordon on), the gate on exit or
# interrupt, and reset.sh before anything else.
DEMO_CORDON_LABEL=reliefgrid.io/demo-cordoned
_kctx() { kubectl --context "${KUBE_CONTEXT:-kind-$CLUSTER}" "$@"; }
# demo_cordon <node>: cordon it and label it as the demos' cordon
demo_cordon() { _kctx cordon "$1" >/dev/null && _kctx label node "$1" "$DEMO_CORDON_LABEL=true" --overwrite >/dev/null; }
# demo_uncordon <node>: uncordon it and drop the label
demo_uncordon() { _kctx uncordon "$1" >/dev/null && _kctx label node "$1" "$DEMO_CORDON_LABEL-" >/dev/null 2>&1; true; }
# restore_demo_cordons: uncordon every node the demos cordoned; prints the nodes it restored
restore_demo_cordons() {
	local n
	for n in $(_kctx get nodes -l "$DEMO_CORDON_LABEL=true" -o name 2>/dev/null); do
		demo_uncordon "${n#node/}" && echo "${n#node/}"
	done
}
# cordoned_nodes: every unschedulable node, whoever cordoned it
cordoned_nodes() { _kctx get nodes -o json 2>/dev/null | jq -r '.items[] | select(.spec.unschedulable == true) | .metadata.name'; }
