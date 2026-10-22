#!/usr/bin/env bash
# Verify check for S0.2 (kind cluster). Run from the repo root:
#   scripts/verify-kind.sh
#   IDLE_SETTLE_SECONDS=120 BASE=<rev> scripts/verify-kind.sh
# It deletes and recreates the local "reliefgrid" kind cluster, leaves it
# running, and writes idle-RAM evidence to docs/evidence/spike/ram-<host>.txt.
# Exits non-zero if any check fails; prints one PASS/FAIL line per check.
#
# Interface under test:
#   tools.mk          KIND_VERSION, KIND_NODE_IMAGE (kindest/node:vX.Y.Z@sha256:...)
#   cluster.yaml      deploy/kind/cluster.yaml; no image: lines (the pin lives in tools.mk);
#                     apiServerPort and every hostPort bound to 127.0.0.1
#   make cluster-up   creates the cluster (idempotent); make dev does the same for now
#   make cluster-down deletes it (idempotent)
#   make doctor       versions + Docker daemon + available RAM + free ports; accepts
#                     KIND_CONFIG=<file> (ports to check), MEMINFO=<file> (default
#                     /proc/meminfo); ports already held by the reliefgrid cluster are ok.
#                     RAM threshold: 8 GiB (full profile), 4 GiB with PROFILE=lean,
#                     MIN_AVAILABLE_RAM_GIB=<n> overrides both
set -uo pipefail

CLUSTER=reliefgrid
CTX="kind-$CLUSTER"
BASE="${BASE:-origin/main}"
IDLE_SETTLE_SECONDS="${IDLE_SETTLE_SECONDS:-90}"
EVIDENCE="docs/evidence/spike/ram-$(hostname -s).txt"
CONFIG=deploy/kind/cluster.yaml

failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; [ -n "${2:-}" ] && printf '      %s\n' "$2"; failures=$((failures + 1)); }
check() { # check <description> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}
finish() {
	echo
	if [ "$failures" -eq 0 ]; then
		echo "verify-kind: all checks passed"
	else
		echo "verify-kind: $failures check(s) failed"
		exit 1
	fi
	exit 0
}

tmp="$(mktemp -d)"
listener=""
cleanup() {
	[ -n "$listener" ] && kill "$listener" 2>/dev/null
	rm -rf "$tmp"
}
trap cleanup EXIT

pin() { sed -n "s/^$1[[:space:]]*:\{0,1\}=[[:space:]]*//p" tools.mk 2>/dev/null | head -1; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'; }
k() { kubectl --context "$CTX" "$@"; }

kind_pin="$(pin KIND_VERSION)"
image_pin="$(pin KIND_NODE_IMAGE)"
k8s_version="$(sed -n 's/^kindest\/node:\(v[0-9]*\.[0-9]*\.[0-9]*\)@sha256:[0-9a-f]\{64\}$/\1/p' <<<"$image_pin")"

# --- 1. Pins --------------------------------------------------------------------
if [ -n "$k8s_version" ]; then
	pass "tools.mk: KIND_NODE_IMAGE pinned by tag and digest ($image_pin)"
else
	fail "tools.mk: KIND_NODE_IMAGE pinned by tag and digest" "got '$image_pin'"
fi
# kind embeds the node image(s) its release was built and tested with.
kind_bin="$(command -v kind || true)"
if [ -z "$kind_bin" ]; then
	fail "kind: installed"
elif [ "$(kind version 2>/dev/null | grep -Eo 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)" != "$kind_pin" ]; then
	fail "kind: installed version matches KIND_VERSION ($kind_pin)" "$(kind version 2>&1)"
else
	pass "kind: installed version matches KIND_VERSION ($kind_pin)"
	if [ -n "$image_pin" ] && grep -aqF "$image_pin" "$kind_bin"; then
		pass "kind: KIND_NODE_IMAGE is the image kind $kind_pin ships for"
	else
		fail "kind: KIND_NODE_IMAGE is the image kind $kind_pin ships for" \
			"kind embeds: $(grep -aoE 'kindest/node:v[0-9.]+@sha256:[0-9a-f]{64}' "$kind_bin" | sort -u | tr '\n' ' ')"
	fi
fi

# --- 2. Static config and targets ----------------------------------------------
check "cluster.yaml: $CONFIG exists" test -f "$CONFIG"
check "cluster.yaml: names the cluster '$CLUSTER'" grep -Eq "^name: *$CLUSTER\$" "$CONFIG"
if [ -f "$CONFIG" ] && ! grep -Eq '^[[:space:]]*image:' "$CONFIG"; then
	pass "cluster.yaml: no image: lines (node image comes from tools.mk)"
else
	fail "cluster.yaml: no image: lines (node image comes from tools.mk)"
fi
for t in cluster-up cluster-down dev doctor; do
	check "make: target '$t' exists" make -n "$t"
done
up_recipe="$(make -n cluster-up 2>/dev/null)"
if [ -n "$image_pin" ] && grep -Fq -- "--image $image_pin" <<<"$up_recipe" && grep -Fq -- "--config $CONFIG" <<<"$up_recipe"; then
	pass "make cluster-up: uses --image \$(KIND_NODE_IMAGE) and --config $CONFIG"
else
	fail "make cluster-up: uses --image \$(KIND_NODE_IMAGE) and --config $CONFIG"
fi

# --- 3. make doctor against stub tools (hermetic) ------------------------------
go_pin="$(pin GO_VERSION)"
lint_pin="$(pin GOLANGCI_LINT_VERSION)"
stub() { # stub <dir> <name> <output>
	printf '#!/bin/sh\nprintf "%%s\\n" "%s"\n' "$3" >"$1/$2"
	chmod +x "$1/$2"
}
write_stubs() { # write_stubs <dir> <docker info exit code>
	mkdir -p "$1"
	stub "$1" go "go version go${go_pin} linux/amd64"
	stub "$1" golangci-lint "golangci-lint has version ${lint_pin#v} built with go${go_pin} from 0000000 on 2026-10-16T11:33:37Z"
	stub "$1" kind "kind ${kind_pin} go${go_pin} linux/amd64"
	stub "$1" kubectl "Client Version: $(pin KUBECTL_VERSION)"
	stub "$1" helm "$(pin HELM_VERSION)+g0000000"
	cat >"$1/docker" <<-EOF
		#!/bin/sh
		case "\$1" in
		info) [ $2 -eq 0 ] && echo "Server Version: 29.9.1" || echo "Cannot connect to the Docker daemon" >&2; exit $2 ;;
		version) [ $2 -eq 0 ] && echo "29.9.1" || echo "Cannot connect to the Docker daemon" >&2; exit $2 ;;
		*) exit 0 ;;
		esac
	EOF
	chmod +x "$1/docker"
}
meminfo() { # meminfo <file> <MemAvailable GiB>
	printf 'MemTotal:       %d kB\nMemFree:        %d kB\nMemAvailable:   %d kB\n' \
		$((64 * 1048576)) $(($2 * 1048576)) $(($2 * 1048576)) >"$1"
}
kind_config() { # kind_config <file> <apiServerPort> <hostPort>
	cat >"$1" <<-EOF
		kind: Cluster
		apiVersion: kind.x-k8s.io/v1alpha4
		name: doctor-test
		networking:
		  apiServerAddress: 127.0.0.1
		  apiServerPort: $2
		nodes:
		  - role: control-plane
		    extraPortMappings:
		      - containerPort: 30080
		        hostPort: $3
		        listenAddress: 127.0.0.1
	EOF
}
write_stubs "$tmp/up" 0
write_stubs "$tmp/down" 1
meminfo "$tmp/mem-ok" 64
meminfo "$tmp/mem-low" 1
meminfo "$tmp/mem-6" 6
kind_config "$tmp/ports-free.yaml" "$(free_port)" "$(free_port)"
busy="$(free_port)"
python3 -m http.server --bind 127.0.0.1 "$busy" >/dev/null 2>&1 &
listener=$!
for _ in $(seq 50); do (exec 3<>"/dev/tcp/127.0.0.1/$busy") 2>/dev/null && break; sleep 0.1; done
kind_config "$tmp/ports-busy.yaml" "$(free_port)" "$busy"

doctor() { # doctor <stubdir> <meminfo> <kind config> <outfile> [make vars...]
	local dir="$1" mem="$2" cfg="$3" out="$4"
	shift 4
	PATH="$dir:$PATH" make doctor MEMINFO="$mem" KIND_CONFIG="$cfg" "$@" >"$out" 2>&1
}
if doctor "$tmp/up" "$tmp/mem-ok" "$tmp/ports-free.yaml" "$tmp/d-good"; then
	pass "make doctor: passes with pinned tools, Docker up, 64 GiB available, ports free"
else
	fail "make doctor: passes with pinned tools, Docker up, 64 GiB available, ports free" "$(tail -4 "$tmp/d-good" | tr '\n' '|')"
fi
for what in docker 'ram|memory' port; do
	if grep -Eiq "^ok.*($what)" "$tmp/d-good"; then pass "make doctor: reports an ok line for /$what/"; else fail "make doctor: reports an ok line for /$what/"; fi
done
expect_fail() { # expect_fail <desc> <outfile> <pattern> <stubdir> <meminfo> <kind config> [make vars...]
	local desc="$1" out="$2" pat="$3" dir="$4" mem="$5" cfg="$6"
	shift 6
	if doctor "$dir" "$mem" "$cfg" "$out" "$@"; then
		fail "make doctor: $desc" "it exited 0"
	elif grep -Eiq "$pat" "$out"; then
		pass "make doctor: $desc"
	else
		fail "make doctor: $desc" "output does not match /$pat/: $(tr '\n' '|' <"$out")"
	fi
}
expect_fail "fails and names docker when the daemon is unreachable" "$tmp/d-docker" \
	'(missing|fail|down|unreachable).*docker|docker.*(missing|fail|down|unreachable|cannot)' \
	"$tmp/down" "$tmp/mem-ok" "$tmp/ports-free.yaml"
expect_fail "fails and names RAM with 1 GiB available" "$tmp/d-ram" \
	'^(low|fail|short).*(ram|memory)' \
	"$tmp/up" "$tmp/mem-low" "$tmp/ports-free.yaml"
# Profile-aware threshold: 6 GiB is too little for the full profile, enough for lean.
expect_fail "fails with 6 GiB available under the default (full) profile" "$tmp/d-full6" \
	'^(low|fail|short).*(ram|memory)' \
	"$tmp/up" "$tmp/mem-6" "$tmp/ports-free.yaml"
if doctor "$tmp/up" "$tmp/mem-6" "$tmp/ports-free.yaml" "$tmp/d-lean6" PROFILE=lean; then
	pass "make doctor: passes with 6 GiB available under PROFILE=lean (4 GiB threshold)"
else
	fail "make doctor: passes with 6 GiB available under PROFILE=lean (4 GiB threshold)" "$(grep -Eiv '^ok' "$tmp/d-lean6" | tr '\n' '|')"
fi
expect_fail "MIN_AVAILABLE_RAM_GIB=7 overrides PROFILE=lean (6 GiB fails)" "$tmp/d-override" \
	'^(low|fail|short).*(ram|memory)' \
	"$tmp/up" "$tmp/mem-6" "$tmp/ports-free.yaml" PROFILE=lean MIN_AVAILABLE_RAM_GIB=7
if doctor "$tmp/up" "$tmp/mem-6" "$tmp/ports-free.yaml" "$tmp/d-override2" MIN_AVAILABLE_RAM_GIB=5; then
	pass "make doctor: MIN_AVAILABLE_RAM_GIB=5 overrides the full default (6 GiB passes)"
else
	fail "make doctor: MIN_AVAILABLE_RAM_GIB=5 overrides the full default (6 GiB passes)"
fi
expect_fail "fails and names port $busy when something listens on it" "$tmp/d-port" \
	"^(busy|fail|in use).*\\b$busy\\b" \
	"$tmp/up" "$tmp/mem-ok" "$tmp/ports-busy.yaml"
kill "$listener" 2>/dev/null
listener=""

# --- 4. Lifecycle: down is idempotent, make dev creates, up is idempotent ------
if [ -n "$(make -n cluster-up 2>/dev/null)" ]; then
	check "make cluster-down: succeeds" make cluster-down
	check "make cluster-down: succeeds again when no cluster exists" make cluster-down
	if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
		fail "make cluster-down: removes the '$CLUSTER' cluster"
	else
		pass "make cluster-down: removes the '$CLUSTER' cluster"
	fi
	if make dev >"$tmp/dev.out" 2>&1; then
		pass "make dev: creates the cluster"
	else
		fail "make dev: creates the cluster" "$(tail -4 "$tmp/dev.out" | tr '\n' '|')"
	fi
	check "make cluster-up: succeeds again when the cluster exists" make cluster-up
	check "kubeconfig: current context is $CTX" sh -c "[ \"\$(kubectl config current-context)\" = $CTX ]"
else
	fail "lifecycle: skipped (no make cluster-up)"
fi
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
	fail "cluster: '$CLUSTER' is running; skipping cluster checks"
	finish
fi

# --- 5. Node layout -----------------------------------------------------------
check "nodes: all Ready within 3m" k wait --for=condition=Ready nodes --all --timeout=180s
k get nodes -L role >"$tmp/nodes.txt" 2>&1
cat "$tmp/nodes.txt"
count() { k get nodes --no-headers -l "$1" 2>/dev/null | wc -l; }
total="$(k get nodes --no-headers 2>/dev/null | wc -l)"
cp_n="$(count node-role.kubernetes.io/control-plane)"
data_n="$(count role=data)"
infra_n="$(count role=infra)"
[ "$total" -eq 5 ] && pass "nodes: 5 in total" || fail "nodes: 5 in total" "got $total"
[ "$cp_n" -eq 1 ] && pass "nodes: 1 control plane" || fail "nodes: 1 control plane" "got $cp_n"
[ "$data_n" -eq 3 ] && pass "nodes: 3 workers labelled role=data" || fail "nodes: 3 workers labelled role=data" "got $data_n"
[ "$infra_n" -eq 1 ] && pass "nodes: 1 worker labelled role=infra" || fail "nodes: 1 worker labelled role=infra" "got $infra_n"
cp_role="$(k get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[*].metadata.labels.role}' 2>/dev/null)"
[ -z "$cp_role" ] && pass "nodes: control plane has no role label" || fail "nodes: control plane has no role label" "got '$cp_role'"
infra_taints="$(k get nodes -l role=infra -o jsonpath='{range .items[*].spec.taints[*]}{.key}={.value}:{.effect}{"\n"}{end}' 2>/dev/null)"
if [ "$infra_taints" = "role=infra:NoSchedule" ]; then
	pass "nodes: infra worker tainted role=infra:NoSchedule (and nothing else)"
else
	fail "nodes: infra worker tainted role=infra:NoSchedule (and nothing else)" "got '$(tr '\n' ' ' <<<"$infra_taints")'"
fi
data_taints="$(k get nodes -l role=data -o jsonpath='{.items[*].spec.taints}' 2>/dev/null)"
[ -z "$data_taints" ] && pass "nodes: data workers are untainted" || fail "nodes: data workers are untainted" "got '$data_taints'"
versions="$(k get nodes -o jsonpath='{range .items[*]}{.status.nodeInfo.kubeletVersion}{"\n"}{end}' 2>/dev/null | sort -u | tr '\n' ' ')"
if [ "$versions" = "$k8s_version " ]; then
	pass "nodes: every kubelet runs $k8s_version (the pinned node image)"
else
	fail "nodes: every kubelet runs $k8s_version (the pinned node image)" "got '$versions'"
fi

# --- 6. Host ports bound to loopback only ---------------------------------------
api_port="$(sed -n 's/^[[:space:]]*apiServerPort:[[:space:]]*\([0-9]*\).*/\1/p' "$CONFIG")"
host_ports="$(sed -n 's/^[[:space:]]*hostPort:[[:space:]]*\([0-9]*\).*/\1/p' "$CONFIG")"
published="$(docker ps --filter "label=io.x-k8s.kind.cluster=$CLUSTER" --format '{{.Ports}}' 2>/dev/null | tr ',' '\n' | sed 's/^ *//')"
[ -n "$api_port" ] && pass "cluster.yaml: fixed apiServerPort ($api_port)" || fail "cluster.yaml: fixed apiServerPort"
for p in $api_port $host_ports; do
	if grep -Eq "^127\.0\.0\.1:$p->" <<<"$published" && ! grep -Eq "^(0\.0\.0\.0|\[?::\]?):$p->" <<<"$published"; then
		pass "ports: $p published on 127.0.0.1 only"
	else
		fail "ports: $p published on 127.0.0.1 only" "published: $(tr '\n' ' ' <<<"$published")"
	fi
done

# --- 7. The taint actually repels and admits ------------------------------------
ns=s02-verify
k delete namespace "$ns" --ignore-not-found --wait=true >/dev/null 2>&1
k create namespace "$ns" >/dev/null 2>&1
k apply -n "$ns" -f - >/dev/null 2>&1 <<-EOF
	apiVersion: v1
	kind: Pod
	metadata: {name: no-toleration}
	spec:
	  nodeSelector: {role: infra}
	  containers: [{name: pause, image: registry.k8s.io/pause:3.10}]
	---
	apiVersion: v1
	kind: Pod
	metadata: {name: with-toleration}
	spec:
	  nodeSelector: {role: infra}
	  tolerations: [{key: role, operator: Equal, value: infra, effect: NoSchedule}]
	  containers: [{name: pause, image: registry.k8s.io/pause:3.10}]
EOF
cond() { k get pod -n "$ns" "$1" -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].status}/{.status.conditions[?(@.type=="PodScheduled")].reason}' 2>/dev/null; }
for _ in $(seq 30); do
	[ "$(cond no-toleration)" = "False/Unschedulable" ] && [ "$(cond with-toleration | cut -d/ -f1)" = True ] && break
	sleep 1
done
[ "$(cond no-toleration)" = "False/Unschedulable" ] &&
	pass "taint: a pod without the toleration is not scheduled on infra" ||
	fail "taint: a pod without the toleration is not scheduled on infra" "PodScheduled=$(cond no-toleration)"
node="$(k get pod -n "$ns" with-toleration -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
if [ -n "$node" ] && [ "$(k get node "$node" -o jsonpath='{.metadata.labels.role}' 2>/dev/null)" = infra ]; then
	pass "taint: a pod with the toleration is scheduled on infra ($node)"
else
	fail "taint: a pod with the toleration is scheduled on infra" "nodeName='$node'"
fi
k delete namespace "$ns" --wait=true >/dev/null 2>&1

# --- 8. make doctor on this machine, cluster up --------------------------------
if make doctor >"$tmp/d-real" 2>&1; then
	pass "make doctor: passes on this machine with the cluster up (its own ports are ok)"
else
	fail "make doctor: passes on this machine with the cluster up (its own ports are ok)" "$(grep -Eiv '^ok' "$tmp/d-real" | tr '\n' '|')"
fi

# --- 9. Idle RAM evidence -------------------------------------------------------
echo "waiting ${IDLE_SETTLE_SECONDS}s for the cluster to go idle..."
sleep "$IDLE_SETTLE_SECONDS"
mkdir -p "$(dirname "$EVIDENCE")"
ids="$(docker ps -q --filter "label=io.x-k8s.kind.cluster=$CLUSTER")"
{
	echo "# Idle RAM, S0.2 kind cluster"
	echo "host:       $(hostname -s)"
	echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "kernel:     $(uname -r)"
	echo "docker:     $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
	echo "kind:       $(kind version 2>/dev/null)"
	echo "node image: $image_pin"
	echo "settled:    ${IDLE_SETTLE_SECONDS}s after all nodes Ready, no workloads deployed"
	echo
	echo "## kubectl get nodes -L role"
	k get nodes -L role
	echo
	echo "## free -h"
	free -h
	echo
	echo "## docker stats --no-stream (cluster containers)"
	# shellcheck disable=SC2086 # ids is a whitespace-separated list
	docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' $ids
} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE" # kubectl/docker pad empty columns with spaces
cat "$EVIDENCE"
for section in '## kubectl get nodes -L role' '## free -h' '## docker stats'; do
	check "evidence: $EVIDENCE has '$section'" grep -Fq "$section" "$EVIDENCE"
done
rows="$(sed -n '/^## docker stats/,$p' "$EVIDENCE" | grep -c "^$CLUSTER-")"
[ "$rows" -eq 5 ] && pass "evidence: docker stats lists 5 cluster containers" || fail "evidence: docker stats lists 5 cluster containers" "got $rows"
check "evidence: free -h has a Mem: row" grep -Eq '^Mem:' "$EVIDENCE"
if grep -Eq '[[:space:]]$' "$EVIDENCE"; then
	fail "evidence: no trailing whitespace" "lines: $(grep -nE '[[:space:]]$' "$EVIDENCE" | cut -d: -f1 | tr '\n' ' ')"
else
	pass "evidence: no trailing whitespace"
fi

# --- 10. Previous goal still passes ----------------------------------------------
if BASE="$BASE" scripts/verify-skeleton.sh >"$tmp/s01.out" 2>&1; then
	pass "S0.1: scripts/verify-skeleton.sh still passes"
else
	fail "S0.1: scripts/verify-skeleton.sh still passes" "$(grep '^FAIL' "$tmp/s01.out" | tr '\n' '|')"
fi

finish
