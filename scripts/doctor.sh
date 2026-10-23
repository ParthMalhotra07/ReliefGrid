#!/usr/bin/env bash
# Compares installed tool versions with the pins in tools.mk, then checks that
# Docker is up, enough RAM is available and the cluster's host ports are free.
# Run through `make doctor`, which passes the pins and settings as environment variables.
set -uo pipefail

: "${GO_VERSION:?run via make doctor}" "${GOLANGCI_LINT_VERSION:?}" "${KIND_VERSION:?}"
: "${KUBECTL_VERSION:?}" "${HELM_VERSION:?}" "${OAPI_CODEGEN_VERSION:?}"
: "${KIND_CONFIG:?}" "${MEMINFO:?}" "${PROFILE:?}" "${MIN_AVAILABLE_RAM_GIB:?}"

bad=0
semver() { grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1; }

report() { # report <tool> <want> <got>
	local want="${2#v}" got="${3#v}"
	if [ -z "$got" ]; then
		printf 'MISSING   %-14s want %s\n' "$1" "$want"
		bad=1
	elif [ "$got" != "$want" ]; then
		printf 'MISMATCH  %-14s want %s, got %s\n' "$1" "$want" "$got"
		bad=1
	else
		printf 'ok        %-14s %s\n' "$1" "$got"
	fi
}

tool() { # tool <name> <want> <version args...>
	local name="$1" want="$2"
	shift 2
	local got=""
	if command -v "$name" >/dev/null 2>&1; then
		got="$("$name" "$@" 2>/dev/null | semver)"
	fi
	report "$name" "$want" "$got"
}

tool go "$GO_VERSION" version
tool golangci-lint "$GOLANGCI_LINT_VERSION" version
tool kind "$KIND_VERSION" version
tool kubectl "$KUBECTL_VERSION" version --client
tool helm "$HELM_VERSION" version --short

# oapi-codegen runs as `go tool`, so its version is whatever go.mod requires.
oapi="$(grep -E '^[[:space:]]*github\.com/oapi-codegen/oapi-codegen/v2 ' go.mod 2>/dev/null | semver)"
report "oapi-codegen" "$OAPI_CODEGEN_VERSION" "$oapi"

versions_bad="$bad"

# Docker: the daemon must answer, not just the CLI exist.
if ! command -v docker >/dev/null 2>&1; then
	printf 'MISSING   %-14s\n' docker
	bad=1
elif dv="$(docker version --format '{{.Server.Version}}' 2>/dev/null)" && [ -n "$dv" ]; then
	printf 'ok        %-14s daemon %s\n' docker "$dv"
else
	printf 'FAIL      %-14s daemon unreachable (is Docker running?)\n' docker
	bad=1
fi

# RAM: MemAvailable (free + reclaimable cache), not MemFree.
avail_kb="$(awk '/^MemAvailable:/ {print $2}' "$MEMINFO" 2>/dev/null)"
need_kb=$((MIN_AVAILABLE_RAM_GIB * 1048576))
if [ -z "$avail_kb" ]; then
	printf 'FAIL      %-14s cannot read MemAvailable from %s\n' RAM "$MEMINFO"
	bad=1
else
	have="$(awk -v k="$avail_kb" 'BEGIN {printf "%.1f", k / 1048576}')"
	if [ "$avail_kb" -lt "$need_kb" ]; then
		printf 'LOW       %-14s %s GiB available, need %s GiB (PROFILE=%s)\n' RAM "$have" "$MIN_AVAILABLE_RAM_GIB" "$PROFILE"
		bad=1
	else
		printf 'ok        %-14s %s GiB available, need %s GiB (PROFILE=%s)\n' RAM "$have" "$MIN_AVAILABLE_RAM_GIB" "$PROFILE"
	fi
fi

# Host ports from the kind config. A port the cluster itself publishes is fine.
cluster="$(sed -n 's/^name:[[:space:]]*//p' "$KIND_CONFIG" 2>/dev/null | head -1)"
ports="$(sed -n 's/^[[:space:]]*\(apiServerPort\|hostPort\):[[:space:]]*\([0-9][0-9]*\).*/\2/p' "$KIND_CONFIG" 2>/dev/null)"
if [ -z "$ports" ]; then
	printf 'FAIL      %-14s no apiServerPort/hostPort found in %s\n' ports "$KIND_CONFIG"
	bad=1
fi
owned=""
if [ -n "$cluster" ] && command -v docker >/dev/null 2>&1; then
	owned="$(docker ps --filter "label=io.x-k8s.kind.cluster=$cluster" --format '{{.Ports}}' 2>/dev/null)"
fi
for p in $ports; do
	if ! (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
		printf 'ok        %-14s free\n' "port $p"
	elif grep -Eq "(^|[ ,])127\.0\.0\.1:$p->" <<<"$owned"; then
		printf 'ok        %-14s held by kind cluster %s\n' "port $p" "$cluster"
	else
		printf 'BUSY      %-14s in use by another process (see: ss -ltnp "sport = :%s")\n' "port $p" "$p"
		bad=1
	fi
done

if [ "$bad" -ne 0 ]; then
	[ "$versions_bad" -ne 0 ] && echo "doctor: install the pinned versions listed in CONTRIBUTING.md"
	echo "doctor: fix the lines above that are not ok"
	exit 1
fi
echo "doctor: all tools match tools.mk; Docker, RAM and ports ok"
