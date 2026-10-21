#!/usr/bin/env bash
# Compares installed tool versions with the pins in tools.mk.
# Run through `make doctor`, which passes the pins as environment variables.
set -uo pipefail

: "${GO_VERSION:?run via make doctor}" "${GOLANGCI_LINT_VERSION:?}" "${KIND_VERSION:?}"
: "${KUBECTL_VERSION:?}" "${HELM_VERSION:?}" "${OAPI_CODEGEN_VERSION:?}"

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

if [ "$bad" -ne 0 ]; then
	echo "doctor: install the pinned versions listed in CONTRIBUTING.md"
	exit 1
fi
echo "doctor: all tools match tools.mk"
