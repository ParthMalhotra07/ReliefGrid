#!/usr/bin/env bash
# Verify check for S0.1 (repo skeleton). Run from the repo root:
#   scripts/verify-skeleton.sh                # commit range origin/main..HEAD
#   BASE=<rev> EXPECT_AUTHOR_EMAIL=<email> scripts/verify-skeleton.sh
# Exits non-zero if any check fails; prints one PASS/FAIL line per check.
set -uo pipefail

BASE="${BASE:-origin/main}"
EXPECT_AUTHOR_EMAIL="${EXPECT_AUTHOR_EMAIL:-}"
S_OWNER="@coDEmaVEriCK06"
A_OWNER="@ParthMalhotra07"

failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; [ -n "${2:-}" ] && printf '      %s\n' "$2"; failures=$((failures + 1)); }
check() { # check <description> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# --- 1. Layout (v3.1 §6 plus spike/) ----------------------------------------
dirs=(
	contracts contracts/events
	cmd/api cmd/matcher cmd/coordinator cmd/storagenode cmd/invcheck
	internal/ring internal/raftstore internal/fsm internal/workflow internal/geo
	internal/mq internal/outbox internal/reconcile internal/obs internal/resilience internal/auth
	deploy/helm/reliefgrid deploy/kind deploy/platform
	chaos loadtest tests ui infra/gcp
	docs/architecture docs/adr docs/demos docs/evidence docs/operations
	spike/raft spike/mq
)
for d in "${dirs[@]}"; do
	# A directory only survives a clone if it holds a file git will carry
	# (tracked, or untracked and not ignored/excluded).
	if [ -n "$(git ls-files -co --exclude-standard -- "$d/" 2>/dev/null | head -1)" ]; then
		pass "layout: $d/ exists with a file"
	else
		fail "layout: $d/ exists with a file"
	fi
done

# --- 2. go.mod ----------------------------------------------------------------
check "go.mod: module github.com/ParthMalhotra07/ReliefGrid" \
	grep -Eq '^module github\.com/ParthMalhotra07/ReliefGrid$' go.mod
check "go.mod: go 1.27 directive" grep -Eq '^go 1\.27(\.[0-9]+)?$' go.mod
check "go.mod: toolchain go1.27.N line" grep -Eq '^toolchain go1\.27\.[0-9]+$' go.mod
check "go.mod: oapi-codegen pinned as a tool" \
	grep -Eq '^tool github\.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen$' go.mod

# --- 3. Version pins: one place (tools.mk), everything else agrees ----------
pin() { sed -n "s/^$1[[:space:]]*:\{0,1\}=[[:space:]]*//p" tools.mk 2>/dev/null | head -1; }
go_pin="$(pin GO_VERSION)"
lint_pin="$(pin GOLANGCI_LINT_VERSION)"
check "Makefile: includes tools.mk" grep -Eq '^include tools\.mk$' Makefile
for v in GO_VERSION GOLANGCI_LINT_VERSION KIND_VERSION KUBECTL_VERSION HELM_VERSION OAPI_CODEGEN_VERSION; do
	if [ -n "$(pin "$v")" ]; then pass "tools.mk: pins $v"; else fail "tools.mk: pins $v"; fi
done
if [ -n "$go_pin" ] && grep -Eq "^toolchain go${go_pin}\$" go.mod 2>/dev/null; then
	pass "pins: go.mod toolchain matches GO_VERSION"
else
	fail "pins: go.mod toolchain matches GO_VERSION" "GO_VERSION='$go_pin'"
fi
oapi_pin="$(pin OAPI_CODEGEN_VERSION)"
if [ -n "$oapi_pin" ] && grep -Eq "github\.com/oapi-codegen/oapi-codegen/v2 ${oapi_pin}\$" go.mod 2>/dev/null; then
	pass "pins: go.mod oapi-codegen matches OAPI_CODEGEN_VERSION"
else
	fail "pins: go.mod oapi-codegen matches OAPI_CODEGEN_VERSION" "OAPI_CODEGEN_VERSION='$oapi_pin'"
fi
check "pins: CI reads GOLANGCI_LINT_VERSION from tools.mk" \
	grep -Fq 'print-GOLANGCI_LINT_VERSION' .github/workflows/ci.yml
if [ -n "$lint_pin" ] && [ "$(make -s print-GOLANGCI_LINT_VERSION 2>/dev/null)" = "$lint_pin" ]; then
	pass "pins: make print-GOLANGCI_LINT_VERSION prints the pin"
else
	fail "pins: make print-GOLANGCI_LINT_VERSION prints the pin"
fi
if [ -f .github/workflows/ci.yml ] && ! grep -Eq 'version: *v?[0-9]+\.[0-9]+' .github/workflows/ci.yml; then
	pass "pins: CI has no hard-coded tool version"
else
	fail "pins: CI has no hard-coded tool version"
fi

# --- 4. Makefile targets ------------------------------------------------------
for t in check test dev doctor gen; do
	check "make: target '$t' exists" make -n "$t"
done
check "make gen: passes (no-op until contracts land)" make gen
if out="$(make dev 2>&1)"; then
	fail "make dev: exits non-zero as a stub" "it exited 0"
elif grep -q 'not yet: S0.2' <<<"$out"; then
	pass "make dev: exits non-zero with 'not yet: S0.2'"
else
	fail "make dev: exits non-zero with 'not yet: S0.2'" "output: $out"
fi

# make doctor against stub tools: matching pins pass, one mismatch fails.
stub() { # stub <dir> <name> <output>
	printf '#!/bin/sh\nprintf "%%s\\n" "%s"\n' "$3" >"$1/$2"
	chmod +x "$1/$2"
}
write_stubs() { # write_stubs <dir> <kind version>
	mkdir -p "$1"
	stub "$1" go "go version go${go_pin} linux/amd64"
	stub "$1" golangci-lint "golangci-lint has version ${lint_pin#v} built with go${go_pin} from 0000000 on 2026-10-16T11:33:37Z"
	stub "$1" kind "kind $2 go${go_pin} linux/amd64"
	stub "$1" kubectl "Client Version: $(pin KUBECTL_VERSION)"
	stub "$1" helm "$(pin HELM_VERSION)+g0000000"
}
write_stubs "$tmp/good" "$(pin KIND_VERSION)"
write_stubs "$tmp/bad" "v0.0.1"
if PATH="$tmp/good:$PATH" make doctor >"$tmp/doctor-good.out" 2>&1; then
	pass "make doctor: passes when every tool matches its pin"
else
	fail "make doctor: passes when every tool matches its pin" "$(tail -3 "$tmp/doctor-good.out" | tr '\n' ' ')"
fi
if PATH="$tmp/bad:$PATH" make doctor >"$tmp/doctor-bad.out" 2>&1; then
	fail "make doctor: fails and names kind when kind is off-pin" "it exited 0"
elif grep -q kind "$tmp/doctor-bad.out"; then
	pass "make doctor: fails and names kind when kind is off-pin"
else
	fail "make doctor: fails and names kind when kind is off-pin" "output does not mention kind"
fi

# --- 5. Repo hygiene files ----------------------------------------------------
check ".gitignore: ignores kubeconfigs" grep -Eiq 'kubeconfig' .gitignore
check ".gitignore: ignores .env files" grep -Eq '^/?\.env' .gitignore
check ".golangci.yml: v2 config" grep -Eq '^version: *"?2"?' .golangci.yml
check "CONTRIBUTING.md: describes spike/ as throwaway" \
	sh -c 'grep -q "spike/" CONTRIBUTING.md && grep -qi "throwaway" CONTRIBUTING.md'
for h in "Behaviour" "Failure case handled" "How tested" "Limitations" "Goal ID"; do
	check "PR template: has '$h' heading" grep -Eq "^#+ *$h" .github/pull_request_template.md
done

# --- 6. CI workflow -----------------------------------------------------------
ci=.github/workflows/ci.yml
check "CI: runs on pull_request" grep -Eq '^ *pull_request:' "$ci"
check "CI: job 'check'" grep -Eq '^  check:' "$ci"
check "CI: job 'commit-lint'" grep -Eq '^  commit-lint:' "$ci"
check "CI: runs make check" grep -Eq 'run: *make check' "$ci"
check_recipe="$(make -n check 2>/dev/null)"
for step in "gofmt" "go vet" "golangci-lint run" "go test -race"; do
	if grep -Fq "$step" <<<"$check_recipe"; then pass "make check: runs $step"; else fail "make check: runs $step"; fi
done

# --- 7. CODEOWNERS (last matching pattern wins) -------------------------------
owners_of() { # owners_of <path> -> sorted owner list, empty if unowned
	local path="/$1" result="" pattern rest
	while read -r pattern rest; do
		case "$pattern" in "" | \#*) continue ;; esac
		if [ "$pattern" = "*" ] || [ "$pattern" = "$path" ] ||
			{ [ "${pattern%/}" != "$pattern" ] && [ "${path#"$pattern"}" != "$path" ]; }; then
			result="$rest"
		fi
	done <.github/CODEOWNERS
	tr ' ' '\n' <<<"$result" | grep . | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//'
}
S="$S_OWNER"
A="$A_OWNER"
BOTH="$(printf '%s\n%s\n' "$S_OWNER" "$A_OWNER" | LC_ALL=C sort | tr '\n' ' ' | sed 's/ $//')"
expect=(
	"internal/fsm/apply.go|$S" "internal/raftstore/store.go|$S" "internal/ring/ring.go|$S"
	"cmd/storagenode/main.go|$S" "cmd/coordinator/main.go|$S" "cmd/invcheck/main.go|$S"
	"chaos/scripts/kill-leader.sh|$S" "loadtest/k6.js|$S" "infra/gcp/main.tf|$S"
	".github/workflows/ci.yml|$S" ".github/CODEOWNERS|$S"
	"deploy/kind/cluster.yaml|$S" "spike/raft/main.go|$S" "internal/obs/trace.go|$S"
	"deploy/helm/reliefgrid/templates/storage/statefulset.yaml|$S"
	"cmd/api/main.go|$A" "cmd/matcher/main.go|$A" "internal/workflow/match.go|$A"
	"internal/geo/geohash.go|$A" "internal/mq/topology.go|$A" "internal/outbox/relay.go|$A"
	"internal/reconcile/loop.go|$A" "ui/index.html|$A"
	"deploy/helm/reliefgrid/Chart.yaml|$A" "deploy/helm/reliefgrid/templates/api.yaml|$A"
	"spike/mq/mq.sh|$A" "tests/workflow/schedules.md|$A" "internal/auth/auth.go|$A"
	"internal/resilience/breaker.go|$BOTH" "deploy/platform/charts.yaml|$BOTH"
	"Makefile|$BOTH" "tools.mk|$BOTH" "scripts/verify-skeleton.sh|$BOTH" "tests/integration/e2e_test.go|$BOTH"
	"contracts/public.yaml|$BOTH" "docs/adr/001-raft.md|$BOTH"
	"go.mod|$BOTH" "README.md|$BOTH" "CONTRIBUTING.md|$BOTH"
)
if [ -f .github/CODEOWNERS ]; then
	for e in "${expect[@]}"; do
		p="${e%%|*}"
		want="${e#*|}"
		got="$(owners_of "$p")"
		if [ "$got" = "$want" ]; then pass "CODEOWNERS: $p -> $want"; else fail "CODEOWNERS: $p -> $want" "got '$got'"; fi
	done
	unowned=0
	while read -r f; do
		[ -n "$(owners_of "$f")" ] || { fail "CODEOWNERS: $f has an owner"; unowned=1; }
	done < <(git ls-files -co --exclude-standard)
	[ "$unowned" -eq 0 ] && pass "CODEOWNERS: every repo file has an owner"
else
	fail "CODEOWNERS: .github/CODEOWNERS exists"
fi

# --- 8. commit-lint: rejects AI attribution, accepts clean commits ------------
lint="$PWD/scripts/commit-lint.sh"
if [ -x "$lint" ]; then
	r="$tmp/lintrepo"
	git init -q "$r"
	g() { git -C "$r" -c user.name=t -c user.email=t@example.com "$@"; }
	g commit -q --allow-empty -m "chore: base"
	base="$(g rev-parse HEAD)"
	g commit -q --allow-empty -m "feat(ring): clean commit [S1.1]"
	if "$lint" "$base" "$(g rev-parse HEAD)" >/dev/null 2>&1; then
		pass "commit-lint: accepts a clean commit"
	else
		fail "commit-lint: accepts a clean commit"
	fi
	for body in "Co-authored-by: Bot <bot@example.com>" "co-authored-by: bot <b@example.com>" \
		"Generated with some assistant" "🤖 Generated with [Tool](https://example.com)"; do
		g commit -q --allow-empty -m "feat(ring): bad commit [S1.1]" -m "$body"
		if "$lint" "$base" "$(g rev-parse HEAD)" >/dev/null 2>&1; then
			fail "commit-lint: rejects '$body'" "it exited 0"
		else
			pass "commit-lint: rejects '$body'"
		fi
		g reset -q --hard "$base"
		g commit -q --allow-empty -m "feat(ring): clean commit [S1.1]"
	done
	check "commit-lint: this branch ($BASE..HEAD) is clean" "$lint" "$BASE" HEAD
else
	fail "commit-lint: scripts/commit-lint.sh exists and is executable"
fi

# --- 9. Authorship on this branch --------------------------------------------
if [ -n "$EXPECT_AUTHOR_EMAIL" ]; then
	bad="$(git log --format='%ae %ce' "$BASE..HEAD" | tr ' ' '\n' | grep -vxF "$EXPECT_AUTHOR_EMAIL" | sort -u)"
	if [ -z "$bad" ]; then
		pass "authorship: every commit in $BASE..HEAD is by $EXPECT_AUTHOR_EMAIL"
	else
		fail "authorship: every commit in $BASE..HEAD is by $EXPECT_AUTHOR_EMAIL" "others: $bad"
	fi
fi

echo
if [ "$failures" -eq 0 ]; then
	echo "verify-skeleton: all checks passed"
else
	echo "verify-skeleton: $failures check(s) failed"
	exit 1
fi
