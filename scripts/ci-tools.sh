#!/usr/bin/env bash
# Install CI's cluster tools (S4.6): kind, kubectl and helm for linux/amd64, at the versions
# pinned in tools.mk, each checked against the sha256 pinned there (never one fetched from
# the download server).
#
#   scripts/ci-tools.sh <bin dir>         download, verify, install into <bin dir>
#   CI_TOOLS_DRY_RUN=1 scripts/ci-tools.sh <bin dir>
#                                         print "<tool> <url> <sha256>" per tool, download nothing
set -euo pipefail

if [ $# -ne 1 ]; then
	echo "usage: $0 <bin dir>" >&2
	exit 2
fi
bin="$1"
repo="$(cd "$(dirname "$0")/.." && pwd)"
pin() { make -s -C "$repo" "print-$1"; }

kind_v="$(pin KIND_VERSION)" kubectl_v="$(pin KUBECTL_VERSION)" helm_v="$(pin HELM_VERSION)"
tools=(
	"kind https://github.com/kubernetes-sigs/kind/releases/download/$kind_v/kind-linux-amd64 $(pin KIND_SHA256)"
	"kubectl https://dl.k8s.io/release/$kubectl_v/bin/linux/amd64/kubectl $(pin KUBECTL_SHA256)"
	"helm https://get.helm.sh/helm-$helm_v-linux-amd64.tar.gz $(pin HELM_SHA256)"
)

if [ -n "${CI_TOOLS_DRY_RUN:-}" ]; then
	printf '%s\n' "${tools[@]}"
	exit 0
fi

mkdir -p "$bin"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
for t in "${tools[@]}"; do
	read -r name url sum <<<"$t"
	curl -fsSL --retry 3 -o "$tmp/$name.download" "$url"
	echo "$sum  $tmp/$name.download" | sha256sum -c --quiet - || { echo "ci-tools: $name: sha256 mismatch for $url" >&2; exit 1; }
	if [ "$name" = helm ]; then
		tar -xzf "$tmp/$name.download" -C "$tmp" linux-amd64/helm
		install -m 0755 "$tmp/linux-amd64/helm" "$bin/helm"
	else
		install -m 0755 "$tmp/$name.download" "$bin/$name"
	fi
done
"$bin/kind" version
"$bin/kubectl" version --client
"$bin/helm" version --short
