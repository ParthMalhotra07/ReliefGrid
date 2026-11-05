#!/usr/bin/env bash
# Install CI's tools (S4.6, S5.1): kind, kubectl, helm and terraform for linux/amd64, at the
# versions pinned in tools.mk, each checked against the sha256 pinned there (never one fetched
# from the download server).
#
#   scripts/ci-tools.sh <bin dir> [tool...]
#                                         download, verify, install into <bin dir>; the tools
#                                         default to kind, kubectl and helm (the cluster tools)
#   CI_TOOLS_DRY_RUN=1 scripts/ci-tools.sh <bin dir> [tool...]
#                                         print "<tool> <url> <sha256>" per tool, download nothing
set -euo pipefail

if [ $# -lt 1 ]; then
	echo "usage: $0 <bin dir> [kind|kubectl|helm|terraform...]" >&2
	exit 2
fi
bin="$1"
shift
names=("$@")
[ ${#names[@]} -gt 0 ] || names=(kind kubectl helm)
repo="$(cd "$(dirname "$0")/.." && pwd)"
pin() { make -s -C "$repo" "print-$1"; }

tools=()
for name in "${names[@]}"; do
	case "$name" in
	kind) v="$(pin KIND_VERSION)" && tools+=("kind https://github.com/kubernetes-sigs/kind/releases/download/$v/kind-linux-amd64 $(pin KIND_SHA256)") ;;
	kubectl) v="$(pin KUBECTL_VERSION)" && tools+=("kubectl https://dl.k8s.io/release/$v/bin/linux/amd64/kubectl $(pin KUBECTL_SHA256)") ;;
	helm) v="$(pin HELM_VERSION)" && tools+=("helm https://get.helm.sh/helm-$v-linux-amd64.tar.gz $(pin HELM_SHA256)") ;;
	terraform) v="$(pin TERRAFORM_VERSION)" && tools+=("terraform https://releases.hashicorp.com/terraform/$v/terraform_${v}_linux_amd64.zip $(pin TERRAFORM_SHA256)") ;;
	*) echo "ci-tools: unknown tool $name (kind, kubectl, helm, terraform)" >&2; exit 2 ;;
	esac
done

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
	case "$name" in
	helm)
		tar -xzf "$tmp/$name.download" -C "$tmp" linux-amd64/helm
		install -m 0755 "$tmp/linux-amd64/helm" "$bin/helm"
		;;
	terraform)
		unzip -q -o "$tmp/$name.download" terraform -d "$tmp"
		install -m 0755 "$tmp/terraform" "$bin/terraform"
		;;
	*) install -m 0755 "$tmp/$name.download" "$bin/$name" ;;
	esac
done
for name in "${names[@]}"; do
	case "$name" in
	kind) "$bin/kind" version ;;
	kubectl) "$bin/kubectl" version --client ;;
	helm) "$bin/helm" version --short ;;
	terraform) "$bin/terraform" version ;;
	esac
done
