#!/usr/bin/env bash
set -euo pipefail
cluster="$1"
[[ "$cluster" =~ ^reliefgrid-offline(-[a-z0-9][a-z0-9-]{0,24})?$ ]]
export KUBECONFIG=/kubeconfig KUBE_CONTEXT="kind-$cluster" CLUSTER="$cluster" NAMESPACE=reliefgrid
export GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GIT_OPTIONAL_LOCKS=0
export GIT_CONFIG_GLOBAL=/tmp/offline-gitconfig
git config --global --add safe.directory /kit/source
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
iptables -N RELIEFGRID_OFFLINE
for net in 127.0.0.0/8 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16; do iptables -A RELIEFGRID_OFFLINE -d "$net" -j RETURN; done
iptables -A RELIEFGRID_OFFLINE -j REJECT
iptables -I OUTPUT 1 -j RELIEFGRID_OFFLINE
ip6tables -N RELIEFGRID_OFFLINE
for net in ::1/128 fc00::/7 fe80::/10; do ip6tables -A RELIEFGRID_OFFLINE -d "$net" -j RETURN; done
ip6tables -A RELIEFGRID_OFFLINE -j REJECT
ip6tables -I OUTPUT 1 -j RELIEFGRID_OFFLINE
if curl --noproxy '*' -ks --connect-timeout 2 --max-time 4 https://1.1.1.1/ -o /dev/null; then echo 'external access unexpectedly succeeded' >&2; exit 1; fi
echo 'PASS: runner external HTTPS blocked' | tee /evidence/network.txt
kubectl --context "$KUBE_CONTEXT" get nodes >> /evidence/network.txt
export DEMO_OUTBOX_DRAIN_BINARY=/kit/bin/drain-outbox EVIDENCE_ROOT=/evidence
export STORAGE_IMAGE="$(python3 -c 'import json;print(json.load(open("/kit/manifest.json"))["roles"]["storagenode"])')"
export COORDINATOR_IMAGE="$(python3 -c 'import json;print(json.load(open("/kit/manifest.json"))["roles"]["coordinator"])')"
export TOOLS_IMAGE="$(python3 -c 'import json;print(json.load(open("/kit/manifest.json"))["roles"]["tools"])')"
cd /kit/source
source scripts/demos/common.sh
trap restore_demo_cordons EXIT
for n in {1..9}; do
 echo "Running offline demo-$n"
 make CLUSTER="$cluster" NAMESPACE=reliefgrid TOOLS_IMAGE="$TOOLS_IMAGE" "demo-$n" > "/evidence/demo-$n.log" 2>&1
 echo "PASS offline demo-$n"
done
printf 'OFFLINE ACCEPTANCE PASS:9/9 demos with runner/node external egress denied\n' | tee /evidence/result.txt
