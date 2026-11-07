#!/usr/bin/env bash
# measure.sh <label> <seconds>: the broker pod's CPU (millicores), CFS throttling and probe
# failures over a window, from its cgroup on the kind node. One JSON line.
set -euo pipefail
label="$1" secs="$2"
K="kubectl --context kind-reliefgrid -n reliefgrid"
pod=reliefgrid-rabbitmq-0
read -r node uid qos < <($K get pod $pod -o json | jq -r '"\(.spec.nodeName) \(.metadata.uid | gsub("-"; "_")) \(.status.qosClass | ascii_downcase)"')
if [ "$qos" = guaranteed ]; then cg=/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-pod$uid.slice
else cg=/sys/fs/cgroup/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-$qos.slice/kubelet-kubepods-$qos-pod$uid.slice; fi
stat() { docker exec "$node" sh -c "awk '/^usage_usec|^nr_periods|^nr_throttled/ {printf \"%s \", \$2}' $cg/cpu.stat; cat $cg/cpu.max"; }
unhealthy() { $K get events --field-selector involvedObject.name=$pod,reason=Unhealthy -o json | jq '[.items[].count] | add // 0'; }
read -r u1 p1 t1 max1 _ < <(stat); e1=$(unhealthy); a=$(( $(date +%s%N) / 1000000 ))
sleep "$secs"
read -r u2 p2 t2 _ _ < <(stat); e2=$(unhealthy); b=$(( $(date +%s%N) / 1000000 ))
restarts=$($K get pod $pod -o jsonpath='{.status.containerStatuses[0].restartCount}')
probes=$($K get pod $pod -o json | jq -c '.spec.containers[0] | {startup: (.startupProbe | keys - ["failureThreshold","periodSeconds","timeoutSeconds","successThreshold","initialDelaySeconds","terminationGracePeriodSeconds"]), readiness: (.readinessProbe | keys - ["failureThreshold","periodSeconds","timeoutSeconds","successThreshold"]), liveness: (.livenessProbe | keys - ["failureThreshold","periodSeconds","timeoutSeconds","successThreshold"])}')
jq -nc --arg label "$label" --argjson secs "$(( (b - a) / 1000 ))" --arg cpumax "$max1" \
  --argjson mc "$(( (u2 - u1) / (b - a) ))" --argjson periods "$((p2 - p1))" --argjson thr "$((t2 - t1))" \
  --argjson unhealthy "$((e2 - e1))" --argjson restarts "$restarts" --argjson probes "$probes" \
  '{label: $label, seconds: $secs, cpuMax: $cpumax, millicores: $mc, cfsPeriods: $periods, throttledPeriods: $thr,
    throttledPct: (if $periods > 0 then ($thr * 1000 / $periods | floor) / 10 else 0 end), probeFailures: $unhealthy, restarts: $restarts, probes: $probes}'
