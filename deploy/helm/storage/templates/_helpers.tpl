{{/*
Names are fixed, not release-prefixed: every replica's stable Raft address
(storage-shard-N-i.storage-shard-N.<namespace>.svc.cluster.local:7000) is
stored in its group's Raft configuration and must never change.
*/}}

{{- define "storage.ring" -}}
{{- $ring := .Files.Get "files/ring.yaml" | fromYaml -}}
{{- if not $ring.shards -}}
{{- fail "files/ring.yaml (a link to contracts/ring.yaml) is missing or has no shards" -}}
{{- end -}}
{{- toJson $ring -}}
{{- end }}

{{- define "storage.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: storage
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Pods of one shard. Call with (dict "shard" N). */}}
{{- define "storage.shardSelector" -}}
app.kubernetes.io/name: storage
app.kubernetes.io/component: storagenode
reliefgrid.io/shard: {{ .shard | quote }}
{{- end }}

{{/* Manifest ConfigMap names, in shard order. */}}
{{- define "storage.manifests" -}}
{{- $ring := include "storage.ring" . | fromJson -}}
{{- $names := list -}}
{{- range $s := until (int $ring.shards) -}}
{{- $names = append $names (printf "storage-shard-%d-manifest" $s) -}}
{{- end -}}
{{- join "," $names -}}
{{- end }}

{{/*
Manifest specs for the bootstrap Job (MANIFESTS), in shard order: what the
chart knows about each group. storagectl creates a missing manifest from its
spec and never resets an existing one; the chart renders no manifest itself.
*/}}
{{- define "storage.manifestSpecs" -}}
{{- $ring := include "storage.ring" . | fromJson -}}
{{- $specs := list -}}
{{- range $s := until (int $ring.shards) -}}
{{- $peers := list -}}
{{- range $i := until 3 -}}
{{- $pod := printf "storage-shard-%d-%d" $s $i -}}
{{- $peers = append $peers (dict "podName" $pod "address" (printf "%s.storage-shard-%d.%s.svc.cluster.local:7000" $pod $s $.Release.Namespace)) -}}
{{- end -}}
{{- $specs = append $specs (dict "name" (printf "storage-shard-%d-manifest" $s) "groupId" (printf "shard-%d" $s) "shard" $s "ringVersion" ($ring.ringVersion | int64) "peers" $peers) -}}
{{- end -}}
{{- toJson $specs -}}
{{- end }}

{{/* By digest when image.digest is set (pinned deployments); by tag on kind. */}}
{{- define "storage.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}
{{- end }}

{{- define "storage.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
fsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "storage.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end }}
