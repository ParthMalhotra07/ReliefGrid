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

{{- define "storage.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
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
