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

{{/* By digest when image.digest is set (pinned deployments); by tag on kind. With
requireDigests (cloud profiles, S5.1) a missing digest fails the render: no deploy by tag. */}}
{{- define "storage.image" -}}
{{- if and .Values.requireDigests (not .Values.image.digest) -}}
{{- fail "requireDigests: set image.digest (sha256:...) for the storage node image" -}}
{{- end -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}
{{- end }}

{{- define "storage.coordinatorImage" -}}
{{- if and .Values.requireDigests (not .Values.coordinator.image.digest) -}}
{{- fail "requireDigests: set coordinator.image.digest (sha256:...) for the coordinator image" -}}
{{- end -}}
{{- with .Values.coordinator.image -}}
{{- if .digest -}}
{{ .repository }}@{{ .digest }}
{{- else -}}
{{ .repository }}:{{ .tag }}
{{- end -}}
{{- end -}}
{{- end }}

{{/* The coordinator's in-cluster URL (COORDINATOR_URL on storage pods). */}}
{{- define "storage.coordinatorURL" -}}
http://storage-coordinator.{{ .Release.Namespace }}.svc.cluster.local:8080
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

{{/*
OTEL_* environment for one service when tracing is on (S4.4); nothing when
it is off. Call with (dict "root" $ "service" "coordinator").
*/}}
{{- define "storage.tracingEnv" -}}
{{- $t := .root.Values.tracing -}}
{{- if $t.enabled }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $t.endpoint | quote }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: http/protobuf
- name: OTEL_SERVICE_NAME
  value: {{ .service | quote }}
- name: OTEL_TRACES_SAMPLER
  value: {{ $t.sampler | quote }}
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ $t.samplerArg | toString | quote }}
{{- end }}
{{- end }}
