{{- define "tracing.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: reliefgrid-tracing
{{- end }}

{{/* Pin to the infra worker (deploy/kind/cluster.yaml). */}}
{{- define "tracing.infra" -}}
nodeSelector:
  role: infra
tolerations:
  - key: role
    operator: Equal
    value: infra
    effect: NoSchedule
{{- end }}

{{- define "tracing.podSecurity" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "tracing.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end }}

{{- define "tracing.image" -}}
{{ .repository }}:{{ .tag }}@{{ .digest }}
{{- end }}
