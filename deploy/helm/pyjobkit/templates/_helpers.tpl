{{/*
Common labels and helpers for the Pyjobkit chart.
*/}}

{{- define "pyjobkit.fullname" -}}
{{- printf "%s-%s" .Release.Name "pyjobkit" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pyjobkit.labels" -}}
app.kubernetes.io/name: pyjobkit
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "pyjobkit.selectorLabels" -}}
app.kubernetes.io/name: pyjobkit
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "pyjobkit.dsnSecret" -}}
{{- if .Values.existingSecret -}}
{{ .Values.existingSecret }}
{{- else -}}
{{ include "pyjobkit.fullname" . }}-dsn
{{- end -}}
{{- end -}}

{{- define "pyjobkit.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{ .Values.image.repository }}:{{ $tag }}
{{- end -}}
