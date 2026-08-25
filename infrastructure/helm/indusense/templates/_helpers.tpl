{{/*
Standard labels applied to every resource this chart creates.
*/}}
{{- define "indusense.labels" -}}
app.kubernetes.io/part-of: indusense
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{/*
Selector labels — the subset of indusense.labels that must never change
across an upgrade, since Deployment/StatefulSet selectors are immutable.
*/}}
{{- define "indusense.selectorLabels" -}}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{/*
Postgres DSN shared by every service that talks to Postgres directly.
*/}}
{{- define "indusense.postgresDSN" -}}
postgres://{{ .Values.postgres.user }}:{{ .Values.postgres.password }}@{{ .Release.Name }}-postgres:5432/{{ .Values.postgres.db }}?sslmode=disable
{{- end -}}
