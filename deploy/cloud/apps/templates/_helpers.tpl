{{/*
The parts every Application shares: Argo CD's namespace, the sync wave, a
finalizer so deleting the root Application deletes everything it made (what
`make cloud-down` relies on), and automated sync with pruning and self-heal.
Server-side apply throughout: the CRDs are too big for client-side apply's
annotation, and Argo CD then owns only the fields it sets, which leaves the
operator's own (the aggregator's partition, its config-suffix annotation and
the config revisions in its ConfigMap) alone.
*/}}
{{- define "spillway.meta" -}}
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: {{ .name }}
  namespace: argocd
  annotations:
    argocd.argoproj.io/sync-wave: {{ .wave | quote }}
  finalizers:
    - resources-finalizer.argocd.argoproj.io
{{- end }}

{{- define "spillway.sync" -}}
project: default
destination:
  server: https://kubernetes.default.svc
  namespace: {{ .namespace }}
syncPolicy:
  automated:
    prune: true
    selfHeal: true
  syncOptions:
    - CreateNamespace=true
    - ServerSideApply=true
  retry:
    limit: 10
    backoff:
      duration: 10s
      factor: 2
      maxDuration: 3m
{{- end }}

{{/* The repo itself, as a source of plain manifests or of Helm values ($values). */}}
{{- define "spillway.repo" -}}
repoURL: {{ .Values.repoURL }}
targetRevision: {{ .Values.revision }}
{{- end }}

{{/* An image built from this repo, with its tag from generated.yaml. */}}
{{- define "spillway.image" -}}
spillway/{{ .name }}={{ .root.Values.images.registry }}/{{ .name }}:{{ index .root.Values.images.tags .name }}
{{- end }}
