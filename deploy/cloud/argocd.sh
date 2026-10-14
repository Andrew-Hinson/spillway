#!/usr/bin/env bash
# Installs Argo CD into the cluster KUBECONFIG points at, and the root
# Application that deploys the whole stack from this repo (deploy/cloud/apps).
# Run by `make argocd` (kind) and `make cloud-up` (EKS).
#
#   PROFILE   aws or kind
#   REVISION  the branch, tag or commit Argo CD follows (default main)
#   BUCKET, REGION, ENDPOINT
#             the cold bucket (aws: from Terraform; kind: Silo's, the default)
#   REGISTRY  where the images are (aws: GHCR; kind: loaded by `make kind-images`)
set -euo pipefail
cd "$(dirname "$0")/../.."

: "${PROFILE:?aws or kind}"
REVISION=${REVISION:-main}
REPO=https://github.com/Andrew-Hinson/spillway.git
k=bin/kubectl
helm=bin/helm

case $PROFILE in
  aws)
    : "${BUCKET:?the cold bucket, from terraform output}"
    : "${REGION:?the bucket region}"
    ENDPOINT=${ENDPOINT:-}
    REGISTRY=${REGISTRY:-ghcr.io/andrew-hinson/spillway}
    ;;
  kind)
    # Silo, as `make storage` sets it up. The operator keeps its manifest's
    # cold-storage args, so the bucket isn't passed.
    BUCKET='' REGION=us-east-1 ENDPOINT=''
    REGISTRY=${REGISTRY:-spillway}
    # The credentials Silo and the aggregators share: made once, here,
    # because they're generated, not kept in the repo.
    for ns in storage vector; do
      $k create namespace $ns --dry-run=client -o yaml | $k apply -f - >/dev/null
    done
    if ! $k -n storage get secret cold-storage >/dev/null 2>&1; then
      $k -n storage create secret generic cold-storage \
        --from-literal=AWS_ACCESS_KEY_ID="spillway-$(openssl rand -hex 4)" \
        --from-literal=AWS_SECRET_ACCESS_KEY="$(openssl rand -hex 20)"
    fi
    $k -n storage get secret cold-storage -o json | python3 -c 'import json,sys; s=json.load(sys.stdin); \
      print(json.dumps({"apiVersion":"v1","kind":"Secret","metadata":{"name":"cold-storage","namespace":"vector"},"data":s["data"]}))' |
      $k apply -f -
    ;;
  *) echo "PROFILE must be aws or kind" >&2; exit 2 ;;
esac

chart_version=$(sed -n 's/^ARGOCD_CHART_VERSION *:= *//p' Makefile)
$helm upgrade --install argocd argo-cd --repo https://argoproj.github.io/argo-helm \
  --version "$chart_version" --namespace argocd --create-namespace \
  -f deploy/cloud/argocd/values.yaml --wait --timeout 10m

$k apply -f - <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: spillway
  namespace: argocd
  # Deleting this deletes every Application under it, and their resources.
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  destination:
    server: https://kubernetes.default.svc
    namespace: argocd
  source:
    repoURL: $REPO
    targetRevision: $REVISION
    path: deploy/cloud/apps
    helm:
      valueFiles: [values.yaml, generated.yaml]
      parameters:
        - {name: profile, value: "$PROFILE"}
        - {name: revision, value: "$REVISION"}
        - {name: images.registry, value: "$REGISTRY"}
        - {name: cold.bucket, value: "$BUCKET"}
        - {name: cold.region, value: "$REGION"}
        - {name: cold.endpoint, value: "$ENDPOINT"}
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
EOF

echo "Argo CD is deploying $REPO@$REVISION ($PROFILE). Watch it with:"
echo "  $k -n argocd get applications"
