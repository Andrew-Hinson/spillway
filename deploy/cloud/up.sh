#!/usr/bin/env bash
# `make cloud-up`: creates the EKS environment with Terraform, then deploys
# the stack with Argo CD from this repo's main branch. Terraform shows its plan
# and asks before creating anything; from then on the environment is billed
# by the hour until `make cloud-down`.
#
# Needs the AWS CLI with credentials (the default profile, or AWS_PROFILE),
# and the images of main's commit published to GHCR (.github/workflows/images.yml).
set -euo pipefail
cd "$(dirname "$0")/../.."

tf() { bin/terraform -chdir=deploy/cloud/terraform "$@"; }
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

aws sts get-caller-identity --query Account --output text >/dev/null

tf init -input=false
tf apply -input=false   # prompts for "yes"

region=$(tf output -raw region)
cluster=$(tf output -raw cluster_name)
bucket=$(tf output -raw cold_bucket)

export KUBECONFIG=$PWD/.kubeconfig-cloud
aws eks update-kubeconfig --region "$region" --name "$cluster" --kubeconfig "$KUBECONFIG"
bin/kubectl wait --for=condition=Ready nodes --all --timeout=10m

PROFILE=aws REVISION=${REVISION:-main} BUCKET=$bucket REGION=$region deploy/cloud/argocd.sh

log "waiting for every Application to be Synced and Healthy (about 10 minutes)"
for _ in $(seq 120); do
  apps=$(bin/kubectl -n argocd get applications -o jsonpath='{range .items[*]}{.metadata.name} {.status.sync.status} {.status.health.status}{"\n"}{end}')
  if [ "$(echo "$apps" | wc -l)" -gt 1 ] && ! echo "$apps" | grep -qv ' Synced Healthy$'; then
    echo "$apps"
    log "up. export KUBECONFIG=$KUBECONFIG"
    exit 0
  fi
  sleep 10
done
echo "$apps"
log "not every Application is healthy after 20 minutes; the environment is still running (and billed): make cloud-down"
exit 1
