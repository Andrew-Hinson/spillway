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

# Before anything is billed: every image the cluster will run must be
# published, and pullable without credentials (GHCR makes a new package
# private until its visibility is changed in GitHub's settings).
registry=ghcr.io/andrew-hinson/spillway
missing=0
anon=$(mktemp -d)   # an empty Docker config: check as an anonymous client would
while IFS=: read -r name _ tag _; do
  if ! DOCKER_CONFIG=$anon docker manifest inspect "$registry/$name:$tag" >/dev/null 2>&1; then
    echo "not pullable: $registry/$name:$tag" >&2
    missing=1
  fi
done < <(make -s image-list)
rmdir "$anon"
if [ "$missing" = 1 ]; then
  echo "Publish them (the Images workflow runs on every push to main) and make the packages public first." >&2
  exit 1
fi

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
