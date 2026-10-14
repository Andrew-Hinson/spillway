#!/usr/bin/env bash
# `make cloud-down`: deletes everything `make cloud-up` made, then checks that
# nothing billable is left.
#
# The order matters. Persistent volumes are EBS volumes the CSI driver
# created, not Terraform: deleting the cluster first would leave them behind,
# billed. So the stack goes first (the root Application's finalizer deletes
# every Application and their resources), then every claim, which the gp3
# class's reclaim policy turns into deleted volumes, and only then the
# cluster, the network and the bucket.
set -euo pipefail
cd "$(dirname "$0")/../.."

tf() { bin/terraform -chdir=deploy/cloud/terraform "$@"; }
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
export KUBECONFIG=$PWD/.kubeconfig-cloud
k=bin/kubectl

region=$(tf output -raw region 2>/dev/null || echo us-east-1)
cluster=$(tf output -raw cluster_name 2>/dev/null || echo spillway)

if [ -f "$KUBECONFIG" ] && $k get nodes >/dev/null 2>&1; then
  log "deleting the stack"
  $k -n argocd delete application spillway --ignore-not-found --timeout=15m
  # Claims from StatefulSets (Kafka keeps its own: deleteClaim is false)
  # outlive their workloads.
  log "deleting persistent volume claims"
  $k delete pvc --all --all-namespaces --wait=false
  for _ in $(seq 60); do
    [ -z "$($k get pv -o name)" ] && break
    sleep 10
  done
  if [ -n "$($k get pv -o name)" ]; then
    $k get pv
    log "volumes are still being deleted; not destroying the cluster until they are (rerun make cloud-down)"
    exit 1
  fi
else
  log "no reachable cluster; going straight to terraform destroy"
fi

log "destroying the cluster, network and bucket"
tf destroy -input=false -auto-approve

# What the CSI driver made carries the spillway tag (extraVolumeTags), as does
# everything Terraform made.
left=$(aws ec2 describe-volumes --region "$region" --filters "Name=tag:spillway,Values=$cluster" \
  --query 'Volumes[].VolumeId' --output text)
tagged=$(aws resourcegroupstaggingapi get-resources --region "$region" \
  --tag-filters "Key=spillway,Values=$cluster" --query 'ResourceTagMappingList[].ResourceARN' --output text)
if [ -n "$left$tagged" ]; then
  log "still tagged spillway=$cluster (the tagging API can lag a few minutes behind deletions):"
  printf '%s\n' $left $tagged
  exit 1
fi
rm -f "$KUBECONFIG"
log "down: nothing tagged spillway=$cluster is left in $region"
