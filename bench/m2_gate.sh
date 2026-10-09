#!/usr/bin/env bash
# M2 gate demo: run against a fresh `make up`. Prints a timestamped log of
# each gate criterion, for docs/results/m2-gate.md.
#
#   1. an example LogPipeline is rendered, validated, canaried and Ready, with
#      events flowing (and the time from apply to first event queryable)
#   2. a malformed spec is rejected at apply time, and one that's valid alone
#      but conflicts with another is marked Invalid and never reaches an
#      aggregator
#   3. a config change that fails at runtime is canaried and rolled back
#
# Step 3 points the operator's cold storage at an endpoint that doesn't exist
# (MinIO isn't deployed until M3), so a cold-routed pipeline fails at runtime
# while passing every offline check. The script restores the operator and
# deletes its LogPipelines at the end.
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/.kubeconfig"
k=bin/kubectl
ts() { date -u +%H:%M:%S; }
log() { echo "$(ts) $*"; }
reason() { $k get lp "$1" -o jsonpath='{.status.conditions[0].reason}' 2>/dev/null; }
message() { $k get lp "$1" -o jsonpath='{.status.conditions[0].message}' 2>/dev/null; }
sts() { $k -n vector get sts vector-aggregator -o jsonpath="$1"; }

# wait_for LP REASON_REGEX TIMEOUT_S: log each status change until it matches.
wait_for() {
  local prev="" line end=$((SECONDS + $3))
  while ((SECONDS < end)); do
    line="$(reason "$1"): $(message "$1")"
    [[ $line != "$prev" ]] && log "  $1 → $line"
    prev=$line
    [[ $(reason "$1") =~ ^($2)$ ]] && return 0
    sleep 3
  done
  log "  TIMEOUT waiting for $1 to reach $2"; return 1
}

# Port-forward Loki and Prometheus for queries.
$k -n observability port-forward svc/loki 3100:3100 >/dev/null 2>&1 & pf1=$!
$k -n observability port-forward svc/prometheus-server 9090:80 >/dev/null 2>&1 & pf2=$!
cleanup() {
  kill "$pf1" "$pf2" 2>/dev/null || true
  wait "$pf1" "$pf2" 2>/dev/null || true
  $k delete lp edits edits-copy content --ignore-not-found >/dev/null 2>&1 || true
  $k -n spillway-system patch deployment spillway-operator --type json \
    -p '[{"op":"remove","path":"/spec/template/spec/containers/0/args"}]' >/dev/null 2>&1 || true
}
trap cleanup EXIT
sleep 3
loki() { # loki QUERY: instant query; prints "label=value ..." (or "total=value")
  curl -sG localhost:3100/loki/api/v1/query --data-urlencode "query=$1" | python3 -c '
import json, sys
out = []
for x in json.load(sys.stdin)["data"]["result"]:
    labels = list(x["metric"].values()) or ["total"]
    out.append(labels[0] + "=" + format(float(x["value"][1]), ".1f"))
print(" ".join(out) or "none")'
}
first_event_after() { # first_event_after TEAM UNIX_NS: prints the first event's timestamp (ns) or nothing
  curl -sG localhost:3100/loki/api/v1/query_range --data-urlencode "query={team=\"$1\"}" \
    --data-urlencode "start=$2" --data-urlencode direction=forward --data-urlencode limit=1 |
    python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(min(int(v[0]) for s in r for v in s["values"]) if r else "")'
}

echo "## Setup"
log "aggregator pods: $($k -n vector get pods -l app.kubernetes.io/component=Aggregator -o jsonpath='{range .items[*]}{.metadata.name}({.status.containerStatuses[0].ready}) {end}')"
log "waiting for the operator's first config to be promoted (stable, no canary)"
for _ in $(seq 1 120); do
  [[ -n $(sts '{.metadata.annotations.spillway\.dev/stable-config}') && -z $(sts '{.metadata.annotations.spillway\.dev/canary-config}') && $(sts '{.spec.updateStrategy.rollingUpdate.partition}') == 0 ]] && break
  sleep 5
done
log "stable config $(sts '{.metadata.annotations.spillway\.dev/stable-config}'), partition $(sts '{.spec.updateStrategy.rollingUpdate.partition}')"

echo
echo "## 1. An example spec is rendered, validated, canaried and Ready, with events flowing"
log "spillwayctl validate examples/edits.yaml:"
bin/spillwayctl validate --vector-bin bin/vector examples/edits.yaml | sed 's/^/    /'
applied_ns=$(($(date +%s) * 1000000000))
start=$SECONDS
log "kubectl apply -f examples/edits.yaml"
$k apply -f examples/edits.yaml | sed 's/^/    /'
first="" prev=""
while ((SECONDS - start < 600)); do
  if [[ -z $first ]]; then
    first=$(first_event_after edits "$applied_ns")
    [[ -n $first ]] && log "  first {team=\"edits\"} event queryable in Loki: $(((first - applied_ns) / 1000000000))s after apply"
  fi
  line="$(reason edits): $(message edits)"
  [[ $line != "$prev" ]] && log "  edits → $line"
  prev=$line
  [[ $(reason edits) =~ ^(RolledOut|CanaryFailed|Invalid)$ ]] && break
  sleep 3
done
log "edits Ready $((SECONDS - start))s after apply"
sleep 60
log "events/s by type for {team=\"edits\"}, last 1m: $(loki 'sum by (type) (count_over_time({team="edits"} | json [1m])) / 60')"
log "events/s total for {team=\"edits\"}, last 1m: $(loki 'sum(count_over_time({team="edits"}[1m])) / 60')  (budget: 25/s)"

echo
echo "## 2. Malformed specs never reach an aggregator"
cat > /tmp/spillway-malformed.yaml <<'EOF'
apiVersion: spillway.dev/v1alpha1
kind: LogPipeline
metadata: {name: malformed}
spec:
  team: Edits Team
  sources:
    - {name: both, kafka: {topic: x}, kubernetes: {namespaces: [a]}}
  sampling: {keepPercent: {info: 150}}
  routing: {hot: false}
EOF
log "kubectl apply of a malformed spec:"
$k apply -f /tmp/spillway-malformed.yaml 2>&1 | sed 's/, spec\./\n* spec./g; s/^/    /' || true
log "  stored? $($k get lp malformed -o name 2>&1 | head -1)"
log "spillwayctl validate on the same file:"
bin/spillwayctl validate --no-vector /tmp/spillway-malformed.yaml 2>&1 | sed 's/^/    /' || true
before_stable=$(sts '{.metadata.annotations.spillway\.dev/stable-config}')
before_keys=$($k -n vector get cm vector-aggregator-config -o jsonpath='{.data}' | python3 -c 'import json,sys; print(sorted(json.load(sys.stdin)))')
log "a spec that's valid alone but claims a team already claimed (edits-copy):"
sed 's/name: edits$/name: edits-copy/' examples/edits.yaml | $k apply -f - | sed 's/^/    /'
wait_for edits-copy 'Invalid' 60
sleep 10
log "  stable config before: $before_stable, after: $(sts '{.metadata.annotations.spillway\.dev/stable-config}')"
log "  config revisions before: $before_keys"
log "  config revisions after:  $($k -n vector get cm vector-aggregator-config -o jsonpath='{.data}' | python3 -c 'import json,sys; print(sorted(json.load(sys.stdin)))')"
log "  canary started? '$(sts '{.metadata.annotations.spillway\.dev/canary-config}')' (empty means no)"
$k delete lp edits-copy >/dev/null

echo
echo "## 3. A change that fails at runtime is canaried and rolled back"
log "configuring the operator's cold storage at an endpoint that doesn't exist"
$k -n spillway-system patch deployment spillway-operator --type json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args","value":["--cold-bucket=spillway-cold","--cold-endpoint=http://minio.storage.svc:9000"]}]' >/dev/null
$k -n spillway-system rollout status deploy/spillway-operator --timeout=2m >/dev/null
stable_before=$(sts '{.metadata.annotations.spillway\.dev/stable-config}')
log "spillwayctl validate examples/content.yaml (cold storage configured): every offline check passes"
bin/spillwayctl validate --vector-bin bin/vector --cold-bucket spillway-cold --cold-endpoint http://minio.storage.svc:9000 examples/content.yaml | sed 's/^/    /'
start=$SECONDS
log "kubectl apply -f examples/content.yaml"
$k apply -f examples/content.yaml | sed 's/^/    /'
wait_for content 'CanaryFailed|RolledOut' 600
log "content settled $((SECONDS - start))s after apply"
log "  edits meanwhile: $(reason edits)"
sleep 75 # the canary pod drains and restarts on the stable config
log "  stable config before: $stable_before, after: $(sts '{.metadata.annotations.spillway\.dev/stable-config}')"
log "  pods: $($k -n vector get pods -l app.kubernetes.io/component=Aggregator -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.annotations.spillway\.dev/config-suffix} {end}')"
log "  {team=\"edits\"} events/s, last 1m: $(loki 'sum(count_over_time({team="edits"}[1m])) / 60')"

echo
echo "## Operator metrics (since the operator restarted for step 3)"
$k -n spillway-system port-forward deploy/spillway-operator 18080:8080 >/dev/null 2>&1 & pf3=$!
sleep 3
curl -s localhost:18080/metrics | grep -E '^spillway_operator_(validations|rollouts|orphaned_buffers)_total|^spillway_operator_validation_duration_seconds_(sum|count)' | sed 's/^/    /'
kill "$pf3"; wait "$pf3" 2>/dev/null || true
log "done"
