#!/usr/bin/env bash
# M4.2 chaos experiments, against a fresh `make up`. Each scenario runs the
# load generator under a run name of its own, injects a fault partway, waits
# for the backlog to drain, and counts every event in each sink by event ID
# (bench/eventcheck.py). Prints Markdown for docs/results/m4.2-chaos.md.
#
#   aggkill    SIGKILL an aggregator (force-delete its pod, no grace)
#   lokistop   freeze Loki (SIGSTOP) for OUTAGE seconds, then resume it
#   s3stop     freeze cold storage (Silo) the same way
#   diskfull   put an aggregator's data dir on a tmpfs with only FREE MiB
#              free, then freeze Loki until its hot buffer fills it
#   agentkill  write the events as pod logs, and SIGKILL the agent on the
#              generator's node
#   pods       the same pod logs with no fault: the agent path's control
#
# Two teams read the events (deploy/chaos/pipelines.yaml): chaos drops
# nothing, so the check is exact; chaosdrop samples out debug and throttles,
# so the check is produced = delivered + counted drops.
#
# SCENARIOS (default: all, in that order), RATE (events/s, default 1000;
# PODS_RATE 500 for agentkill), MINUTES per run (default 6), FAULT_AT
# (seconds into the run, default 120), OUTAGE (seconds, default 120), FREE
# (MiB left free on the disk, default 200), OUT (default bin/chaos/<UTC time>).
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/.kubeconfig"
k=bin/kubectl
SCENARIOS=${SCENARIOS:-aggkill lokistop s3stop diskfull pods agentkill}
RATE=${RATE:-1000}
PODS_RATE=${PODS_RATE:-500}
MINUTES=${MINUTES:-6}
FAULT_AT=${FAULT_AT:-120}
OUTAGE=${OUTAGE:-120}
FREE=${FREE:-200}
# Vector refuses to start on a disk smaller than its buffers' total max_size
# (3 GiB on an aggregator: loki, loki_pods, hot_loki, cold_s3), so the tmpfs
# is big enough and a filler file takes all but FREE MiB.
DISK=3300
OUT=${OUT:-bin/chaos/$(date -u +%Y%m%dT%H%M%SZ)}
# Run names are unique per invocation: cold storage keeps earlier runs' events.
SUFFIX=$(date -u +%H%M)
mkdir -p "$OUT/cold"
ts() { date -u +%H:%M:%S; }
log() { echo "$(ts) $*" >&2; }
event() { echo "- $(ts) $*"; }   # into the report

node_of() { $k -n "$2" get pod "$1" -o jsonpath='{.spec.nodeName}'; }
loki_node() { node_of loki-0 observability; }
s3_pod() { $k -n storage get pods -l app.kubernetes.io/name=s3 -o jsonpath='{.items[0].metadata.name}'; }
s3_node() { node_of "$(s3_pod)" storage; }
# Signal a process by name inside a kind node: the node container sees
# every pod's processes. No Kubernetes object changes.
signal() { docker exec "$1" pkill "-$2" -x "$3"; }

tmpfs_node= tmpfs_path=
pf=()
cleanup() {
  # Never leave a sink frozen or an aggregator on a tmpfs.
  for n in $(bin/kind get nodes --name spillway 2>/dev/null); do
    docker exec "$n" pkill -CONT -x loki 2>/dev/null || true
    docker exec "$n" pkill -CONT -x silo 2>/dev/null || true
  done
  if [[ -n $tmpfs_path ]]; then
    docker exec "$tmpfs_node" umount -l "$tmpfs_path" 2>/dev/null || true
    $k -n vector delete pod vector-aggregator-0 --wait=false >/dev/null 2>&1 || true
  fi
  ((${#pf[@]})) && kill "${pf[@]}" 2>/dev/null
  $k delete -f deploy/chaos/pipelines.yaml --ignore-not-found >/dev/null 2>&1 || true
  $k delete namespace chaos chaos-pods --ignore-not-found --wait=false >/dev/null 2>&1 || true
  $k -n kafka delete kafkatopic chaos.load --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

# The operator has no rollout in progress: no canary, every pod current and Ready.
idle() {
  [[ -z $($k -n vector get sts vector-aggregator -o jsonpath='{.metadata.annotations.spillway\.dev/canary-config}') ]] &&
    [[ $($k -n vector get sts vector-aggregator -o jsonpath='{.status.updateRevision}') == \
       $($k -n vector get sts vector-aggregator -o jsonpath='{.status.currentRevision}') ]] &&
    [[ $($k -n vector get sts vector-aggregator -o jsonpath='{.status.readyReplicas}') == \
       $($k -n vector get sts vector-aggregator -o jsonpath='{.spec.replicas}') ]]
}
wait_idle() {
  local start=$SECONDS
  until idle; do
    ((SECONDS - start < 600)) || { log "TIMEOUT waiting for the aggregators to settle"; exit 1; }
    sleep 5
  done
}

restarts() {
  $k get pods -A -l 'app.kubernetes.io/name in (vector,loki)' \
    -o jsonpath='{range .items[*]}{.metadata.name}={.status.containerStatuses[0].restartCount}{.status.containerStatuses[0].lastState.terminated.reason} {end}'
}

start_run() { # run namespace stdout rate
  sed -e "s|spillway/loadgen:dev|spillway/loadgen:$tag|" -e "s|NAMESPACE|$2|g" \
      -e "s|loadgen-RUN|loadgen-$1|" -e "s|value: \"RUN\"|value: \"$1\"|" -e "s|value: \"STDOUT\"|value: \"$3\"|" \
      -e "s|value: \"STEPS\"|value: \"$4\"|" -e "s|value: \"STEP_DURATION\"|value: \"${MINUTES}m\"|" \
      deploy/chaos/loadgen.yaml | $k apply -f - >/dev/null
  until [[ -n $($k -n "$2" get pods -l job-name="loadgen-$1" -o name 2>/dev/null) ]]; do sleep 1; done
  $k -n "$2" wait pod -l job-name="loadgen-$1" --for=condition=Ready --timeout=2m >/dev/null
  event "run \`$1\` started: $4 events/s for $MINUTES min$([[ $3 == true ]] && echo ', as pod logs in `chaos-pods`')"
}

# Committed lag, summed over a consumer group's partitions, from Kafka
# itself. (Vector's lag gauge keeps a partition's last value after a
# rebalance moves the partition away, so it can show a backlog forever.)
kafka_lag() {
  $k -n kafka exec spillway-dual-role-0 -c kafka -- bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$1" 2>/dev/null |
    awk '$1 == g && $6 ~ /^[0-9]+$/ { s += $6 } END { print s + 0 }' g="$1"
}

buffered() {
  curl -s http://localhost:9090/api/v1/query --data-urlencode \
    'query=sum(vector_buffer_size_events{app_kubernetes_io_component="Aggregator",component_id=~"hot_loki|cold_s3"}) or vector(0)' |
    python3 -c 'import json,sys; print(int(float(json.load(sys.stdin)["data"]["result"][0]["value"][1])))'
}

finish_run() { # run namespace [consumer groups] [lag slack]
  $k -n "$2" wait job/"loadgen-$1" --for=condition=Complete --timeout="$((MINUTES + 10))m" >/dev/null || {
    log "run $1 didn't complete"; $k -n "$2" logs job/"loadgen-$1" | tail -5 >&2; exit 1; }
  $k -n "$2" logs job/"loadgen-$1" > "$OUT/$1.log"
  # The run's records, which a fast-rotating pod log may have lost.
  $k -n "$2" get pods -l job-name="loadgen-$1" \
    -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.message}' >> "$OUT/$1.log"
  event "run \`$1\` finished: $(grep -o '"acked":[0-9]*' "$OUT/$1.log" | tail -1 | cut -d: -f2) events acknowledged"
  log "waiting for the backlog to drain"
  local groups=${3:-spillway-chaos-load spillway-chaosdrop-load} slack=${4:-0} start=$SECONDS lag buf
  while :; do
    lag=0; for g in $groups; do lag=$((lag + $(kafka_lag "$g"))); done
    buf=$(buffered)
    ((lag <= slack && buf == 0)) && break
    ((SECONDS - start < 900)) || { log "the backlog didn't drain: lag $lag, buffered $buf"; exit 1; }
    sleep 10
  done
  # One more scrape and commit interval, so the counters include the last events.
  sleep 20
  # Cold storage batches for up to 60 s.
  sleep 75
  event "drained; restarts: $(restarts)"
}

sync_cold() {
  docker run --rm --network host --user "$(id -u):$(id -g)" -v "$PWD/$OUT/cold:/out" \
    -e AWS_ACCESS_KEY_ID="$($k -n storage get secret cold-storage -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)" \
    -e AWS_SECRET_ACCESS_KEY="$($k -n storage get secret cold-storage -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)" \
    -e AWS_DEFAULT_REGION=us-east-1 -e HOME=/tmp amazon/aws-cli:2.37.12 \
    --endpoint-url http://localhost:9000 s3 sync --only-show-errors s3://spillway-cold /out
}

check() { # run source(s): load checks both teams; pods checks chaos only
  sync_cold
  echo
  python3 bench/eventcheck.py --log "$OUT/$1.log" --team chaos --source "$2" --cold-dir "$OUT/cold" --json "$OUT/$1-chaos.json"
  if [[ $2 == load ]]; then
    python3 bench/eventcheck.py --log "$OUT/$1.log" --team chaosdrop --source load --cold-dir "$OUT/cold" --drops --json "$OUT/$1-chaosdrop.json"
  fi
}

kafka_run() { start_run "$1" chaos false "$RATE"; }

scenario_aggkill() {
  echo "### Aggregator kill"; echo
  kafka_run aggkill$SUFFIX
  sleep "$FAULT_AT"
  $k -n vector delete pod vector-aggregator-0 --grace-period=0 --force >/dev/null 2>&1
  event "SIGKILLed vector-aggregator-0 (force delete, no grace)"
  finish_run aggkill$SUFFIX chaos
  check aggkill$SUFFIX load
}

scenario_lokistop() {
  echo "### Hot sink outage (Loki frozen)"; echo
  kafka_run lokistop$SUFFIX
  sleep "$FAULT_AT"
  local node; node=$(loki_node)
  signal "$node" STOP loki; event "froze Loki (SIGSTOP) on $node"
  sleep "$OUTAGE"
  signal "$node" CONT loki; event "resumed Loki after $OUTAGE s"
  finish_run lokistop$SUFFIX chaos
  check lokistop$SUFFIX load
}

scenario_s3stop() {
  echo "### Cold sink outage (Silo frozen)"; echo
  kafka_run s3stop$SUFFIX
  sleep "$FAULT_AT"
  local node; node=$(s3_node)
  signal "$node" STOP silo; event "froze Silo (SIGSTOP) on $node"
  sleep "$OUTAGE"
  signal "$node" CONT silo; event "resumed Silo after $OUTAGE s"
  finish_run s3stop$SUFFIX chaos
  check s3stop$SUFFIX load
}

scenario_diskfull() {
  echo "### Disk full (an aggregator's data dir with $FREE MiB free)"; echo
  tmpfs_node=$(node_of vector-aggregator-0 vector)
  local pv; pv=$($k -n vector get pvc data-vector-aggregator-0 -o jsonpath='{.spec.volumeName}')
  tmpfs_path=$($k get pv "$pv" -o jsonpath='{.spec.hostPath.path}{.spec.local.path}')
  # Mounted over the volume's directory on the node, then the pod restarted
  # (gracefully, with nothing buffered), so its data dir is the tmpfs.
  docker exec "$tmpfs_node" mount -t tmpfs -o size="${DISK}m" tmpfs "$tmpfs_path"
  docker exec "$tmpfs_node" fallocate -l "$((DISK - FREE))MiB" "$tmpfs_path/filler"
  $k -n vector delete pod vector-aggregator-0 >/dev/null
  sleep 5; wait_idle
  event "vector-aggregator-0's data dir is a ${DISK} MiB tmpfs with $FREE MiB free ($tmpfs_path on $tmpfs_node)"
  kafka_run diskfull$SUFFIX
  sleep "$FAULT_AT"
  local node; node=$(loki_node)
  signal "$node" STOP loki; event "froze Loki (SIGSTOP), so the hot buffer grows"
  local start=$SECONDS used=0
  while ((SECONDS - start < 2 * OUTAGE)); do
    used=$(docker exec "$tmpfs_node" df --output=pcent "$tmpfs_path" | tail -1 | tr -dc 0-9)
    ((used >= 100)) && break
    sleep 5
  done
  event "tmpfs ${used}% full after $((SECONDS - start)) s"
  sleep 60   # a minute with the disk full
  signal "$node" CONT loki; event "resumed Loki after $((SECONDS - start)) s"
  finish_run diskfull$SUFFIX chaos
  check diskfull$SUFFIX load
  # Back to the real volume: unmount, and restart the pod (its buffers are empty).
  docker exec "$tmpfs_node" umount -l "$tmpfs_path"; tmpfs_path=
  $k -n vector delete pod vector-aggregator-0 >/dev/null
  sleep 5; wait_idle
  event "vector-aggregator-0 back on its volume"
}

scenario_pods() {
  echo "### Pod logs, no fault (the agent scenario's control)"; echo
  start_run pods$SUFFIX chaos-pods true "$PODS_RATE"
  sleep 60
  finish_run pods$SUFFIX chaos-pods spillway-pods 500
  check pods$SUFFIX pods
}

scenario_agentkill() {
  echo "### Agent kill (events as pod logs)"; echo
  start_run agentkill$SUFFIX chaos-pods true "$PODS_RATE"
  sleep "$FAULT_AT"
  local node agent
  node=$($k -n chaos-pods get pods -l job-name=loadgen-agentkill$SUFFIX -o jsonpath='{.items[0].spec.nodeName}')
  agent=$($k -n vector get pods -l app.kubernetes.io/component=Agent --field-selector spec.nodeName="$node" -o jsonpath='{.items[0].metadata.name}')
  $k -n vector delete pod "$agent" --grace-period=0 --force >/dev/null 2>&1
  event "SIGKILLed agent $agent on $node (force delete, no grace)"
  # Other pods' logs keep the shared agents source's lag a little above 0,
  # and the agent's own buffer (on its node) drains first: a minute more.
  sleep 60
  finish_run agentkill$SUFFIX chaos-pods spillway-pods 500
  check agentkill$SUFFIX pods
}

tag=$(cat go.mod go.sum $(find feeders/loadgen -type f | sort) | sha256sum | cut -c1-12)
log "building spillway/loadgen:$tag"
docker build -q -t "spillway/loadgen:$tag" -f feeders/loadgen/Dockerfile . >/dev/null
bin/kind load docker-image "spillway/loadgen:$tag" --name spillway >/dev/null

$k -n observability port-forward svc/prometheus-server 9090:80 >/dev/null 2>&1 & pf+=($!)
$k -n observability port-forward svc/loki 3100:3100 >/dev/null 2>&1 & pf+=($!)
$k -n storage port-forward svc/s3 9000:9000 >/dev/null 2>&1 & pf+=($!)

wait_idle
log "creating chaos.load and the chaos and chaosdrop teams"
$k apply -f deploy/chaos/topic.yaml >/dev/null
$k -n kafka wait kafkatopic/chaos.load --for=condition=Ready --timeout=2m >/dev/null
$k create namespace chaos-pods --dry-run=client -o yaml | $k apply -f - >/dev/null
$k apply -f deploy/chaos/pipelines.yaml >/dev/null
start=$SECONDS
for lp in chaos chaosdrop; do
  until [[ $($k get lp $lp -o jsonpath='{.status.conditions[0].reason}') == RolledOut ]]; do
    ((SECONDS - start < 600)) || { log "TIMEOUT waiting for $lp to roll out"; exit 1; }
    sleep 5
  done
done
sleep 60   # both aggregators join the teams' consumer groups
echo "## Chaos run $(date -u +%Y-%m-%d) at $(git rev-parse --short HEAD)"
echo
event "teams chaos and chaosdrop RolledOut; restarts: $(restarts)"
echo
for s in $SCENARIOS; do
  log "scenario $s"
  # Not a pipe: a scenario runs in this shell, so cleanup sees its tmpfs.
  "scenario_$s" > >(tee "$OUT/$s.md")
  echo
done
