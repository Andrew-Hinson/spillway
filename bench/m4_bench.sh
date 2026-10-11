#!/usr/bin/env bash
# M4.1 benchmark: stepped load through a full team pipeline, against a fresh
# `make up`. Prints the setup and bench/throughput.py's report as Markdown,
# for docs/results/m4.1-benchmark.md.
#
#   1. builds the load generator (feeders/loadgen) and loads it into kind
#   2. creates the bench.load topic and the bench team (deploy/bench), and
#      waits for its config to roll out through the canary
#   3. runs the generator: each rate in STEPS for STEP
#   4. waits for the backlog to drain, then reports each step and the run
#
# STEPS (events/s, comma-separated), STEP (per step, default 4m) and WARMUP
# (seconds of each step not measured, default 60). The run's files go to
# OUT (default bin/bench/<UTC time>). The script deletes the Job, the bench
# LogPipeline and the topic at the end.
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/.kubeconfig"
k=bin/kubectl
STEPS=${STEPS:-500,1000,2000,3000,4000,6000,8000}
STEP=${STEP:-4m}
WARMUP=${WARMUP:-60}
OUT=${OUT:-bin/bench/$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$OUT"
ts() { date -u +%H:%M:%S; }
log() { echo "$(ts) $*" >&2; }
reason() { $k get lp bench -o jsonpath='{.status.conditions[0].reason}' 2>/dev/null; }

pf=
cleanup() {
  [[ -n $pf ]] && kill "$pf" 2>/dev/null
  $k -n bench delete job loadgen --ignore-not-found >/dev/null 2>&1 || true
  $k delete lp bench --ignore-not-found >/dev/null 2>&1 || true
  $k -n kafka delete kafkatopic bench.load --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

tag=$(cat go.mod go.sum $(find feeders/loadgen -type f | sort) | sha256sum | cut -c1-12)
log "building spillway/loadgen:$tag"
docker build -q -t "spillway/loadgen:$tag" -f feeders/loadgen/Dockerfile . >/dev/null
bin/kind load docker-image "spillway/loadgen:$tag" --name spillway >/dev/null

log "creating bench.load and the bench team"
$k apply -f deploy/bench/topic.yaml >/dev/null
$k -n kafka wait kafkatopic/bench.load --for=condition=Ready --timeout=2m >/dev/null
start=$SECONDS
$k apply -f deploy/bench/pipeline.yaml >/dev/null
until [[ $(reason) =~ ^(RolledOut|CanaryFailed|Invalid|ValidationFailed)$ ]]; do
  ((SECONDS - start < 600)) || { log "TIMEOUT waiting for the bench pipeline to roll out"; exit 1; }
  sleep 5
done
log "bench → $(reason) $((SECONDS - start))s after apply"
[[ $(reason) == RolledOut ]] || exit 1
# Both aggregators join the team's consumer group and export its metrics.
sleep 60

log "starting the load generator: $STEPS events/s, $STEP each"
$k -n bench delete job loadgen --ignore-not-found >/dev/null
sed -e "s|spillway/loadgen:dev|spillway/loadgen:$tag|" \
    -e "s|value: \"250,500,1000\"|value: \"$STEPS\"|" \
    -e "s|value: \"3m\"|value: \"$STEP\"|" deploy/bench/loadgen.yaml | $k apply -f - >/dev/null
n=$(tr ',' '\n' <<<"$STEPS" | wc -l)
mins=${STEP%m}
$k -n bench wait job/loadgen --for=condition=Complete --timeout="$((n * mins + 10))m" >/dev/null || {
  log "the load generator didn't complete"; $k -n bench logs job/loadgen | tail -20 >&2; exit 1; }
$k -n bench logs job/loadgen > "$OUT/loadgen.log"
log "load generator done; its log is in $OUT/loadgen.log"

$k -n observability port-forward svc/prometheus-server 9090:80 >/dev/null 2>&1 & pf=$!
sleep 2
log "waiting up to 15 min for the backlog to drain, then reporting"
python3 bench/throughput.py --log "$OUT/loadgen.log" --warmup "$WARMUP" --wait-drain 900 | tee "$OUT/report.md"
