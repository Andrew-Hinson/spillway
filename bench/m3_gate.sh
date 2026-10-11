#!/usr/bin/env bash
# M3 gate demo: run against a fresh `make up`. Prints a timestamped log of
# each gate criterion, for docs/results/m3-gate.md:
#
#   1. two teams on two feeders (content on Wikimedia, oss on GitHub), each
#      with redaction, sampling, a budget, and hot and cold routing, Ready
#      through the canary
#   2. injected fixtures (in both feeds and in pod logs) produce zero leaks
#      in the hot sink, and none in cold storage
#   3. volume reduction measured against the M1 baseline
#
# WINDOW (default 15m) is how long traffic and fixtures run before measuring.
# The script deletes its LogPipelines and the fixture injector at the end.
set -euo pipefail
cd "$(dirname "$0")/.."
export KUBECONFIG="$PWD/.kubeconfig"
k=bin/kubectl
W=${WINDOW:-15m}
ts() { date -u +%H:%M:%S; }
log() { echo "$(ts) $*"; }
reason() { $k get lp "$1" -o jsonpath='{.status.conditions[0].reason}' 2>/dev/null; }

cleanup() {
  $k delete lp content oss --ignore-not-found >/dev/null 2>&1 || true
  $k -n fixtures delete deployment fixture-injector --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "## Setup"
log "commit $(git rev-parse --short HEAD); LogPipelines before: $($k get lp -o name | wc -l | tr -d ' ')"
log "feeders: $($k -n feeders get deploy -o jsonpath='{range .items[*]}{.metadata.name} {end}')"
log "starting the fixture injector: 4 fixtures/s into each of wikimedia.recentchange, github.events and its pod's stdout"
make -s fixtures RATE=4 >/dev/null

echo
echo "## 1. Two teams on two feeders, each with every policy"
start=$SECONDS
$k apply -f examples/content.yaml -f examples/oss.yaml | sed 's/^/    /'
$k get lp content oss -o json | python3 -c '
import json, sys
print("    | Team | Feeder topic | Redaction | Sampling | Budget | Hot | Cold |")
print("    |---|---|---|---|---|---|---|")
for p in json.load(sys.stdin)["items"]:
    s = p["spec"]
    r = s.get("redaction") or {}
    if r.get("disabled"):
        red = "off"
    elif r.get("patterns"):
        red = ", ".join(r["patterns"])
    else:
        red = "every pattern (default)"
    smp = s.get("sampling")
    if smp:
        keep = ", ".join("%s %s%%" % kv for kv in smp["keepPercent"].items())
        smp = "%s: %s" % (smp.get("levelField", "level"), keep)
    else:
        smp = "none"
    rt = s.get("routing") or {}
    budget = (s.get("budget") or {}).get("maxEventsPerSec", "none")
    topic = s["sources"][0]["kafka"]["topic"]
    print("    | %s | %s | %s | %s | %s/s | %s | %s |" % (s["team"], topic, red, smp, budget, rt.get("hot", True), rt.get("cold", False)))'
for lp in content oss; do
  until [[ $(reason $lp) =~ ^(RolledOut|CanaryFailed|Invalid|ValidationFailed)$ ]]; do
    ((SECONDS - start < 600)) || { log "TIMEOUT waiting for $lp"; exit 1; }
    sleep 5
  done
  log "$lp → $(reason $lp) $((SECONDS - start))s after apply"
  [[ $(reason $lp) == RolledOut ]] || exit 1
done

echo
log "measuring over the next $W"
secs=$(( ${W%m} * 60 ))
sleep "$secs"
# Cold storage batches for up to 60 s: let the window's last batch land.
sleep 75

echo
echo "## 2. Injected fixtures: zero leaks"
echo "### Hot sink (make leak-check)"
leak=0; make -s leak-check WINDOW="$W" | sed 's/^/    /' || leak=$?
echo "### Cold storage (make cold-check)"
cold=0; make -s cold-check 2>/dev/null | grep -v -E '^(Unable|[0-9a-f.]+: Pull|Digest|Status)' | sed 's/^/    /' || cold=$?

echo
echo "## 3. Volume against the M1 baseline (make volume)"
make -s volume WINDOW="$W" | sed 's/^/    /'

echo
echo "## Policies per team"
make -s sampling-check WINDOW="$W" | sed 's/^/    /'
echo
make -s cost-report WINDOW="$W" | sed 's/^/    /'

echo
if ((leak || cold)); then
  log "GATE FAILED: leak-check exit $leak, cold-check exit $cold"
  exit 1
fi
log "GATE PASSED"
