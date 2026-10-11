#!/usr/bin/env python3
"""Measure hot-storage volume against volume in, over a window, as Markdown.

The M1 baseline (docs/results/baseline.md) defined the reduction metric as
event bytes into the hot sink ÷ event bytes in at the aggregator, and
measured 102.9% with no policies. This computes the same ratio now:
overall (every source the aggregators read, against every hot sink) and per
team reading a feeder (its Kafka source against <team>_hot).

    bench/volume.py --window 15m    (`make volume` port-forwards Prometheus)
"""
import argparse
import json
import urllib.parse
import urllib.request

M1_RATIO = 1.029  # docs/results/baseline.md


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--window", default="15m")
    ap.add_argument("--end", help="window end, Unix seconds (default: now)")
    a = ap.parse_args()
    w = a.window

    def q(expr, by="component_id"):
        params = {"query": expr}
        if a.end:
            params["time"] = a.end
        u = f"{a.prometheus}/api/v1/query?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(u, timeout=60) as r:
            return {x["metric"].get(by, ""): float(x["value"][1]) for x in json.load(r)["data"]["result"]}

    # Sources: every source on the aggregators (Kafka topics, and the pod
    # logs the agents send). Prometheus scrapes the agents too; their own
    # source would count pod logs twice.
    agg = 'app_kubernetes_io_component="Aggregator"'
    src = q(f'sum by (component_id) (increase(vector_component_sent_event_bytes_total{{{agg},component_kind="source",component_id!="internal_metrics"}}[{w}]))')
    sinks = q(f'sum by (component_id) (increase(vector_component_received_event_bytes_total{{{agg},component_id=~"hot_loki|loki|loki_pods"}}[{w}]))')
    hot = q(f'sum by (team) (increase(vector_component_sent_event_bytes_total{{team!="",component_id=~".+_hot"}}[{w}]))', "team")
    team_src = q(f'sum by (team) (increase(vector_component_sent_event_bytes_total{{team!="",component_id=~".+_src_.+",component_type="kafka"}}[{w}]))', "team")
    mb = lambda b: f"{b / 1e6:,.1f} MB"

    total_in, total_hot = sum(src.values()), sum(sinks.values())
    print(f"Volume over the last {w}: event bytes into hot storage (Loki) against event bytes in.\n")
    print("| | Event bytes in | Event bytes to hot | Hot / in | M1 baseline |")
    print("|---|---:|---:|---:|---:|")
    for team in sorted(team_src):
        i, h = team_src[team], hot.get(team, 0)
        print(f"| team `{team}` (its feeder topic) | {mb(i)} | {mb(h)} | {h / i:.1%} | {M1_RATIO:.1%} |")
    print(f"| **everything** (sources: {', '.join(sorted(src))}; sinks: {', '.join(sorted(sinks))}) "
          f"| {mb(total_in)} | {mb(total_hot)} | **{total_hot / total_in:.1%}** | {M1_RATIO:.1%} |")
    if total_in:
        print(f"\nHot volume is **{1 - (total_hot / total_in) / M1_RATIO:.1%} lower** than the M1 baseline's ratio would give "
              "for the same input.")


if __name__ == "__main__":
    main()
