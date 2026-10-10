#!/usr/bin/env python3
"""Report what each team's policies dropped, and check sampling against the spec.

For every LogPipeline in the cluster, prints the events its dedupe, sampling
and budget stages dropped over the window (from the team-tagged Vector
metrics), then, for each team with sampling, the events in Loki per level.
With --ref TEAM=REF, where REF is a team reading the same source with no
sampling or budget, it also prints each level's kept share next to the
spec's keepPercent.

    bench/sampling.py --window 10m --ref edits=wiki-all    (`make sampling-check` port-forwards Loki and Prometheus)
"""
import argparse
import json
import subprocess
import urllib.parse
import urllib.request


def get(url, path, params):
    with urllib.request.urlopen(f"{url}{path}?" + urllib.parse.urlencode(params), timeout=60) as r:
        return json.load(r)["data"]["result"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--loki", default="http://localhost:3100")
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--kubectl", default="bin/kubectl")
    ap.add_argument("--window", default="10m")
    ap.add_argument("--ref", action="append", default=[], help="TEAM=REF: an unsampled team on the same source")
    args = ap.parse_args()
    w = args.window
    refs = dict(r.split("=", 1) for r in args.ref)

    pipelines = json.loads(subprocess.check_output([args.kubectl, "get", "lp", "-A", "-o", "json"]))["items"]

    def prom(q):
        return get(args.prometheus, "/api/v1/query", {"query": q})

    def by_level(team, field):
        q = f'sum by (lvl) (count_over_time({{team="{team}"}} | json lvl="{field}" [{w}]))'
        return {r["metric"].get("lvl", ""): float(r["value"][1])
                for r in get(args.loki, "/loki/api/v1/query", {"query": q})}

    stages = ("dedupe", "sample", "budget")
    # A team's src_ filters also count discards (other namespaces' pod logs),
    # so only the policy stages are summed.
    drops = {}
    for r in prom(f'sum by (team, component_id) (increase(vector_component_discarded_events_total'
                  f'{{intentional="true",team!="",component_id=~".+_({"|".join(stages)})"}}[{w}]))'):
        drops[(r["metric"]["team"], r["metric"]["component_id"].rsplit("_", 1)[1])] = float(r["value"][1])
    received = {r["metric"]["team"]: float(r["value"][1]) for r in prom(
        f'sum by (team) (increase(vector_component_received_events_total{{team!="",component_id=~".+_ids"}}[{w}]))')}
    sent = {r["metric"]["team"]: float(r["value"][1]) for r in prom(
        f'sum by (team) (increase(vector_component_sent_events_total{{team!="",component_id=~".+_hot"}}[{w}]))')}

    print(f"Policy drops over the last {w} (vector_component_discarded_events_total by team).\n")
    print("| Team | Events in | Dedupe | Sampling | Budget | To the hot sink | Kept |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for p in sorted(pipelines, key=lambda p: p["spec"]["team"]):
        t = p["spec"]["team"]
        n = received.get(t, 0)
        kept = f"{sent.get(t, 0) / n:.1%}" if n else "-"
        print(f"| `{t}` | {n:,.0f} | " + " | ".join(f"{drops.get((t, s), 0):,.0f}" for s in stages) +
              f" | {sent.get(t, 0):,.0f} | {kept} |")
    print("\nCounters are scraped every 15 s and `increase()` extrapolates to the window edges, so"
          " columns can differ from each other by a fraction of a percent.")

    for p in sorted(pipelines, key=lambda p: p["spec"]["team"]):
        s = p["spec"].get("sampling")
        if not s:
            continue
        t, field = p["spec"]["team"], s.get("levelField", "level")
        keep = s["keepPercent"]
        got = by_level(t, field)
        ref = refs.get(t)
        base = by_level(ref, field) if ref else {}
        print(f"\n### `{t}`: events in Loki by `{field}`" + (f", against `{ref}`" if ref else "") + "\n")
        if ref:
            print(f"| {field} | keepPercent | `{ref}` | `{t}` | Kept |")
            print("|---|---:|---:|---:|---:|")
        else:
            print(f"| {field} | keepPercent | `{t}` |")
            print("|---|---:|---:|")
        for lvl in sorted(set(got) | set(base), key=lambda l: -max(got.get(l, 0), base.get(l, 0))):
            pct = keep.get(lvl.lower(), 100 if lvl else "100 (none)")
            row = f"| `{lvl or '(none)'}` | {pct} |"
            if ref:
                b = base.get(lvl, 0)
                row += f" {b:,.0f} | {got.get(lvl, 0):,.0f} | " + (f"{got.get(lvl, 0) / b:.1%}" if b else "-") + " |"
            else:
                row += f" {got.get(lvl, 0):,.0f} |"
            print(row)


if __name__ == "__main__":
    main()
