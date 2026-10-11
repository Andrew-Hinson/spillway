#!/usr/bin/env python3
"""Print volume and estimated cost per team, hot vs cold, as Markdown.

The same queries as the Spillway cost dashboard (dashboards/cost.json), over
one window: event bytes entering each team's pipeline, into Loki (hot) and
into cold storage, the cold sink's measured gzip ratio, and an estimated
monthly cost from the prices given. Unclaimed data (the platform paths) is
hot only. Prices are defaults to edit, not quotes.

    bench/cost.py --window 30m    (`make cost-report` port-forwards Prometheus)
"""
import argparse
import json
import urllib.parse
import urllib.request

MONTH = 30 * 86400


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--window", default="30m")
    ap.add_argument("--hot-price", type=float, default=0.50, help="$ per GB ingested into hot storage")
    ap.add_argument("--cold-price", type=float, default=0.023, help="$ per GB-month in cold storage")
    ap.add_argument("--cold-retention-months", type=float, default=12)
    a = ap.parse_args()
    w = a.window

    def q(expr):
        u = f"{a.prometheus}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
        with urllib.request.urlopen(u, timeout=60) as r:
            return {x["metric"].get("team", ""): float(x["value"][1]) for x in json.load(r)["data"]["result"]}

    unclaimed = q(f'sum(rate(vector_component_received_event_bytes_total{{component_id=~"loki|loki_pods"}}[{w}]))').get("", 0)
    by_team = lambda cid, metric: q(f'sum by (team) (rate(vector_component_{metric}_total{{team!="",component_id=~"{cid}"}}[{w}]))')
    rin = by_team(".+_ids", "received_event_bytes")
    hot = by_team(".+_hot", "sent_event_bytes")
    cold = by_team(".+_cold", "sent_event_bytes")
    up = q(f'sum(rate(vector_component_sent_bytes_total{{component_id="cold_s3"}}[{w}]))').get("", 0)
    took = q(f'sum(rate(vector_component_received_event_bytes_total{{component_id="cold_s3"}}[{w}]))').get("", 0)
    ratio = up / took if took else 0

    rows = [(t, rin.get(t, 0), hot.get(t, 0), cold.get(t, 0)) for t in sorted(set(rin) | set(hot) | set(cold))]
    if unclaimed:
        rows.append(("(unclaimed)", unclaimed, unclaimed, 0))
    gb = lambda bps, secs: bps * secs / 1e9
    print(f"Volume and estimated cost over the last {w}, at its average rate. Prices: hot ${a.hot_price}/GB ingested, "
          f"cold ${a.cold_price}/GB-month kept {a.cold_retention_months:g} months. Cold gzip ratio measured: {ratio:.1%}.\n")
    print("| Team | In GB/day | Hot GB/day | Hot kept | Cold GB/day (events) | Cold GB/day (stored) | Hot $/month | Cold $/month | Total $/month |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    tot = [0.0] * 7
    for t, i, h, c in rows:
        hot_usd = gb(h, MONTH) * a.hot_price
        cold_usd = gb(c, MONTH) * ratio * a.cold_price * a.cold_retention_months
        vals = [gb(i, 86400), gb(h, 86400), gb(c, 86400), gb(c, 86400) * ratio, hot_usd, cold_usd, hot_usd + cold_usd]
        tot = [x + y for x, y in zip(tot, vals)]
        kept = f"{h / i:.1%}" if i else "-"
        print(f"| `{t}` | {vals[0]:.3f} | {vals[1]:.3f} | {kept} | {vals[2]:.3f} | {vals[3]:.4f} | ${vals[4]:.2f} | ${vals[5]:.2f} | ${vals[6]:.2f} |")
    kept = f"{tot[1] / tot[0]:.1%}" if tot[0] else "-"
    print(f"| **total** | {tot[0]:.3f} | {tot[1]:.3f} | {kept} | {tot[2]:.3f} | {tot[3]:.4f} | ${tot[4]:.2f} | ${tot[5]:.2f} | **${tot[6]:.2f}** |")


if __name__ == "__main__":
    main()
