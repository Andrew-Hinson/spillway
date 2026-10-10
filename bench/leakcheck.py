#!/usr/bin/env python3
"""Search the hot sink for injected fixture values and print a Markdown report.

The fixture injector (`make fixtures`) mixes fictional PII into the live
streams, each record tagged `spillway.fixture: {id, pattern}`. This counts,
over a window, the fixtures that reached Loki on every path (team streams,
the platform feed and platform pod logs), and searches the same streams for
any fixture value that survived redaction. Fixture values come from ranges no
real data uses, so any hit is a leak.

Exits 1 on a leak, and also if no fixtures arrived: zero leaks over zero
fixtures proves nothing.

    bench/leakcheck.py --window 15m    (`make leak-check` port-forwards Loki and Prometheus)
"""
import argparse
import json
import sys
import urllib.parse
import urllib.request

# Everything Spillway writes to Loki, by the label each path sets.
STREAMS = {
    "team": '{team=~".+"}',           # hot_loki: team pipelines
    "feeder": '{feeder=~".+"}',       # loki: the platform feed path
    "namespace": '{namespace=~".+"}', # loki_pods: platform pod logs
}
# The tag, raw in a JSON line or escaped inside a pod log's message string.
TAG = r'fixture\\?":\{\\?"id'
# One per pattern, matching only the fictional ranges (feeders/fixtures).
LEAKS = {
    "ssn": r"123-45-\d{4}",
    "email": r"fixtures\.spillway\.test",
    "phone": r"555-01\d\d",
    "memberId": r"MBR-9\d{7}",
}


def get(url, path, params):
    with urllib.request.urlopen(f"{url}{path}?" + urllib.parse.urlencode(params), timeout=60) as r:
        return json.load(r)["data"]["result"]


def by_label(results, label):
    return {r["metric"].get(label, "total"): float(r["value"][1]) for r in results}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--loki", default="http://localhost:3100")
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--window", default="15m")
    args = ap.parse_args()
    w = args.window

    def loki(expr):
        return get(args.loki, "/loki/api/v1/query", {"query": expr})

    injected = by_label(get(args.prometheus, "/api/v1/query",
                            {"query": f"sum by (output) (increase(spillway_fixtures_injected_total[{w}]))"}), "output")

    rows, delivered, leaks, unmasked, samples = [], 0, 0, 0, []
    for label, sel in STREAMS.items():
        got = by_label(loki(f"sum by ({label}) (count_over_time({sel} |~ `{TAG}` [{w}]))"), label)
        # A fixture that arrives without a mask, even if no value regex hits.
        bare = by_label(loki(f"sum by ({label}) (count_over_time({sel} |~ `{TAG}` !~ `\\[REDACTED\\]` [{w}]))"), label)
        hits = {}
        for pattern, rx in LEAKS.items():
            for stream, n in by_label(loki(f"sum by ({label}) (count_over_time({sel} |~ `{rx}` [{w}]))"), label).items():
                hits.setdefault(stream, {})[pattern] = n
                if n and len(samples) < 3:
                    lines = get(args.loki, "/loki/api/v1/query_range",
                                {"query": f"{sel} |~ `{rx}`", "limit": 1, "since": w})
                    samples += [v[1][:300] for s in lines for v in s["values"]]
        for stream in sorted(set(got) | set(hits)):
            leaked = sum(hits.get(stream, {}).values())
            rows.append((f"{label}={stream}", got.get(stream, 0), bare.get(stream, 0),
                         ", ".join(f"{p} {n:.0f}" for p, n in hits.get(stream, {}).items() if n) or "0"))
            delivered += got.get(stream, 0)
            unmasked += bare.get(stream, 0)
            leaks += leaked

    print(f"Fixture leak check over the last {w}.\n")
    print("Injected (spillway_fixtures_injected_total): " +
          (", ".join(f"{k} {v:,.0f}" for k, v in sorted(injected.items())) or "none"))
    print("\n| Stream | Fixtures delivered | Delivered without a mask | Fixture values found |")
    print("|---|---:|---:|---|")
    for stream, got, bare, found in rows:
        print(f"| `{stream}` | {got:,.0f} | {bare:,.0f} | {found} |")
    print(f"| **total** | **{delivered:,.0f}** | **{unmasked:,.0f}** | **{leaks:,.0f}** |")
    for s in samples:
        print(f"\nLeaked line: `{s}`")

    if leaks or unmasked:
        print(f"\nFAIL: {leaks:.0f} fixture values and {unmasked:.0f} unmasked fixtures in the hot sink.")
        sys.exit(1)
    if not delivered:
        print("\nFAIL: no fixtures reached the hot sink, so there's nothing to check. Is `make fixtures` running?")
        sys.exit(1)
    print(f"\nPASS: {delivered:,.0f} fixtures delivered, 0 values leaked.")


if __name__ == "__main__":
    main()
