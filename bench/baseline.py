#!/usr/bin/env python3
"""Measure pipeline volume and latency over a window and print a Markdown report.

Reads Prometheus (default http://localhost:9090; `make baseline` port-forwards
it). Every number is computed from counter increases or histogram buckets over
the same window, so the report can be re-run and compared later, e.g. against
M3's sampling and routing.

    bench/baseline.py --window 15m [--end 2026-10-01T17:15:00Z]
"""
import argparse
import datetime as dt
import json
import platform
import urllib.parse
import urllib.request

UNITS = {"s": 1, "m": 60, "h": 3600}


def seconds(window):
    return int(window[:-1]) * UNITS[window[-1]]


def query(prom, expr, at):
    url = f"{prom}/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": at.timestamp()})
    result = json.load(urllib.request.urlopen(url))["data"]["result"]
    return float(result[0]["value"][1]) if result else 0.0


def human_bytes(n):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:,.1f} {unit}"
        n /= 1024


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--window", default="15m", help="measurement window, e.g. 15m or 1h")
    ap.add_argument("--end", help="window end, RFC 3339 (default: now)")
    args = ap.parse_args()

    end = dt.datetime.fromisoformat(args.end.replace("Z", "+00:00")) if args.end else dt.datetime.now(dt.timezone.utc)
    w, secs = args.window, seconds(args.window)
    start = end - dt.timedelta(seconds=secs)

    def inc(metric, selector=""):
        return query(args.prometheus, f"sum(increase({metric}{{{selector}}}[{w}]))", end)

    def quantile(q):
        return query(args.prometheus,
                     f"histogram_quantile({q}, sum by (le) (increase(spillway_pipeline_latency_milliseconds_bucket[{w}])))", end)

    produced = inc("spillway_feeder_events_produced_total")
    produced_bytes = inc("spillway_feeder_produced_bytes_total")
    consumed = inc("vector_component_received_events_total", 'component_id="wikimedia"')
    raw_in = inc("vector_component_received_bytes_total", 'component_id="wikimedia"')
    event_in = inc("vector_component_sent_event_bytes_total", 'component_id="wikimedia"')
    to_sink = inc("vector_component_sent_events_total", 'component_id="loki"')
    sink_event_bytes = inc("vector_component_received_event_bytes_total", 'component_id="loki"')
    sink_wire = inc("vector_component_sent_bytes_total", 'component_id="loki"')
    latency_n = inc("spillway_pipeline_latency_milliseconds_count")
    latency_sum = inc("spillway_pipeline_latency_milliseconds_sum")
    errors = inc("vector_component_errors_total")
    discarded = inc("vector_component_discarded_events_total")
    produce_errors = inc("spillway_feeder_produce_errors_total")
    reconnects = inc("spillway_feeder_stream_reconnects_total")

    day = 86400 / secs
    rows = [
        ("Events produced by the feeder", f"{produced:,.0f}", f"{produced / secs:,.1f}/s"),
        ("Events consumed by the aggregator", f"{consumed:,.0f}", f"{consumed / secs:,.1f}/s"),
        ("Events sent to Loki", f"{to_sink:,.0f}", f"{to_sink / secs:,.1f}/s"),
        ("Bytes read from Kafka (raw payload)", human_bytes(raw_in), f"{human_bytes(raw_in / secs)}/s · {human_bytes(raw_in * day)}/day"),
        ("Event bytes in at the aggregator", human_bytes(event_in), f"{human_bytes(event_in / secs)}/s · {human_bytes(event_in * day)}/day"),
        ("Event bytes into the Loki sink (hot)", human_bytes(sink_event_bytes), f"{human_bytes(sink_event_bytes / secs)}/s · {human_bytes(sink_event_bytes * day)}/day"),
        ("Bytes sent to Loki (compressed)", human_bytes(sink_wire), f"{human_bytes(sink_wire / secs)}/s · {human_bytes(sink_wire * day)}/day"),
        ("Bytes produced by the feeder", human_bytes(produced_bytes), f"{human_bytes(produced_bytes / secs)}/s"),
    ]
    avg_event = raw_in / consumed if consumed else 0
    hot_ratio = sink_event_bytes / event_in if event_in else 0

    print(f"## Baseline: {start:%Y-%m-%d %H:%M}–{end:%H:%M} UTC ({w})\n")
    print(f"Measured {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M} UTC on {platform.node()} "
          f"({platform.machine()}, {platform.system()} {platform.release()}).\n")
    print("### Volume\n")
    print("| Measure | Window total | Rate |\n|---|---|---|")
    for r in rows:
        print(f"| {r[0]} | {r[1]} | {r[2]} |")
    print(f"\nAverage event size from Kafka: **{avg_event:,.0f} B**. "
          f"Hot-sink event bytes vs bytes in: **{hot_ratio:.1%}** (the M3 reduction baseline; "
          f"above 100% because the pipeline adds `spillway.sink_at` and `pipeline_latency_ms`).\n")
    print("### Latency (feeder produce → aggregator pre-sink)\n")
    print("| p50 | p95 | p99 | Mean | Events measured |\n|---|---|---|---|---|")
    mean = latency_sum / latency_n if latency_n else 0
    print(f"| {quantile(0.5):,.1f} ms | {quantile(0.95):,.1f} ms | {quantile(0.99):,.1f} ms | {mean:,.1f} ms | {latency_n:,.0f} |")
    print("\nQuantiles are interpolated within histogram buckets "
          "(1, 2.5, 5, 10, 15, 20, 30, 50, 75, 100, 150, 250, 500 ms, ...).\n")
    print("### Errors\n")
    print("| Vector errors | Vector discarded events | Feeder produce errors | Feeder reconnects |\n|---|---|---|---|")
    print(f"| {errors:,.0f} | {discarded:,.0f} | {produce_errors:,.0f} | {reconnects:,.0f} |")


if __name__ == "__main__":
    main()
