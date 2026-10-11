#!/usr/bin/env python3
"""Report a stepped-load benchmark run as Markdown: per-step throughput,
backlog, drops and latency, and the highest step the pipeline sustained.

Reads the load generator's log (step start and end times, events sent and
acknowledged by Kafka) and Prometheus (default http://localhost:9090;
bench/m4_bench.sh port-forwards it). Each step is measured over its last
part, after --warmup, so the rate change at its start doesn't count against
it. All times are UTC.

A step is **sustained** if, over its measured part:
  - the generator sent at least 98% of the target rate (else the load
    generator or Kafka was the limit, not the pipeline),
  - the aggregators consumed at least 95% of what was produced (counters are
    scraped every 15 s, so a few percent of noise is expected; backlog is
    the strict test),
  - Kafka consumer lag and both sink buffers grew by no more than 2 s of
    traffic over it, by a linear fit (no growing backlog),
  - no events were discarded, no component reported errors, and neither
    sink had a request rejected (a 429 from Loki is retried, so it isn't
    an error to Vector, but it means the sink is being throttled).

    bench/throughput.py --log loadgen.log [--warmup 60] [--drain-end 2026-10-11T15:00:00Z]
"""
import argparse
import datetime as dt
import json
import platform
import time
import urllib.parse
import urllib.request

AGG = 'app_kubernetes_io_component="Aggregator"'
TEAM = "bench"
SOURCE = "bench_src_load"
SINKS = ("hot_loki", "cold_s3")
# Pod name prefixes of the components whose CPU is reported.
APPS = ["vector-aggregator", "spillway-dual-role", "loki", "s3", "loadgen", "vector-agent", "prometheus-server"]


def parse_time(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def read_log(path):
    steps, results = {}, {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            rec = json.loads(line)
            if rec.get("msg") == "step finished":
                steps[rec["step"]] = {
                    "rate": rec["rate"], "start": parse_time(rec["start"]), "end": parse_time(rec["end"]),
                    "sent": rec["sent"],
                }
            elif rec.get("msg") == "step result":
                results[rec["step"]] = rec
    out = []
    for i in sorted(steps):
        s = steps[i]
        r = results.get(i, {})
        s["acked"], s["failed"] = r.get("acked"), r.get("failed")
        out.append(s)
    return out


class Prom:
    def __init__(self, url):
        self.url = url

    def query(self, expr, at):
        q = urllib.parse.urlencode({"query": expr, "time": at.timestamp()})
        res = json.load(urllib.request.urlopen(f"{self.url}/api/v1/query?{q}"))
        if res["status"] != "success":
            raise RuntimeError(f"{expr}: {res}")
        return res["data"]["result"]

    def scalar(self, expr, at):
        r = self.query(expr, at)
        return float(r[0]["value"][1]) if r else 0.0

    def by(self, expr, at, label):
        return {s["metric"].get(label, ""): float(s["value"][1]) for s in self.query(expr, at)}


def measure(p, start, end):
    """Everything for one interval [start, end]."""
    d = int((end - start).total_seconds())
    w = f"{d}s"

    def inc(metric, sel):
        return p.scalar(f"sum(increase({metric}{{{sel}}}[{w}]))", end)

    def backlog(metric, sel):
        """Mean, max and growth over the interval (the trend, from a linear
        fit: one spiky sample at either end doesn't decide it)."""
        series = f"sum(clamp_min({metric}{{{sel}}}, 0))[{w}:15s]"
        return (p.scalar(f"avg_over_time({series})", end), p.scalar(f"max_over_time({series})", end),
                p.scalar(f"deriv({series})", end) * d)

    m = {"secs": d}
    m["produced"] = inc("spillway_feeder_events_produced_total", 'feeder="loadgen"')
    m["consumed"] = inc("vector_component_received_events_total", f'{AGG},component_id="{SOURCE}"')
    for s in SINKS:
        m[s] = inc("vector_component_sent_events_total", f'{AGG},component_id="{s}"')
    m["discarded"] = inc("vector_component_discarded_events_total", f'{AGG},team="{TEAM}"') + sum(
        inc("vector_component_discarded_events_total", f'{AGG},component_id="{s}"') for s in SINKS)
    # Sinks retry throttled (429) and failed requests, so these aren't in
    # component_errors_total; they're the first sign a sink can't keep up.
    m["rejected"] = {c: inc("vector_http_client_responses_total", f'{AGG},component_id="{c}",status!~"2.."')
                     for c in SINKS}
    m["errors"] = inc("vector_component_errors_total", f'{AGG},component_id=~"{TEAM}_.+|{"|".join(SINKS)}"')
    lag_sel = f'{AGG},component_id="{SOURCE}"'
    m["lag"] = backlog("vector_kafka_consumer_lag", lag_sel)
    for s in SINKS:
        m["buf_" + s] = backlog("vector_buffer_size_events", f'{AGG},component_id="{s}"')
    for q in (0.5, 0.99):
        m[f"p{int(q * 100)}"] = p.scalar(
            f'histogram_quantile({q}, sum by (le) (increase(spillway_pipeline_latency_milliseconds_bucket{{{AGG},team="{TEAM}"}}[{w}])))', end)
    m["lat_n"] = inc("spillway_pipeline_latency_milliseconds_count", f'{AGG},team="{TEAM}"')
    # Vector's exporter expires a histogram whose flow stalls (backpressure)
    # and later republishes it from a different base, which increase() can't
    # undo: a step with a reset has no trustworthy latency.
    m["lat_resets"] = p.scalar(
        f'sum(resets(spillway_pipeline_latency_milliseconds_count{{{AGG},team="{TEAM}"}}[{w}]))', end)
    # CPU cores per component, from cAdvisor.
    m["cpu"] = p.by(
        'sum by (app) (label_replace(rate(container_cpu_usage_seconds_total{container!="",container!="POD"}'
        f'[{w}]), "app", "$1", "pod", "({"|".join(APPS)})-.*"))', end, "app")
    m["mem"] = p.by(
        'sum by (pod) (container_memory_working_set_bytes{container!="",container!="POD",namespace="vector",pod=~"vector-aggregator-.*"})',
        end, "pod")
    return m


def exact_totals(p, start, end):
    """Exact counts over [start, end], from every raw sample of each counter.
    increase() extrapolates to the range's edges, and a value at the end
    misses series that expired (Vector drops an idle component's metrics) or
    were left over from a previous run (a removed component's counter is
    exported a while longer). So each series is walked sample by sample, and
    a drop is taken as a restart from 0."""
    d = int((end - start).total_seconds())

    def key(series):
        return tuple(sorted(series["metric"].items()))

    def count(metric, sel):
        # Vector creates a series with its first event (one per Kafka
        # partition, say), so one absent at the start counts from 0.
        before = {key(s): float(s["value"][1]) for s in p.query(f"{metric}{{{sel}}}", start)}
        total = 0.0
        for series in p.query(f"{metric}{{{sel}}}[{d}s]", end):
            prev = before.get(key(series), 0.0)
            for _, v in series["values"]:
                v = float(v)
                total += v - prev if v >= prev else v
                prev = v
        return total

    if not p.query(f"vector_component_received_events_total{{{AGG},component_id=\"{SOURCE}\"}}[{d}s]", end):
        raise SystemExit("no bench source metrics during the run: was the bench pipeline deleted before the report?")
    t = {
        "consumed": count("vector_component_received_events_total", f'{AGG},component_id="{SOURCE}"'),
        "discarded": count("vector_component_discarded_events_total", f'{AGG},team="{TEAM}"'),
        "errors": count("vector_component_errors_total", f'{AGG},component_id=~"{TEAM}_.+|{"|".join(SINKS)}"'),
    }
    for c in SINKS:
        t[c] = count("vector_component_sent_events_total", f'{AGG},component_id="{c}"')
        t["rejected_" + c] = count("vector_http_client_responses_total", f'{AGG},component_id="{c}",status!~"2.."')
    return t


def loki_lines(url, start, end):
    """Lines Loki holds for the bench team between start and end: the count
    that matters, independent of Vector's own counters."""
    d = int((end - start).total_seconds())
    q = urllib.parse.urlencode({"query": f'sum(count_over_time({{team="{TEAM}"}}[{d}s]))', "time": end.timestamp()})
    res = json.load(urllib.request.urlopen(f"{url}/loki/api/v1/query?{q}", timeout=600))["data"]["result"]
    return int(float(res[0]["value"][1])) if res else 0


def wait_drain(p, timeout):
    """Wait until the run's backlog (Kafka lag and both sink buffers) is gone,
    so the totals include every event. Returns when it was, or None."""
    sel = f'{AGG},component_id=~"{SOURCE}|{"|".join(SINKS)}"'
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        now = dt.datetime.now(dt.timezone.utc)
        # Vector briefly reports a partition's lag as negative; that's no backlog.
        backlog = p.scalar(f'(sum(clamp_min(vector_kafka_consumer_lag{{{sel}}}, 0)) or vector(0))'
                           f' + (sum(vector_buffer_size_events{{{sel}}}) or vector(0))', now)
        if backlog == 0:
            # One more scrape, so the counters include the last events.
            time.sleep(35)
            return dt.datetime.now(dt.timezone.utc)
        time.sleep(15)
    return None


def verdict(step, m):
    rate = step["rate"]
    slack = 2 * rate
    reasons = []
    achieved = step["sent"] / (step["end"] - step["start"]).total_seconds()
    if achieved < 0.98 * rate:
        reasons.append(f"generator sent {achieved:,.0f}/s")
    if m["produced"] and m["consumed"] < 0.95 * m["produced"]:
        reasons.append(f"consumed {m['consumed'] / m['produced']:.0%} of produced")
    if m["lag"][2] > slack:
        reasons.append(f"lag grew {m['lag'][2]:,.0f}")
    for s in SINKS:
        if m["buf_" + s][2] > slack:
            reasons.append(f"{s} buffer grew {m['buf_' + s][2]:,.0f}")
    if m["discarded"] >= 1:
        reasons.append(f"{m['discarded']:,.0f} discarded")
    if m["errors"] >= 1:
        reasons.append(f"{m['errors']:,.0f} errors")
    for c, n in m["rejected"].items():
        if n >= 1:
            reasons.append(f"{c} had {n:,.0f} requests rejected")
    return not reasons, reasons


TOP_BUCKET_MS = 60000


def fmt_ms(v):
    if v != v:  # NaN: no samples
        return "–"
    if v >= TOP_BUCKET_MS:
        return f"≥ {TOP_BUCKET_MS // 1000} s"
    return f"{v:,.0f} ms" if v >= 100 else f"{v:,.1f} ms"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", required=True, help="the load generator's log (kubectl logs job/loadgen)")
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--loki", default="http://localhost:3100")
    ap.add_argument("--warmup", type=int, default=60, help="seconds at the start of each step not measured")
    ap.add_argument("--drain-end", help="when the run's totals are read, RFC 3339 (default: now)")
    ap.add_argument("--wait-drain", type=int, default=0, metavar="SECS",
                    help="first wait up to SECS for the backlog to drain; totals are read then")
    args = ap.parse_args()

    p = Prom(args.prometheus)
    steps = read_log(args.log)
    if not steps:
        raise SystemExit("no finished steps in the log")
    rows = []
    for s in steps:
        m = measure(p, s["start"] + dt.timedelta(seconds=args.warmup), s["end"])
        rows.append((s, m) + verdict(s, m))
    # The highest step with every step below it sustained too.
    sustained = None
    for s, m, ok, _ in rows:
        if not ok:
            break
        sustained = (s, m)

    run_start, run_end = steps[0]["start"], steps[-1]["end"]
    drained = wait_drain(p, args.wait_drain) if args.wait_drain else None
    drain_end = parse_time(args.drain_end) if args.drain_end else drained or dt.datetime.now(dt.timezone.utc)
    total = exact_totals(p, run_start - dt.timedelta(seconds=30), drain_end)
    in_loki = loki_lines(args.loki, run_start - dt.timedelta(seconds=30), drain_end)

    print(f"## Run: {run_start:%Y-%m-%d %H:%M}–{run_end:%H:%M} UTC\n")
    print(f"Reported {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M} UTC on {platform.node()} "
          f"({platform.machine()}, {platform.system()} {platform.release()}). Each step is measured over its "
          f"last {int((steps[0]['end'] - steps[0]['start']).total_seconds()) - args.warmup} s, after {args.warmup} s of warm-up.\n")

    print("### Throughput per step\n")
    print("| Target | Sent | Consumed | Hot (Loki) | Cold (S3) | Kafka lag mean / max / growth | Hot buffer mean / max / growth | Cold buffer mean / max / growth | Discarded | Errors | Sink requests rejected (hot / cold) | Sustained |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for s, m, ok, reasons in rows:
        secs = m["secs"]
        sent = s["sent"] / (s["end"] - s["start"]).total_seconds()

        def trio(t):
            return f"{t[0]:,.0f} / {t[1]:,.0f} / {t[2]:+,.0f}"
        print(f"| {s['rate']:,}/s | {sent:,.0f}/s | {m['consumed'] / secs:,.0f}/s | {m['hot_loki'] / secs:,.0f}/s | "
              f"{m['cold_s3'] / secs:,.0f}/s | {trio(m['lag'])} | {trio(m['buf_hot_loki'])} | {trio(m['buf_cold_s3'])} | "
              f"{m['discarded']:,.0f} | {m['errors']:,.0f} | {m['rejected']['hot_loki']:,.0f} / {m['rejected']['cold_s3']:,.0f} | {'yes' if ok else 'no: ' + '; '.join(reasons)} |")

    print("\n### Latency per step (feeder produce → aggregator pre-sink)\n")
    print("| Target | p50 | p99 | Events measured |\n|---|---|---|---|")
    for s, m, _, _ in rows:
        if m["lat_n"] < 1:
            # The hot buffer was full, so nothing reached the pre-sink stamp.
            print(f"| {s['rate']:,}/s | – | – | 0 |")
        elif m["lat_resets"]:
            print(f"| {s['rate']:,}/s | – | – | not measurable: the flow stalled and the histogram reset |")
        else:
            print(f"| {s['rate']:,}/s | {fmt_ms(m['p50'])} | {fmt_ms(m['p99'])} | {m['lat_n']:,.0f} |")

    print("\n### CPU per component (cores, mean over each step's measured part)\n")
    apps = APPS
    names = {"spillway-dual-role": "kafka", "vector-aggregator": "aggregators (2)", "vector-agent": "agents",
             "prometheus-server": "prometheus", "s3": "s3 (Silo)"}
    print("| Target | " + " | ".join(names.get(a, a) for a in apps) + " | Aggregator memory |")
    print("|---|" + "---|" * (len(apps) + 1))
    for s, m, _, _ in rows:
        mem = ", ".join(f"{v / 2**20:,.0f} MiB" for _, v in sorted(m["mem"].items()))
        print(f"| {s['rate']:,}/s | " + " | ".join(f"{m['cpu'].get(a, 0):.2f}" for a in apps) + f" | {mem} |")

    print(f"\n### Whole run, through the drain ({run_start:%H:%M}–{drain_end:%H:%M} UTC)\n")
    if args.wait_drain and not drained:
        print(f"**The backlog hadn't drained {args.wait_drain} s after the run**, so these totals are short of it.\n")
    acked = sum(s["acked"] or 0 for s in steps)
    failed = sum(s["failed"] or 0 for s in steps)
    print("| Acked by Kafka (generator log) | Produce failures | Consumed | Delivered hot | Lines in Loki | Delivered cold | Discarded | Errors | Sink requests rejected (hot / cold) |")
    print("|---|---|---|---|---|---|---|---|---|")
    print(f"| {acked:,} | {failed:,} | {total['consumed']:,.0f} | {total['hot_loki']:,.0f} | {in_loki:,} | {total['cold_s3']:,.0f} | "
          f"{total['discarded']:,.0f} | {total['errors']:,.0f} | "
          f"{total['rejected_hot_loki']:,.0f} / {total['rejected_cold_s3']:,.0f} |")
    print("\nVector's counts are exact: every raw sample of each counter from 30 s before the first step to the end "
          "of the drain, with restarts handled. *Lines in Loki* counts `{team=\"bench\"}` over the same range. "
          "The live feeds don't reach these components, so every event counted is the generator's. ", end="")
    print("**Every acknowledged event reached Loki.**" if in_loki == acked else
          f"**Loki holds {in_loki - acked:+,} lines against what Kafka acknowledged.**")
    print()

    if sustained:
        s, m = sustained
        print(f"**Sustained throughput: {s['rate']:,} events/s** (the highest step with every lower step also sustained), "
              f"p99 latency {fmt_ms(m['p99'])} at that rate.")
    else:
        print("**No step was sustained.**")


if __name__ == "__main__":
    main()
