#!/usr/bin/env python3
"""Count a chaos run's events in each sink, by event ID, and print Markdown.

A run is one load generator Job with -run NAME (feeders/loadgen): its events
carry loadgen.run and loadgen.seq (1 to N), and their event_id is
uuid5(namespace, "NAME/seq"), so every line in a sink identifies one event.
For each team and sink the check reports how many of the N acknowledged
events arrived, which are missing, and how many arrived more than once.

  - hot: every line Loki holds for {team, source} in the run's time range,
    exported in 2-second windows (Loki returns at most 5,000 lines a query;
    a full window is split)
  - cold: the team's objects in a local copy of the bucket (--cold-dir)

A team that drops on purpose (--drops) is expected to have no debug event
in hot, and every other event either in hot or counted as throttled by its
budget, from Vector's counters:

    produced = distinct in hot + sampled (debug) + throttled

    bench/eventcheck.py --log loadgen.log --team chaos --source load [--cold-dir DIR] [--drops]
"""
import argparse
import datetime as dt
import gzip
import json
import os
import sys
import urllib.parse
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(__file__))
from throughput import AGG, Prom, counter_total, parse_time  # noqa: E402

ID_SPACE = uuid.UUID("6f1c2a8e-3b5d-4c7a-9e21-5d8b0f4a7c63")  # feeders/loadgen/event.go
LIMIT = 5000


def event_id(run, seq):
    return str(uuid.uuid5(ID_SPACE, f"{run}/{seq}"))


def read_run(path):
    """The run's name, how many events it had acknowledged, and its span."""
    run, start, end, acked = None, None, None, {}
    with open(path) as f:
        for line in f:
            if not line.startswith("{"):
                continue
            rec = json.loads(line)
            msg = rec.get("msg")
            # With -stdout the events share the pod's log, and kubelet rotates
            # it, so the first lines can be gone: an event names the run too.
            if not run and isinstance(rec.get("loadgen"), dict):
                run = rec["loadgen"].get("run")
            if msg == "load generator starting":
                run = rec.get("run")
            elif msg == "step finished":
                start = start or parse_time(rec["start"])
                end = parse_time(rec["end"])
            elif msg == "step result":
                acked[rec["step"]] = rec["acked"]  # the summary repeats it
    if not run or start is None or not acked:
        raise SystemExit(f"{path}: not a named run's finished load generator log")
    return run, sum(acked.values()), start, end


class Tally:
    """Lines seen per sequence number, with every line's ID checked."""

    def __init__(self, run):
        self.run = run
        self.seen = {}
        self.bad_ids = 0
        self.lines = 0

    def add(self, seq, eid):
        self.lines += 1
        self.seen[seq] = self.seen.get(seq, 0) + 1
        if eid != event_id(self.run, seq):
            self.bad_ids += 1

    def report(self, expected):
        """expected: the sequence numbers that should be here."""
        missing = sorted(expected - self.seen.keys())
        extra = sorted(self.seen.keys() - expected)
        dupes = sum(n - 1 for n in self.seen.values() if n > 1)
        return {"expected": len(expected), "distinct": len(self.seen.keys() & expected), "missing": missing,
                "unexpected": extra, "duplicates": dupes, "duplicated_events": sum(1 for n in self.seen.values() if n > 1),
                "bad_ids": self.bad_ids, "lines": self.lines}


def export_hot(loki, team, source, run, start, end, tally):
    """Every line Loki holds for team/source and run between start and end."""
    query = (f'{{team="{team}", source="{source}"}} |= `"run":"{run}"` '
             f'| json seq="loadgen.seq", id="spillway.event_id", r="loadgen.run" | r="{run}" '
             '| line_format "{{.seq}} {{.id}}"')

    def window(a, b):  # nanoseconds, [a, b)
        q = urllib.parse.urlencode({"query": query, "start": a, "end": b - 1, "limit": LIMIT, "direction": "forward"})
        res = json.load(urllib.request.urlopen(f"{loki}/loki/api/v1/query_range?{q}", timeout=300))
        values = [v for stream in res["data"]["result"] for v in stream["values"]]
        if len(values) >= LIMIT:
            if b - a <= 1_000_000:  # a millisecond with 5,000 lines: give up rather than undercount
                raise SystemExit(f"more than {LIMIT} lines in one millisecond at {a}")
            mid = (a + b) // 2
            window(a, mid)
            window(mid, b)
            return
        for _, line in values:
            seq, eid = line.split()
            tally.add(int(seq), eid)

    step = 2 * 10**9  # about 2,000 lines at 1,000 events/s
    t, stop = int(start.timestamp()) * 10**9, int(end.timestamp() + 1) * 10**9
    while t < stop:
        window(t, min(t + step, stop))
        t += step


def read_cold(cold_dir, team, source, run, tally):
    """Every event of run from team's objects in a local copy of the bucket."""
    root = os.path.join(cold_dir, f"team={team}")
    objects = 0
    for dirpath, _, files in os.walk(root):
        for name in files:
            objects += 1
            with gzip.open(os.path.join(dirpath, name), "rt") as f:
                for line in f:
                    ev = json.loads(line)
                    tag = ev.get("loadgen") or {}
                    if tag.get("run") == run and (ev.get("spillway") or {}).get("source") == source:
                        tally.add(int(tag["seq"]), ev["spillway"]["event_id"])
    return objects


def ranges(nums, limit=8):
    """1, 2, 3, 7 -> '1–3, 7', at most limit ranges."""
    out, i = [], 0
    while i < len(nums) and len(out) < limit:
        j = i
        while j + 1 < len(nums) and nums[j + 1] == nums[j] + 1:
            j += 1
        out.append(f"{nums[i]:,}" if i == j else f"{nums[i]:,}–{nums[j]:,}")
        i = j + 1
    return ", ".join(out) + (", …" if i < len(nums) else "")


def row(sink, r):
    missing = f"**{len(r['missing']):,}** ({ranges(r['missing'])})" if r["missing"] else "0"
    return (f"| {sink} | {r['expected']:,} | {r['distinct']:,} | {missing} | {r['duplicates']:,} "
            f"({r['duplicated_events']:,} events) | {len(r['unexpected']):,} | {r['bad_ids']:,} |")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", required=True, help="the run's load generator log")
    ap.add_argument("--team", required=True)
    ap.add_argument("--source", required=True, help="the team's source the run's events came through")
    ap.add_argument("--cold-dir", help="local copy of the cold bucket; omit to skip cold")
    ap.add_argument("--drops", action="store_true", help="the team samples out debug and has a budget")
    ap.add_argument("--loki", default="http://localhost:3100")
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--end", help="end of the range searched and counted, RFC 3339 (default: now)")
    ap.add_argument("--json", help="also write the full result (every missing sequence number) here")
    args = ap.parse_args()

    run, acked, start, gen_end = read_run(args.log)
    end = parse_time(args.end) if args.end else dt.datetime.now(dt.timezone.utc)
    produced = set(range(1, acked + 1))
    debug = {s for s in produced if s % 4 == 0}

    hot = Tally(run)
    export_hot(args.loki, args.team, args.source, run, start - dt.timedelta(seconds=5), end, hot)
    results = {"run": run, "team": args.team, "source": args.source, "acked": acked}
    results["hot"] = hot.report(produced - debug if args.drops else produced)
    if args.drops:
        results["debug_in_hot"] = len(hot.seen.keys() & debug)
    if args.cold_dir:
        cold = Tally(run)
        results["cold_objects"] = read_cold(args.cold_dir, args.team, args.source, run, cold)
        results["cold"] = cold.report(produced)

    print(f"#### Team `{args.team}`, source `{args.source}`: run `{run}`, {acked:,} events acknowledged "
          f"({start:%H:%M:%S}–{gen_end:%H:%M:%S} UTC)\n")
    print("| Sink | Expected | Arrived (distinct) | Missing | Duplicate lines | Unexpected | Wrong IDs |")
    print("|---|---|---|---|---|---|---|")
    print(row("hot (Loki)" + (", debug excluded" if args.drops else ""), results["hot"]))
    if args.cold_dir:
        print(row(f"cold (S3, {results['cold_objects']} objects)", results["cold"]))
    if args.drops:
        p = Prom(args.prometheus)
        c0 = start - dt.timedelta(seconds=30)
        sampled = counter_total(p, "vector_component_discarded_events_total", f'{AGG},component_id="{args.team}_sample"', c0, end)
        throttled = counter_total(p, "vector_component_discarded_events_total", f'{AGG},component_id="{args.team}_budget"', c0, end)
        h = results["hot"]
        results.update(sampled=sampled, throttled=throttled)
        print(f"\nDrops, from Vector's counters: **{sampled:,.0f} sampled** (debug: {len(debug):,} produced, "
              f"{results['debug_in_hot']:,} in hot) and **{throttled:,.0f} throttled**. "
              f"Produced {acked:,} = {h['distinct']:,} distinct in hot + {sampled:,.0f} sampled + {throttled:,.0f} throttled "
              f"= {h['distinct'] + sampled + throttled:,.0f} (**{h['distinct'] + sampled + throttled - acked:+,.0f}**). "
              "The counters include duplicates that were dropped again, and miss a killed pod's last increments.")
    print()
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f)


if __name__ == "__main__":
    main()
