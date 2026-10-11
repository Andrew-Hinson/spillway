#!/usr/bin/env python3
"""Check what's in cold storage and print a Markdown report.

Reads a local copy of the cold bucket (`make cold-check` syncs it with
aws-cli) and checks every object: its key is team=<team>/date=<YYYY-MM-DD>/,
it's gzip, every line is a JSON event whose spillway.team and log date match
the key, and no injected fixture value survived redaction. Counts events and
bytes per team and day.

Exits 1 on a leak, a mismatched or undecodable object, or an empty bucket.

    bench/coldcheck.py DIR    (`make cold-check` port-forwards the store and syncs the bucket)
"""
import collections
import gzip
import json
import os
import re
import sys

# The fixture values, as in bench/leakcheck.py.
LEAKS = {
    "ssn": re.compile(r"123-45-\d{4}"),
    "email": re.compile(r"fixtures\.spillway\.test"),
    "phone": re.compile(r"555-01\d\d"),
    "memberId": re.compile(r"MBR-9\d{7}"),
}
KEY = re.compile(r"^team=(?P<team>[a-z0-9-]+)/date=(?P<date>\d{4}-\d{2}-\d{2})/[^/]+\.log\.gz$")


def main():
    root = sys.argv[1]
    stats = collections.defaultdict(lambda: collections.Counter())
    problems, leaks, samples = [], collections.Counter(), []
    for dirpath, _, files in os.walk(root):
        for f in files:
            path = os.path.join(dirpath, f)
            key = os.path.relpath(path, root)
            m = KEY.match(key)
            if not m:
                problems.append(f"unexpected key `{key}`")
                continue
            s = stats[(m["team"], m["date"])]
            s["objects"] += 1
            s["stored bytes"] += os.path.getsize(path)
            try:
                with gzip.open(path, "rt") as fh:
                    lines = fh.read().splitlines()
            except (OSError, EOFError) as e:
                problems.append(f"`{key}` isn't gzip: {e}")
                continue
            for line in lines:
                s["events"] += 1
                s["raw bytes"] += len(line) + 1
                try:
                    e = json.loads(line)
                except ValueError:
                    problems.append(f"`{key}` has a line that isn't JSON")
                    continue
                sp = e.get("spillway") or {}
                if sp.get("team") != m["team"]:
                    problems.append(f"`{key}` holds an event of team {sp.get('team')!r}")
                if not str(e.get("_timestamp", "")).startswith(m["date"]):
                    problems.append(f"`{key}` holds an event logged at {e.get('_timestamp')!r}")
                if "fixture" in sp:
                    s["fixtures"] += 1
                for pattern, rx in LEAKS.items():
                    if rx.search(line):
                        leaks[(m["team"], pattern)] += 1
                        if len(samples) < 3:
                            samples.append(line[:300])

    print("Cold storage contents.\n")
    print("| Team | Date | Objects | Events | Fixtures | Event bytes | Stored (gzip) | Ratio |")
    print("|---|---|---:|---:|---:|---:|---:|---:|")
    for (team, date), s in sorted(stats.items()):
        ratio = s["stored bytes"] / s["raw bytes"] if s["raw bytes"] else 0
        print(f"| `{team}` | {date} | {s['objects']:,} | {s['events']:,} | {s['fixtures']:,} | "
              f"{s['raw bytes']:,} | {s['stored bytes']:,} | {ratio:.1%} |")
    for p in sorted(set(problems))[:20]:
        print(f"\nProblem: {p}")
    for (team, pattern), n in sorted(leaks.items()):
        print(f"\nLeak: {n} {pattern} values in `{team}`")
    for line in samples:
        print(f"\nLeaked line: `{line}`")

    if leaks or problems:
        print(f"\nFAIL: {sum(leaks.values())} leaked values and {len(problems)} problems.")
        sys.exit(1)
    if not stats:
        print("\nFAIL: the bucket is empty.")
        sys.exit(1)
    events = sum(s["events"] for s in stats.values())
    fixtures = sum(s["fixtures"] for s in stats.values())
    print(f"\nPASS: {events:,} events in {sum(s['objects'] for s in stats.values()):,} objects, "
          f"{fixtures:,} of them fixtures, 0 values leaked.")


if __name__ == "__main__":
    main()
