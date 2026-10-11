# M3 gate: policy features, end to end

The M3 gate from the [build plan](../plan.md), run in one pass against a fresh `make up` with `make m3-gate` ([`bench/m3_gate.sh`](../../bench/m3_gate.sh)):

> Two teams on two feeders, each with redaction, sampling, a budget, and hot and cold routing; injected fixtures produce zero leaks in the hot sink; volume reduction measured against the M1 baseline.

**Result: passed.**

## Setup

| | |
|---|---|
| Run | 2026-10-11 18:15–18:34 UTC, commit `5a55bf0` plus this change |
| Stack | Fresh `make up`: kind with Kubernetes v1.37.0 (1 control plane, 2 workers), Kafka, Loki, Silo (cold storage), Vector 0.58.0 (2 aggregators, 3 agents), both feeders, the operator |
| Traffic | Live Wikimedia `recentchange` (~31 events/s) and GitHub public events (unauthenticated, ~2 events/s, paced) |
| Fixtures | `make fixtures RATE=4`: 4 fictional-PII records/s into each of `wikimedia.recentchange`, `github.events` and the injector's pod logs |
| Window | 15 minutes after both teams were Ready, plus 75 s for the last cold batches |

## Results

| Gate criterion | Result |
|---|---|
| Two teams on two feeders | `content` reads `wikimedia.recentchange`; `oss` reads `github.events` |
| …each with redaction, sampling, a budget, hot and cold | Both: every PII pattern (the default), sampling (`content`: categorize 10%, log 50%; `oss`: pushevent 20%), a budget (100/s and 4/s), hot and cold. **Both Ready 131 s after `kubectl apply`**, through the canary |
| Zero leaks in the hot sink | `make leak-check`: **9,000 fixtures delivered, 0 values leaked**: 3,597 via `content`, 1,807 via `oss` and 3,596 via unclaimed pod logs |
| …and in cold storage | `make cold-check`: 41,168 events in 68 objects, 8,717 of them fixtures, **0 values leaked**, every object keyed `team=…/date=…/` and gzipped |
| Volume reduction against M1 | Hot / in: **72.4%** for `content` on Wikimedia, against **102.9%** in the M1 baseline. Across everything the aggregators read: **63.5%**, so **38.3% less hot volume** for the same input |

### Volume against the M1 baseline

The M1 baseline defined the metric as event bytes into the hot sink ÷ event bytes in at the aggregator, and measured 102.9% with no policies (the pipeline adds a timestamp and a latency field). `make volume`, over the gate's window (ending 18:33 UTC):

| | Event bytes in | Event bytes to hot | Hot / in | M1 baseline |
|---|---:|---:|---:|---:|
| team `content` (Wikimedia) | 39.6 MB | 28.7 MB | 72.4% | 102.9% |
| team `oss` (GitHub) | 10.6 MB | 6.0 MB | 57.2% | 102.9% |
| **everything** (sources: `agents`, `content_src_wikimedia`, `oss_src_events`; sinks: `hot_loki`, `loki_pods`) | 58.3 MB | 37.0 MB | **63.5%** | 102.9% |

The gate's own log printed 56.1% for "everything". That run counted the agents' `pods` source as well as the aggregators' `agents` source, the same pod logs twice. `bench/volume.py` now counts only sources on the aggregators, and the table above is the corrected run over the same window. The per-team rows didn't change.

Cold storage still holds everything: 0.82 GB/day gzipped, against 4.8 GB/day of events.

### Policies per team

| Team | Events in | Sampling drops | Budget drops | To the hot sink | Kept |
|---|---:|---:|---:|---:|---:|
| `content` | 28,300 | 8,286 | 0 | 20,015 | 70.7% |
| `oss` | 5,607 | 538 | 2,342 | 2,727 | 48.6% |

- `content` in Loki: `edit` 18,010 and `new` 551 kept whole, `categorize` 898 (at 10%), `log` 595 (at 50%).
- `oss` in Loki: `PushEvent` 92 (at 20%). All other types were kept, but trimmed by the budget.
- **`oss` went over its budget because of the fixtures:** 4 fixtures/s on top of ~2 real events/s is about 6.2/s against 4/s. That's why only 1,807 of its 3,600 fixtures reached the hot sink, and why `IssueCommentEvent`, the fixtures' type, leads its Loki counts. Every dropped fixture was counted as a budget drop and kept in cold storage.

### Cost

`make cost-report` at the window's average rate, with the default prices ($0.50/GB hot, $0.023/GB-month cold, 12 months): `content` $46.53/month, `oss` $9.98/month, unclaimed pod logs $3.32/month. The total is **$59.83/month**, 89% of it hot.

## Notes

- **Two problems found while building M3.7 were fixed before this run.** A budget filtered out whole GitHub event types until the feeder paced its polls. The canary rolled back a healthy cold sink until stalls were judged at a checkpoint (ADR 0002 amendment). See [m3.7-github-feeder.md](m3.7-github-feeder.md).
- **Per-milestone detail** is in the results for [M3.2 redaction](m3.2-redaction.md), [M3.3 sampling](m3.3-sampling.md), [M3.4 budgets](m3.4-budgets.md), [M3.5 cold path](m3.5-cold-path.md), [M3.6 cost](m3.6-cost.md) and [M3.7 the GitHub feeder](m3.7-github-feeder.md).
