# ADR 0005: Budgets are split evenly across the aggregators

Status: proposed · 2026-10-10 · M3.4

## Context

A team's `budget.maxEventsPerSec` became a Vector `throttle` with that threshold over a 1 s window. Measured in kind with a budget of 10/s ([results](../results/m3.4-budgets.md)), it was wrong in both directions:

- **Over:** every aggregator runs its own throttle, with no shared state, and there are two. A Kafka team whose partitions spread over both got 17.5/s.
- **Under:** events arrive in batches, and a throttle releases at most one window's threshold at once, so gaps longer than the window waste budget. A team whose pod logs reached one aggregator got 7.9/s out of 40/s offered.

Vector has no distributed rate limiter, and a team's load isn't spread predictably. Kafka partitions are split by the consumer group, roughly evenly. Pod logs follow the agents' long-lived gRPC connections, which can all point at one aggregator.

## Decision

1. **Each aggregator gets `floor(maxEventsPerSec / replicas)`, and at least 1.** The operator renders with the StatefulSet's replica count, so scaling it rolls out new shares through the usual canary. `spillwayctl` assumes 2, the chart's default.
2. **The window is 10 s**, with the threshold scaled to match (10 × the share). That absorbs batched arrival and keeps the same average.

## Consequences

Good:
- **The budget is a real cap.** The team-wide rate never exceeds `maxEventsPerSec` while the budget is at least the number of pods. Measured: 9.8/s against 10.
- **No loss to batching:** each pod passed exactly its share (5.00/s).
- **Drops stay counted.** Over-budget events are intentional discards on `<team>_budget`, tagged with the team. They still reach cold storage if the team routes there.

Bad:
- **Uneven load under-delivers.** A team whose events all reach one aggregator gets 1/replicas of its budget. Measured: 5.0/s against 10, for pod logs.
- **Bursts:** after 10 s idle, a team can send 10 s of its share at once.
- **Rounding:** 25/s on two pods allows 24/s.
- **Scaling the aggregators changes every budgeted team's config.**

Alternatives considered:
- **Give every pod the whole budget:** never under-delivers, but a team can get up to replicas × its budget. The point of a budget is a ceiling on cost, so exceeding it is the worse failure.
- **Size each pod's share from observed load:** the operator already scrapes the pods, but every adjustment is a config rollout (2.5–4 min and a restart per pod), so shares would chase load and restart aggregators to do it.
- **Rate limits in Loki (per-tenant ingestion limits):** Loki rejects over-limit pushes, and Vector retries them, which turns a budget into backpressure on every team sharing the sink rather than counted drops for one.
- **Keep the 1 s window:** the measured 6–8/s against 10 shows it doesn't deliver the budget even on one pod.

Revisit if pod-log teams need their full budget. Spreading agent connections across aggregators (a sink per aggregator pod, or reconnecting periodically) would make an even split fit pod logs as well as it fits Kafka.
