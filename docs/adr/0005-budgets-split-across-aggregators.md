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
3. **Pod logs travel through Kafka.** An even split only delivers the whole budget if a team's events reach the aggregators evenly. Kafka partitions do that for feeder topics, but the agents sent pod logs straight to the aggregators' Service over gRPC, and each agent's long-lived connection pinned it to one pod. A pod-log team then got 5.0/s against 10. The agents now write to a `spillway.pods` topic with no key, so batches spread over its 6 partitions (6 divides evenly among 1, 2, 3 or 6 aggregators). The aggregators read it as one consumer group, like the feeder topics.

## Consequences

Good:
- **The budget is a real cap, and teams get all of it.** The team-wide rate never exceeds `maxEventsPerSec` while the budget is at least the number of pods. Measured against 10/s: 9.8/s for a Kafka team, and 9.8/s for a pod-log team (5.0/s before pod logs moved to Kafka).
- **No loss to batching:** each pod passed exactly its share (5.00/s).
- **Drops stay counted.** Over-budget events are intentional discards on `<team>_budget`, tagged with the team. They still reach cold storage if the team routes there.

Bad:
- **Uneven partition counts under-deliver.** A topic whose partitions don't divide evenly among the aggregators (the 3-partition Wikimedia topic on two) splits a team's load 2:1, so a team offered between 1 and 1.5 times its budget gets somewhat less than all of it.
- **Kafka now carries pod logs.** If Kafka is down, pod logs wait in each agent's 256 MiB disk buffer instead of reaching the aggregators, the same as feeder events. In exchange they gain a day of retention, failover by consumer-group rebalance, and the same delivery path as everything else.
- **Bursts:** after 10 s idle, a team can send 10 s of its share at once.
- **Rounding:** 25/s on two pods allows 24/s.
- **Scaling the aggregators changes every budgeted team's config.**

Alternatives considered:
- **Give every pod the whole budget:** never under-delivers, but a team can get up to replicas × its budget. The point of a budget is a ceiling on cost, so exceeding it is the worse failure.
- **One agent sink per aggregator pod, with a hash route:** spreads pod logs, but the agents' config would carry the replica count, and logs for an aggregator that's down would wait for it rather than fail over.
- **Size each pod's share from observed load:** the operator already scrapes the pods, but every adjustment is a config rollout (2.5–4 min and a restart per pod), so shares would chase load and restart aggregators to do it.
- **Rate limits in Loki (per-tenant ingestion limits):** Loki rejects over-limit pushes, and Vector retries them, which turns a budget into backpressure on every team sharing the sink rather than counted drops for one.
- **Keep the 1 s window:** the measured 6–8/s against 10 shows it doesn't deliver the budget even on one pod.

Revisit if a team's load can't be spread evenly, for example a single-partition topic, or if aggregators are scaled to a count that 6 partitions don't divide.
