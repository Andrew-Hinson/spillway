# M1 baseline: volume and latency

The M1 pipeline before any policy features: every Wikimedia recentchange event goes
feeder → Kafka → Vector aggregator → Loki, with no sampling, dedup, redaction or
routing. M3's volume reduction and the M4 benchmarks are measured against these numbers.

## Setup

| | |
|---|---|
| Window | 2026-10-01 22:00–22:15 UTC (15 min), live traffic, no restarts during the window |
| Source | Wikimedia EventStreams `recentchange`, all wikis (canary events excluded) |
| Pipeline | 1 feeder, Kafka 4.3.1 (1 broker, 3 partitions), Vector 0.58.0 aggregator (1 replica, 1 GiB disk buffer), Loki 3.7.8 (monolithic) |
| Cluster | kind v0.33.0, Kubernetes v1.37.0, 1 control plane and 2 workers |
| Host | AMD Ryzen 7 7800X3D (8 cores / 16 threads), 61 GiB RAM, Docker 29.7.2, Linux 7.2.5 |

Wikimedia's edit rate follows the time of day across all wikis, so throughput here is
what the source sent in this window, not the pipeline's capacity. Sustained throughput
under stepped load is M4.1's job.

## Results

### Volume

| Measure | Window total | Rate |
|---|---|---|
| Events produced by the feeder | 41,724 | 46.4/s |
| Events consumed by the aggregator | 41,442 | 46.0/s |
| Events sent to Loki | 41,468 | 46.1/s |
| Bytes read from Kafka (raw payload) | 60.2 MiB | 68.5 KiB/s · 5.6 GiB/day |
| **Event bytes in at the aggregator** | 62.5 MiB | 71.2 KiB/s · 5.9 GiB/day |
| **Event bytes into the Loki sink (hot)** | 64.3 MiB | 73.2 KiB/s · 6.0 GiB/day |
| Bytes sent to Loki (compressed, on the wire) | 15.5 MiB | 17.6 KiB/s · 1.4 GiB/day |

- **Average event:** 1,524 B as read from Kafka.
- **Hot-sink volume is 102.9% of volume in.** This is the baseline M3's reduction is
  measured against: event bytes into the Loki sink ÷ event bytes in at the aggregator.
  It's above 100% because the aggregator adds `spillway.sink_at` and
  `spillway.pipeline_latency_ms` to every event.
- **Compression:** Loki's push protocol (snappy) sends about 24% of the event bytes.
- **Event counts differ slightly between stages** (up to 0.7%). That's timing at the
  window edges: counters are scraped every 15s at slightly different moments and
  `increase()` extrapolates to the window boundaries. It isn't loss. M1.4 matched
  every Kafka record to a Loki line by `event_id` across a Loki outage and an
  aggregator restart, with none missing or duplicated.

### Latency (feeder produce → aggregator pre-sink)

| p50 | p95 | p99 | Mean | Events measured |
|---|---|---|---|---|
| 17.1 ms | 28.6 ms | 29.7 ms | 15.3 ms | 41,692 |

Latency is `spillway.sink_at − spillway.produced_at`: from the feeder handing the event
to its Kafka producer, to the aggregator's last transform before the Loki sink. It
includes the producer's 20 ms linger, Kafka, and Vector's consume and transform. It
doesn't include the sink's batching (up to 1 s) or Loki's write.

Quantiles are interpolated within histogram buckets (…, 15, 20, 30, 50 ms, …). p95 and
p99 both fall in the 20–30 ms bucket, so they mean "under 30 ms" rather than a more
precise value.

### Errors

| Vector errors | Vector discarded events | Feeder produce errors | Feeder reconnects |
|---|---|---|---|
| 0 | 0 | 0 | 2 |

The two reconnects are Wikimedia closing healthy connections. The feeder resumed from
the last event ID both times.

## How to reproduce

```bash
make up                    # wait for 15+ minutes of steady state after the last restart
make baseline              # or: make baseline WINDOW=1h
```

`make baseline` port-forwards Prometheus and runs `bench/baseline.py`, which prints this
report's tables as Markdown. All values are `increase()` over the window ending now (or
`--end`), using these series:

| Measure | Prometheus series |
|---|---|
| Events produced | `spillway_feeder_events_produced_total` |
| Events consumed / sent to Loki | `vector_component_received_events_total{component_id="wikimedia"}` / `vector_component_sent_events_total{component_id="loki"}` |
| Bytes read from Kafka | `vector_component_received_bytes_total{component_id="wikimedia"}` |
| Event bytes in / into Loki sink | `vector_component_sent_event_bytes_total{component_id="wikimedia"}` / `vector_component_received_event_bytes_total{component_id="loki"}` |
| Bytes sent to Loki | `vector_component_sent_bytes_total{component_id="loki"}` |
| Latency | `spillway_pipeline_latency_milliseconds` histogram |

The **Spillway pipeline** dashboard in Grafana shows the same series live.
