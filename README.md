# Spillway

Spillway is a Kubernetes log platform that will turn a short YAML spec per team into a running, tested [Vector](https://vector.dev) pipeline, with collection, PII redaction, sampling, per-team budgets, and hot and cold routing. It runs on real public event streams, and every number in this README comes from a measurement in [`docs/results`](docs/results).

## Status

**M1 (real traffic, end to end) is built.** Live Wikimedia edits and the cluster's own pod logs flow through Kafka and Vector into Loki and can be queried in Grafana, and a baseline has been recorded. The operator, policy features, and benchmarks (M2–M4) are not started yet. See the [build plan](docs/plan.md).

## Architecture

```
Wikimedia EventStreams ──► wikimedia-feeder (Go) ──► Kafka (Strimzi) ──┐
                                                                        ├──► Vector aggregator ──► Loki ──► Grafana
pod logs on every node ──► Vector agent (DaemonSet) ────────────────────┘          │
                                                                                     └──► Prometheus (pipeline metrics)
```

- **Feeder** ([`feeders/wikimedia`](feeders/wikimedia)): reads the `recentchange` SSE stream, resumes from the last event ID after a disconnect, and stamps each event with a unique ID and a produce timestamp before writing it to Kafka.
- **Aggregator** ([`vector/aggregator`](vector/aggregator)): consumes from Kafka and stamps each event with a pre-sink time, so feeder-to-sink latency can be measured. It writes to Loki through a 1 GiB disk buffer and commits Kafka offsets only once events are in that buffer.
- **Agent** ([`vector/agent`](vector/agent)): runs on every node, tails pod logs, and forwards them to the aggregator. The aggregator writes them to Loki through a separate sink, labelled `namespace`, `pod` and `container`.
- **Self-monitoring**: Prometheus scrapes the feeder and every Vector instance. The *Spillway pipeline* dashboard ([`dashboards/pipeline.json`](dashboards/pipeline.json)) shows throughput, latency, consumer lag, buffer size and errors.

## Quickstart

You need Docker, `make` and `curl`. Go 1.27 is needed only for `make test`, and `python3` only for `make baseline`. The kind, kubectl, helm and golangci-lint versions the repo uses are downloaded into `./bin` automatically.

```bash
make up            # kind cluster + Kafka, Loki, Prometheus, Grafana + feeder, aggregator, agents (~5 min)
make grafana-ui    # http://localhost:3000, prints the admin password
make down          # delete the cluster; nothing is left behind
```

The cluster's kubeconfig is written to `./.kubeconfig`, so your `~/.kube/config` is never touched. If you use [mise](https://mise.jdx.dev), the repo's [`mise.toml`](mise.toml) sets Go, `PATH` and `KUBECONFIG` for you.

In Grafana's Explore view, try `{feeder="wikimedia"}` for edits or `{namespace="kafka"}` for pod logs.

## Development

| Command | What it does |
|---|---|
| `make test` | Go unit tests, with the race detector |
| `make lint` | golangci-lint |
| `make vector-check` | `vector validate` on both configs, plus the `vector test` unit tests in [`vector/tests`](vector/tests) |
| `make baseline` | Volume and latency over the last 15 minutes, as Markdown |
| `make help` | Every target |

CI runs `lint`, `test` and `vector-check` on every PR.

## Baseline (M1)

Measured on live traffic over 15 minutes, with no sampling or redaction. The full report is in [`docs/results/baseline.md`](docs/results/baseline.md).

| Events/s | Event bytes into Loki | p50 latency | p99 latency | Errors / drops |
|---|---|---|---|---|
| 46 | 6.0 GiB/day | 17 ms | < 30 ms | 0 / 0 |

Latency is measured from the feeder handing an event to Kafka until the aggregator's last step before the Loki sink. Throughput is whatever Wikimedia sent during the window, not the pipeline's capacity. Measuring capacity under stepped load is part of M4.

## Layout

```
feeders/wikimedia/   Go SSE → Kafka feeder
vector/              agent and aggregator configs, Helm values, unit tests
deploy/              kind cluster, platform Helm values, feeder manifests
dashboards/          Grafana dashboards
bench/               baseline measurement script
docs/                build plan and results
```
