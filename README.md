# Spillway

Spillway is a Kubernetes log platform that will turn a short YAML spec per team into a running, tested [Vector](https://vector.dev) pipeline, with collection, PII redaction, sampling, per-team budgets, and hot and cold routing. It runs on real public event streams, and every number in this README comes from a measurement in [`docs/results`](docs/results).

## Status

**M1 (real traffic, end to end) is built.** Live Wikimedia edits and the cluster's own pod logs flow through Kafka and Vector into Loki and can be queried in Grafana, and a baseline has been recorded.

**M2 (the operator) is under way.** The `LogPipeline` CRD and a scaffolded operator are in place, and the API server rejects malformed specs at apply time. A renderer turns specs into a complete Vector aggregator config, but the operator doesn't use it yet. Wiring it into the operator, validating the config and rolling it out come next. Policy features and benchmarks (M3–M4) haven't started. See the [build plan](docs/plan.md).

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
- **Operator** ([`cmd/operator`](cmd/operator), [`api/v1alpha1`](api/v1alpha1)): watches `LogPipeline` resources, one per team. For now it only logs what it sees.
- **Renderer** ([`internal/render`](internal/render)): turns LogPipelines into one aggregator config. Each team's events are tagged and redacted, then split. Cold storage gets the complete redacted stream. The hot path is sampled and rate-capped before Loki. Sinks are shared, so adding a team doesn't add disk buffers. The rendered config for each example is checked in under [`internal/render/testdata`](internal/render/testdata) and validated by Vector in CI.
- **Self-monitoring**: Prometheus scrapes the feeder and every Vector instance. The *Spillway pipeline* dashboard ([`dashboards/pipeline.json`](dashboards/pipeline.json)) shows throughput, latency, consumer lag, buffer size and errors.

## Quickstart

You need Docker, `make` and `curl`. Go 1.27 is needed only for `make test`, and `python3` only for `make baseline`. The versions of kind, kubectl, helm and the Go tooling (golangci-lint, controller-gen, setup-envtest) that the repo uses are downloaded into `./bin` automatically.

```bash
make up            # kind cluster + Kafka, Loki, Prometheus, Grafana + feeder, aggregator, agents, operator (~5 min)
make grafana-ui    # http://localhost:3000, prints the admin password
make down          # delete the cluster; nothing is left behind
```

The cluster's kubeconfig is written to `./.kubeconfig`, so your `~/.kube/config` is never touched. If you use [mise](https://mise.jdx.dev), the repo's [`mise.toml`](mise.toml) sets Go, `PATH` and `KUBECONFIG` for you.

In Grafana's Explore view, try `{feeder="wikimedia"}` for edits or `{namespace="kafka"}` for pod logs.

## LogPipeline

Each team describes its pipeline in one resource. See [`examples/`](examples) for more.

```yaml
apiVersion: spillway.dev/v1alpha1
kind: LogPipeline
metadata:
  name: payments
spec:
  team: payments
  sources:
    - name: services
      kubernetes: {namespaces: [payments]}   # or kafka: {topic: ...}
  redaction: {patterns: [ssn, email, phone, memberId]}
  sampling: {keepPercent: {debug: 0, info: 25}}
  budget: {maxEventsPerSec: 500}
  routing: {hot: true, cold: true}
```

The schema is enforced by the API server, so `kubectl apply` rejects a malformed spec and reports every error at once:

```
The LogPipeline "broken" is invalid:
* spec.sampling.keepPercent.info: Invalid value: 150: ... should be less than or equal to 100
* spec.team: Invalid value: "Payments": ... should match '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
* spec.sources[0]: Invalid value: exactly one of kafka or kubernetes must be set
* spec.routing: Invalid value: at least one of hot or cold must be enabled
```

Only the schema is enforced in the cluster so far. The renderer handles every field, but the operator doesn't apply its output yet, so specs have no effect on the running pipeline until M2.3.

## Development

| Command | What it does |
|---|---|
| `make test` | Go unit tests with the race detector, plus CRD validation tests against a real API server ([envtest](https://book.kubebuilder.io/reference/envtest)) |
| `make generate` | Regenerate the CRD, RBAC and deepcopy code from the Go types. CI fails if they're out of date |
| `make lint` | golangci-lint |
| `make vector-check` | `vector validate` on the agent and aggregator configs and on every rendered example, plus the `vector test` unit tests in [`vector/tests`](vector/tests) |
| `make baseline` | Volume and latency over the last 15 minutes, as Markdown |
| `make help` | Every target |

CI runs `lint`, `generate`, `test` and `vector-check` on every PR.

## Baseline (M1)

Measured on live traffic over 15 minutes, with no sampling or redaction. The full report is in [`docs/results/baseline.md`](docs/results/baseline.md).

| Events/s | Event bytes into Loki | p50 latency | p99 latency | Errors / drops |
|---|---|---|---|---|
| 46 | 6.0 GiB/day | 17 ms | < 30 ms | 0 / 0 |

Latency is measured from the feeder handing an event to Kafka until the aggregator's last step before the Loki sink. Throughput is whatever Wikimedia sent during the window, not the pipeline's capacity. Measuring capacity under stepped load is part of M4.

## Layout

```
api/v1alpha1/        LogPipeline API types (the CRD is generated from these)
cmd/operator/        operator entrypoint and Dockerfile
internal/controller/ LogPipeline reconciler
internal/render/     LogPipeline → Vector config, with golden files
internal/redact/     PII patterns as VRL redact() filters
examples/            example LogPipeline specs
feeders/wikimedia/   Go SSE → Kafka feeder
vector/              agent and aggregator configs, Helm values, unit tests
deploy/              kind cluster, platform Helm values, feeder and operator manifests (generated CRD and RBAC)
dashboards/          Grafana dashboards
bench/               baseline measurement script
docs/                build plan and results
```
