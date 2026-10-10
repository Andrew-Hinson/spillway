# Spillway

Spillway is a Kubernetes log platform that will turn a short YAML spec per team into a running, tested [Vector](https://vector.dev) pipeline, with collection, PII redaction, sampling, per-team budgets, and hot and cold routing. It runs on real public event streams, and every number in this README comes from a measurement in [`docs/results`](docs/results).

## Status

**M1 (real traffic, end to end) is built.** Live Wikimedia edits and the cluster's own pod logs flow through Kafka and Vector into Loki and can be queried in Grafana, and a baseline has been recorded.

**M2 (the operator) is complete.** Applying a `LogPipeline` changes what runs:
- **Checked:** the operator renders every pipeline into the aggregator's config and checks it with `vector validate`.
- **Canaried:** new config goes to one aggregator first, and is promoted only if its error rate and delivery hold.
- **Reported:** each pipeline is marked Ready, or Invalid or CanaryFailed with the reason.

The [M2 gate run](docs/results/m2-gate.md) shows a team's first event queryable 5 s after `kubectl apply`, and three of three bad configs blocked: one at apply time, one Invalid, and one rolled back by the canary. Config changes go out as canaries ([ADR 0002](docs/adr/0002-canary-rollout-with-statefulset-partition.md)), and `spillwayctl` runs the same checks without a cluster. **M3 (policy features) is under way.** Redaction is on by default, for teams and for unclaimed data ([ADR 0003](docs/adr/0003-redaction-on-by-default.md)). A fixture injector mixes known, fictional PII into the live streams, and `make leak-check` searches the hot sink for it: [0 of 5,054 fixtures leaked](docs/results/m3.2-redaction.md), and the patterns change 0.015% of live Wikimedia events. Sampling keeps each level's share as specified, on Kafka events and on pod logs, which are now parsed and given a normalized level ([ADR 0004](docs/adr/0004-parse-pod-logs-and-trust-only-the-stamp.md)). Replayed events are dropped by event ID, and every drop is counted per team: in kind, 300 of 300 replays removed and info logs kept at 24% against a spec of 25% ([results](docs/results/m3.3-sampling.md)). See the [build plan](docs/plan.md).

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
- **Operator** ([`cmd/operator`](cmd/operator), [`internal/controller`](internal/controller)): watches `LogPipeline` resources, one per team. On every change it renders all of them, validates the result with the Vector binary bundled in its image, and writes it to the aggregator's ConfigMap ([ADR 0001](docs/adr/0001-validate-in-the-operator-image.md)). It then canaries the new config on one of the two aggregator pods for 2 minutes. It compares that pod's errors and sink delivery with the other pod's, then promotes or rolls back, and sets each pipeline's Ready condition. A change whose canary fails is quarantined: that pipeline keeps running its last good spec, and other teams' changes still roll out. If several changes fail together, each is retried alone, and only the ones that fail alone stay quarantined. A pipeline Vector rejects is marked Invalid with Vector's error. If the pipelines only fail together, nothing is applied and the aggregator keeps its last valid config. If two pipelines conflict, the older one keeps the team or namespace and the newer one is marked Invalid. Manual edits to the ConfigMap are reverted.
- **Renderer** ([`internal/render`](internal/render)): turns LogPipelines into one aggregator config. Each team's events are tagged, deduplicated by event ID and redacted (every pattern unless the spec says otherwise), then split. Pod logs are parsed first: JSON lines become fields, and every line gets a normalized `level`. Cold storage gets the complete redacted stream. The hot path is sampled and rate-capped before Loki. Sinks are shared, so adding a team doesn't add disk buffers. Data no team claims takes the platform defaults: the M1 paths above, with every PII pattern masked. A topic or namespace a team claims leaves those paths. With no LogPipelines the render is the platform config, and [`vector/aggregator/vector.yaml`](vector/aggregator/vector.yaml) is generated from it. The rendered config for each example is checked in under [`internal/render/testdata`](internal/render/testdata) and validated by Vector in CI.
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

To onboard a team, `kubectl apply -f examples/edits.yaml` (export `KUBECONFIG` first, as above). Within seconds, `{team="edits"}` shows the Wikimedia stream with its sampling and budget applied, and `kubectl get lp` tracks it through the canary to Ready, about 2 minutes later.

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
  redaction: {patterns: [ssn, email, phone, memberId]}   # the default; {disabled: true} opts out
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

Once a spec is applied, `kubectl get lp` shows whether it's running:

```
NAME        TEAM        READY   REASON      MESSAGE
wiki        wiki        True    RolledOut   the aggregator is running config that includes this pipeline
wiki-copy   wiki        False   Invalid     team "wiki" is claimed by both default/wiki and default/wiki-copy
content     content     False   CanaryFailed   generation 1 config 8232df0ccaed7f8c was rolled back: canary pod vector-aggregator-1 logged 8 errors in 15s against 0 on stable pod vector-aggregator-0. Change the spec to try again
payments    payments    False   Invalid     default/payments routes to cold storage, but no cold storage is configured
```

### Checking specs without a cluster

`spillwayctl` runs the operator's checks locally, for a quick loop while writing a spec or as a CI step:
1. Each spec is checked against the CRD schema, using the API server's own validation libraries, so you see the same errors `kubectl apply` would give.
2. All specs are rendered together, which catches conflicts such as a team claimed twice.
3. The rendered config goes through `vector validate`.
4. The unit tests generated for that config run with `vector test`. The renderer writes tests for every transform it emits, with assertions on behaviour. For example: each PII pattern is masked in nested fields while Spillway's own metadata is kept; a 0% level is dropped; a claimed namespace leaves the platform path.

```bash
make spillwayctl
bin/spillwayctl validate --vector-bin bin/vector examples/        # exit 1 if anything is invalid
bin/spillwayctl render examples/minimal.yaml > aggregator.yaml   # the config the operator would apply
```

```
examples/minimal.yaml (search): ok
broken.yaml (broken): INVALID
  * spec.sources[0]: Invalid value: exactly one of kafka or kubernetes must be set
render: ok (1 pipeline)
```

Cold storage (MinIO) arrives in M3. Until then, pipelines that route to cold storage are marked Invalid instead of being rolled out with a sink that has nowhere to write.

## Development

| Command | What it does |
|---|---|
| `make test` | Go unit tests with the race detector, CRD and operator tests against a real API server ([envtest](https://book.kubebuilder.io/reference/envtest)), and `vector validate` on rendered config, including random schema-valid specs |
| `make generate` | Regenerate the CRD, RBAC and deepcopy code from the Go types. CI fails if they're out of date |
| `make lint` | golangci-lint |
| `make vector-check` | `vector validate` and the generated `vector test` suite on the aggregator config and every rendered example, plus `vector validate` on the agent config |
| `make baseline` | Volume and latency over the last 15 minutes, as Markdown |
| `make fixtures` | Inject known, fictional PII into the live streams (`RATE=` per second, default 1): recentchange-shaped events into the Wikimedia topic, and JSON log lines in the `fixtures` namespace. Each is tagged `spillway.fixture: {id, pattern}`, so it can be counted in any sink |
| `make leak-check` | Search every hot-sink stream for fixture values over `WINDOW` (default 15m). Fails on any leak, or if no fixtures arrived |
| `make redaction-fp` | Capture `DURATION` seconds of the live Wikimedia stream and report what each redaction pattern masks |
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
cmd/spillwayctl/     CLI: validate and render specs without a cluster
internal/controller/ LogPipeline reconciler
internal/render/     LogPipeline → Vector config, with golden files
internal/redact/     PII patterns as VRL redact() filters, and the positive/negative corpus
internal/validate/   vector validate gate for rendered config
internal/schema/     offline CRD schema validation (same libraries as the API server)
examples/            example LogPipeline specs
feeders/wikimedia/   Go SSE → Kafka feeder
feeders/fixtures/    fictional-PII fixture injector (Kafka and pod logs)
vector/              agent and aggregator configs, Helm values, unit tests
deploy/              kind cluster, platform Helm values, feeder and operator manifests (generated CRD and RBAC)
dashboards/          Grafana dashboards
bench/               measurements: baseline, M2 gate, redaction false positives, fixture leak check, per-team drops
docs/                build plan, results and ADRs
```
