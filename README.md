# Spillway

A Kubernetes log platform. Each team writes one short `LogPipeline` spec, and Spillway turns it into a tested, canaried [Vector](https://vector.dev) pipeline with collection, PII redaction, sampling, per-team budgets, and hot (Loki) and cold (S3) routing. It runs on live public data (Wikimedia edits, GitHub events), and every number below is a measurement in [`docs/results`](docs/results).

## Results

| | |
|---|---|
| Onboarding a team | First event queryable **5 s** after `kubectl apply`; on every aggregator in ~2 min ([M2 gate](docs/results/m2-gate.md)) |
| Bad configs | **3 of 3 blocked**: by the schema, a conflict check, and a canary rollback ([M2 gate](docs/results/m2-gate.md)) |
| PII redaction | **0 of 9,000** injected fixtures leaked; patterns touch 0.015% of live events ([M3 gate](docs/results/m3-gate.md), [redaction](docs/results/m3.2-redaction.md)) |
| Hot-storage volume | **38% less** for the same input, from sampling, dedupe and routing ([M3 gate](docs/results/m3-gate.md)) |
| Throughput | **32,000 events/s** sustained on two aggregators in kind; p99 latency 29 ms up to 3,000 events/s ([benchmark](docs/results/m4.1-benchmark.md)) |
| Failures | **0 events lost** through an aggregator kill, Loki and S3 outages, and an agent kill, counted by event ID. A full disk lost 47 ([chaos](docs/results/m4.2-chaos.md)) |

## How it works

```
Wikimedia, GitHub ──► feeders (Go) ──┐
                                     ├──► Kafka ──► Vector aggregators ──┬──► Loki (hot) ──► Grafana
pod logs ──► Vector agents ──────────┘         ▲                        └──► S3 (cold)
                                               │
              LogPipeline specs ──► operator: render, validate, canary
```

- **Operator:** renders every team's spec into one aggregator config, checks it with `vector validate`, then canaries it on one aggregator. It promotes the config if errors and delivery hold, and rolls it back otherwise, quarantining only the failing team's change ([ADR 0001](docs/adr/0001-validate-in-the-operator-image.md), [0002](docs/adr/0002-canary-rollout-with-statefulset-partition.md)).
- **Per-team pipeline:** dedupe by event ID, then redaction (on by default; [ADR 0003](docs/adr/0003-redaction-on-by-default.md)). The complete stream goes to cold storage. The hot path is sampled per level and capped by a budget shared across aggregators ([ADR 0005](docs/adr/0005-budgets-split-across-aggregators.md)). Pod logs are parsed and given a normalized level ([ADR 0004](docs/adr/0004-parse-pod-logs-and-trust-only-the-stamp.md)).
- **Delivery:** Kafka offsets are committed only once events are in a sink's disk buffer. Data no team claims still reaches Loki, redacted.

## Quickstart

Needs Docker, `make` and `curl`; pinned kind, kubectl and helm are fetched into `./bin`.

```bash
make up                                   # kind cluster and the whole stack, ~5 min
export KUBECONFIG=$PWD/.kubeconfig
bin/kubectl apply -f examples/edits.yaml  # onboard a team
bin/kubectl get lp                        # Ready once its canary passes
make grafana-ui                           # http://localhost:3000; try {team="edits"}
make down
```

## A LogPipeline

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
  sampling: {keepPercent: {debug: 0, info: 25}}
  budget: {maxEventsPerSec: 500}
  routing: {hot: true, cold: true}
```

More in [`examples/`](examples). `bin/spillwayctl validate --vector-bin bin/vector examples/` (after `make spillwayctl`) runs the operator's checks without a cluster.

## Development

| | |
|---|---|
| `make test`, `make lint`, `make vector-check` | Go tests (with a real API server and Vector), lint, and Vector's validation and generated unit tests. CI runs these, plus `make generate` |
| `make bench`, `make chaos` | The throughput benchmark and the chaos experiments, against a fresh `make up` |
| `make help` | Every target |

The [build plan](docs/plan.md) tracks progress, [`docs/adr`](docs/adr) records the design decisions, and [`CLAUDE.md`](CLAUDE.md) is the detailed guide to how the system fits together.
