# Spillway — Build Plan

Sep 30, 2026 · @Andrew

## Overview

Spillway is a Kubernetes platform that turns a short YAML spec per team into a running, tested Vector pipeline: collection, PII redaction, sampling, per-team budgets, and routing to hot and cold storage. It runs on real, continuous public data streams, and every claim in the README is backed by a benchmark or a chaos test.



## Repo layout

One monorepo, Go for the operator and CLI, with `make up` bringing the whole stack up on a local kind cluster.

```
spillway/
├── api/v1alpha1/          # CRD types: LogPipeline (Go, kubebuilder)
├── cmd/
│   ├── operator/          # operator entrypoint
│   └── spillwayctl/       # CLI: validate, render, diff a spec locally
├── internal/
│   ├── controller/        # reconcile loop: spec -> Vector config -> rollout
│   ├── render/            # config generator (VRL + config templates)
│   ├── redact/            # redaction pattern library, compiled to VRL
│   └── budget/            # per-team throttle transforms
├── vector/
│   ├── agent/             # DaemonSet base config
│   ├── aggregator/        # StatefulSet base config, disk buffers
│   └── tests/             # `vector test` unit tests for generated VRL
├── deploy/
│   ├── helm/spillway/     # operator + CRD chart
│   ├── gitops/            # Argo CD apps per environment
│   └── terraform/         # cloud env: cluster, bucket, IAM
├── feeders/
│   ├── wikimedia/         # SSE -> Kafka shim for Wikimedia EventStreams
│   └── github-events/     # poller for the public GitHub events API
├── bench/                 # load + latency harness
├── chaos/                 # failure experiments: pod kill, sink outage, disk full
├── dashboards/            # Grafana JSON: per-team volume, drops, lag, cost
├── examples/              # sample LogPipeline specs for fictional teams
├── docs/                  # architecture, ADRs, benchmark results
├── Makefile               # make up / make down / make e2e
└── .github/workflows/     # lint, unit, vector validate + test, e2e on kind
```

Rust comes in through the stretch goal: a custom Vector component or an upstream PR.

## Milestones

Four phases, each ending in a gate you can demo; don't start the next until the gate passes.

| Phase | Goal | Gate (demo this before moving on) |
| --- | --- | --- |
| M1 | Real traffic, end to end | From a clean checkout, `make up` brings the stack up and a Wikimedia edit is queryable in Grafana within seconds; baseline volume and latency numbers recorded |
| M2 | The operator | Applying an example LogPipeline spec renders, validates and rolls out config with the resource marked Ready and events flowing; a malformed spec is marked Invalid and never reaches an aggregator; a config change can be canaried and rolled back |
| M3 | Policy features | Two teams on two feeders, each with redaction, sampling, a budget, and hot and cold routing; injected fixtures produce zero leaks in the hot sink; volume reduction measured against the M1 baseline |
| M4 | Proof and polish | Benchmark and chaos results committed to `docs/results`; the README shows real numbers; the cloud environment has been stood up, measured and torn down |

M1 is the one to protect: once real events flow end to end, everything after is adding features to a working system.

## Starter issues

Each line is one GitHub issue: the title, then the acceptance criteria after the dash.

### M1 — Real traffic, end to end

1. [x] Bootstrap repo, kind cluster, Makefile — `make up` works from a clean checkout; `make down` leaves nothing behind
2. [x] Install Kafka (Strimzi), Loki and Grafana via Helm — all healthy after `make up`, storage persisted
3. [x] Wikimedia feeder in Go — reads the recentchange stream, produces to Kafka, reconnects on drop, stamps every event with a unique ID and a produce timestamp, exports a produced-count metric
4. [x] Vector aggregator: Kafka to Loki — events parsed as JSON and queryable in Grafana; each event stamped with a pre-sink timestamp for latency measurement
5. [x] Vector self-monitoring — internal metrics scraped, starter dashboard for throughput, errors and buffer size; baseline bytes-in and latency recorded
6. [x] CI baseline — lint, unit tests and `vector validate` on every PR
7. [ ] Vector agent DaemonSet — pod logs reach the aggregator with namespace and pod labels attached (not needed for the M1 gate; can slip to M2 if M1 runs long)

### M2 — The operator

1. [ ] Scaffold operator and LogPipeline CRD — schema validation rejects malformed specs at apply time
2. [ ] Renderer: spec to Vector config — golden-file tests for each example spec
3. [ ] Reconcile and roll out — rendered config applied to aggregators, status conditions Ready / Invalid set on the resource
4. [ ] Validation gate — `vector validate` runs on rendered config before apply, either in a Job or with Vector bundled in the operator image (decision recorded as an ADR); a bad spec never reaches a running aggregator
5. [ ] `spillwayctl render` and `validate` — same renderer as the operator, usable without a cluster
6. [ ] VRL unit tests in CI — every generated transform covered by `vector test`
7. [ ] Canary config rollout — new config to one aggregator first, promoted only if error rates hold, rolled back otherwise; drop-rate checks added once M3 policies exist

### M3 — Policy features

1. [ ] Fixture injector — feeder flag or small producer that mixes known SSN, email, phone and member-ID records into the live streams at a set rate, tagged so they can be counted
2. [ ] Redaction library — SSN, email, phone and member-ID patterns; fixtures with positive and negative cases; zero injected fixture values found in the hot sink
3. [ ] Sampling and dedup — per-level sample rates from the spec; dropped counts visible per team
4. [ ] Per-team budgets — throttle transform from `maxEventsPerSec`; over-budget events counted, not silently lost
5. [ ] Cold path to object storage — MinIO locally, S3 in cloud; partitioned by team and date, compressed
6. [ ] Cost attribution dashboard — volume and estimated cost per team, hot vs cold
7. [ ] Second feeder: GitHub public events — different shape and rate than Wikimedia, both flowing at once

### M4 — Proof and polish

1. [ ] Benchmark harness — stepped load; throughput and p99 end-to-end latency written to `docs/results`
2. [ ] Chaos experiments — aggregator kill, sink outage, disk full; each asserts produced = delivered + counted drops (sampled, throttled), with duplicates removed by event ID
3. [ ] Cloud environment — Terraform plus Argo CD; torn down after benchmarks to keep cost near zero
4. [ ] README and ADRs — architecture diagram, quickstart, results table, three to five design decisions written up
5. [ ] Stretch: Rust contribution — upstream Vector PR or a custom component, linked from the README

## Metrics to capture

Start measuring in the milestone listed, so the README and resume bullets carry real numbers, not adjectives.

| Metric | From | How to measure | Resume angle |
| --- | --- | --- | --- |
| Volume reduction (%) | M1 baseline, compared from M3 | Bytes in at the aggregator vs bytes written to the hot sink | Cost savings from sampling, dedup and routing |
| Event loss under failure | M4 | Unique event IDs produced vs delivered plus counted drops, per chaos run | Zero loss across N failure scenarios |
| Sustained throughput (events/s) | M1 | Highest stepped load with stable buffers and no drops | Scale |
| p99 end-to-end latency (ms) | M1 | Feeder produce timestamp vs the aggregator's pre-sink timestamp | Performance under load |
| Redaction leaks | M3 | Injected fixture values found in the hot sink | Compliance by default |
| Time to onboard a team (min) | M2 | Spec applied to first event queryable | Developer experience |
| Bad configs blocked | M2 | Invalid specs stopped by the validation gate | Safe self-service |

Target bullet shape: *Built a Kubernetes operator that generates Vector pipelines from team specs; cut hot-storage log volume X% and lost zero events across N failure scenarios at Y events/s.*
