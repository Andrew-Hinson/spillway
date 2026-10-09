# M2 gate: the operator, end to end

The M2 gate from the [build plan](../plan.md), run in one pass against a fresh `make up` with `make m2-gate` ([`bench/m2_gate.sh`](../../bench/m2_gate.sh)):

> Applying an example LogPipeline spec renders, validates and rolls out config with the resource marked Ready and events flowing; a malformed spec is marked Invalid and never reaches an aggregator; a config change can be canaried and rolled back.

**Result: passed.**

## Setup

| | |
|---|---|
| Run | 2026-10-09 15:00–15:10 UTC, commit `1bfb203` plus the `edits` example and this script |
| Stack | Fresh `make up` (335 s): kind v0.33.0 with Kubernetes v1.37.0 (1 control plane, 2 workers), Kafka 4.3.1, Loki 3.7.8, Vector 0.58.0 (2 aggregators, 3 agents), the operator |
| Host | Apple M5 Pro, 48 GiB RAM, macOS 26.6.2; Docker 29.7.2 with 18 CPUs and 7.7 GiB |
| Canary | Defaults: 2 min bake (at least 2× the longest sink batch), error allowance max(5, 0.1% of events), 3 min ready timeout |
| Traffic | Live Wikimedia `recentchange`, about 30 events/s at the time |

## Results

| Gate criterion | Result |
|---|---|
| Example spec rendered and validated | `spillwayctl validate examples/edits.yaml`: schema ok, render ok, `vector validate` ok, 11 generated `vector test` cases pass. The operator's `vector validate` took about 9 ms per config |
| …rolled out, marked Ready | Canary on `vector-aggregator-1`, 2 min bake, promoted, then rolled to `vector-aggregator-0`. **Ready 132 s after `kubectl apply`** |
| …with events flowing | **First `{team="edits"}` event queryable 5 s after apply**, while still on the canary. Steady state: 13.7 events/s (budget 25/s), `edit` 12.9/s, `new` 0.7/s, `log` 0.1/s (thinned to 10%), `categorize` none (dropped at 0%) |
| Malformed spec never reaches an aggregator: schema | `kubectl apply` rejected it with all four errors, and it was never stored. `spillwayctl validate` reported the same four errors offline |
| Malformed spec marked Invalid: valid alone, conflicting | `edits-copy` (team already claimed) was marked `Invalid` in under a second. The stable config and the ConfigMap's revisions were unchanged, and no canary started |
| Config change canaried and rolled back | `content` passes every offline check (cold storage configured, endpoint missing). Its canary started, then it was **rolled back 125 s after apply**: the stalled-sink check found `cold_s3` took in events and delivered none. `edits` stayed Ready and flowing (14.7 events/s), both pods returned to the stable config, and `content` is `CanaryFailed` (quarantined) |

### Plan metrics (from M2)

| Metric | Value |
|---|---|
| Time to onboard a team (spec applied → first event queryable) | **5 s** (first event, during the canary); **132 s** to Ready on every aggregator |
| Bad configs blocked | **3 of 3** in this run: one at apply time (schema), one Invalid (conflict), one rolled back by the canary. None reached a stable aggregator |

The first event arrives during the canary because a new team's Kafka source has its own consumer group, and only the canary pod runs it until promotion. So a new team's events flow as soon as the canary pod is up, while existing teams keep running on the stable config.

## Observations

- **The runtime failure was caught by the stalled-sink check, not the error check.** The cold-storage sink's retry errors stayed within the traffic-scaled allowance, so the rollback came at the end of the 2 min bake, as designed (see ADR 0002, "Isolating works, at a cost in time"). A sink stall is the more reliable signal for an unreachable destination.
- **A rollback leaves an orphaned buffer.** It was reported as `spillway_operator_orphaned_buffers_total{sink="cold_s3"} 1` and in `content`'s status. Draining it is [issue #43](https://github.com/Andrew-Hinson/spillway/issues/43).
- **Operator counters reset in step 3,** because the script restarts the operator with cold storage configured. The metrics at the end of the log cover step 3 only (1 rollback, 2 validations). Steps 1–2 added a promotion and validations before that restart.

## How to reproduce

```bash
make up          # fresh cluster
make m2-gate     # ~10 minutes; restores the operator and deletes its LogPipelines at the end
```

## Log

```
## Setup
15:00:55 aggregator pods: vector-aggregator-0(true) vector-aggregator-1(true) 
15:00:55 waiting for the operator's first config to be promoted (stable, no canary)
15:02:52 stable config 94e639bdb3e2d5ee, partition 0

## 1. An example spec is rendered, validated, canaried and Ready, with events flowing
15:02:52 spillwayctl validate examples/edits.yaml:
    examples/edits.yaml (edits): ok
    render: ok (1 pipeline)
    vector validate: ok
    vector test: ok (11 tests)
15:02:52 kubectl apply -f examples/edits.yaml
    logpipeline.spillway.dev/edits created
15:02:53   edits → RollingOut: config f592797aef538e25 is starting on canary pod vector-aggregator-1
15:02:56   edits → RollingOut: config f592797aef538e25 is on canary pod vector-aggregator-1; waiting for its metrics
15:02:59   first {team="edits"} event queryable in Loki: 5s after apply
15:03:02   edits → RollingOut: canary: config f592797aef538e25 is running on vector-aggregator-1 (1 of 2 aggregators); promoting at 2026-10-09T15:04:59Z if its error rate holds
15:05:01   edits → RollingOut: config f592797aef538e25 passed its canary and is rolling to every aggregator
15:05:04   edits → RolledOut: the aggregator is running config that includes this pipeline
15:05:04 edits Ready 132s after apply
15:06:04 events/s by type for {team="edits"}, last 1m: edit=12.9 log=0.1 new=0.7
15:06:04 events/s total for {team="edits"}, last 1m: total=13.7  (budget: 25/s)

## 2. Malformed specs never reach an aggregator
15:06:04 kubectl apply of a malformed spec:
    The LogPipeline "malformed" is invalid: 
    * spec.team: Invalid value: "Edits Team": spec.team in body should match '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
    * spec.sampling.keepPercent.info: Invalid value: 150: spec.sampling.keepPercent.info in body should be less than or equal to 100
    * spec.routing: Invalid value: at least one of hot or cold must be enabled
    * spec.sources[0]: Invalid value: exactly one of kafka or kubernetes must be set
15:06:04   stored? Error from server (NotFound): logpipelines.spillway.dev "malformed" not found
15:06:04 spillwayctl validate on the same file:
    /tmp/spillway-malformed.yaml (malformed): INVALID
      * spec.sampling.keepPercent.info: Invalid value: 150: spec.sampling.keepPercent.info in body should be less than or equal to 100
      * spec.team: Invalid value: "Edits Team": spec.team in body should match '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$'
      * spec.routing: Invalid value: at least one of hot or cold must be enabled
      * spec.sources[0]: Invalid value: exactly one of kafka or kubernetes must be set
    render: ok (0 pipelines)
15:06:04 a spec that's valid alone but claims a team already claimed (edits-copy):
    logpipeline.spillway.dev/edits-copy created
15:06:04   edits-copy → Invalid: team "edits" is claimed by both default/edits and default/edits-copy
15:06:14   stable config before: f592797aef538e25, after: f592797aef538e25
15:06:14   config revisions before: ['vector-f592797aef538e25.yaml', 'vector.yaml']
15:06:14   config revisions after:  ['vector-f592797aef538e25.yaml', 'vector.yaml']
15:06:14   canary started? '' (empty means no)

## 3. A change that fails at runtime is canaried and rolled back
15:06:14 configuring the operator's cold storage at an endpoint that doesn't exist
15:06:15 spillwayctl validate examples/content.yaml (cold storage configured): every offline check passes
    examples/content.yaml (content): ok
    render: ok (1 pipeline)
    vector validate: ok
    vector test: ok (8 tests)
15:06:15 kubectl apply -f examples/content.yaml
    logpipeline.spillway.dev/content created
15:06:15   content → RollingOut: config 0789514085b9e438 is starting on canary pod vector-aggregator-1
15:06:19   content → RollingOut: canary: config 0789514085b9e438 is running on vector-aggregator-1 (1 of 2 aggregators); promoting at 2026-10-09T15:08:18Z if its error rate holds
15:08:20   content → CanaryFailed: generation 1 config 0789514085b9e438 was rolled back: canary pod vector-aggregator-1 took in events for cold_s3 but delivered none in 2m0s. Change the spec to try again
15:08:20 content settled 125s after apply
15:08:20   edits meanwhile: RolledOut
15:09:35   stable config before: f592797aef538e25, after: f592797aef538e25
15:09:35   pods: vector-aggregator-0=-f592797aef538e25 vector-aggregator-1=-f592797aef538e25 
15:09:35   {team="edits"} events/s, last 1m: total=14.7

## Operator metrics
    spillway_operator_orphaned_buffers_total{sink="cold_s3"} 1
    spillway_operator_rollouts_total{result="rolled_back"} 1
    spillway_operator_validation_duration_seconds_sum 0.017681165999999998
    spillway_operator_validation_duration_seconds_count 2
    spillway_operator_validations_total{result="valid"} 2
15:09:38 done
```
