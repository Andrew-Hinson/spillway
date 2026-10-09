# ADR 0002: Canary aggregator config with a StatefulSet partition and per-revision config files

Status: accepted · 2026-10-09 · M2.7

## Context

Since M2.3, every config change restarts the whole aggregator with the new config at once. The validation gate (ADR 0001) catches config Vector rejects, but not config that loads and then fails against the real cluster. Examples: a cold-storage endpoint that refuses connections, a Loki that rejects a label set, a source that errors on live data. M2.7 asks for the new config to go to one aggregator first, be promoted only if its error rate holds, and be rolled back otherwise.

Three things constrain the design:

1. **Different pods need different config.** All aggregator pods mount one ConfigMap. If the ConfigMap's content changes, any pod that restarts mid-canary (stable ones included) picks up the untested config.
2. **The operator can't share fields with Helm.** `make aggregator` re-runs `helm upgrade` with server-side apply. A field both Helm and the operator write causes a conflict that fails the upgrade (seen in M1.7). The operator may only touch fields Helm never sets.
3. **Restarts must be survivable.** The operator can restart mid-canary, so rollout state can't live only in memory.

## Decision

**Per-revision config files, selected by a pod annotation.**
- **ConfigMap layout:** the aggregator ConfigMap holds the bootstrap `vector.yaml` plus one `vector-<hash>.yaml` per live revision (stable and canary). Older revisions are pruned after a promotion.
- **How a pod picks its file:** Vector starts with `--config /etc/vector/vector$(SPILLWAY_CONFIG_SUFFIX).yaml` (Helm values). `SPILLWAY_CONFIG_SUFFIX` comes from the pod annotation `spillway.dev/config-suffix` through the downward API, so each pod's config file is fixed by its own template revision, even across restarts.
- **Bootstrap:** before the operator has acted, the annotation is absent, the suffix is empty, and pods load the bootstrap `vector.yaml`. Helm never sets the annotation, so the operator owns it outright.

**Canary through the StatefulSet's `partition`.**
- **Replicas:** the aggregator runs 2 replicas, sharing the Kafka consumer group and the agents' Service.
- **Start the canary:** write `vector-<new>.yaml`, then set the template's suffix to `-<new>` and `partition` to `replicas - 1`. Only the highest-ordinal pod rolls, and it takes a real share of the traffic: its Kafka partitions and the agents connected to it. Helm leaves `updateStrategy` unset, so the operator owns `partition`.
- **Bake:** once the canary pod is Ready on the new revision, the operator scrapes both the canary's and a stable pod's Vector metrics (`:9598`) at the start and end of the bake window. The bake is 2 minutes by default, but never shorter than twice the longest sink batch timeout in the config being canaried. A sink that batches for 60 s has to get the chance to send at least once before a stall can be judged.
- **Promote** if, over the window:
  - the canary didn't restart;
  - its errors grew by no more than the stable pod's, plus an allowance: the larger of 5 and 0.1% of the events its sinks took in, so the allowance scales with traffic. Errors means `vector_component_errors_total`, plus `vector_http_client_errors_total`, plus unintentional discards;
  - no sink stalled: none took events in without delivering any. A sink counts only if it delivers on the stable pod or exists only on the canary, so a shared outage isn't blamed on the canary.

  Promotion sets `partition` to 0, so the remaining pods roll.
- **Roll back** if the canary never becomes Ready (3-minute timeout), restarts, exceeds the error allowance, or has a stalled sink. The template's suffix is restored to the stable revision, so the canary pod returns to stable config.
- **Quarantine the change, not the config.** The pipeline generations the canary introduced (those not in the stable config) are quarantined, and later configs leave them out until their spec changes, so one team's failed change can't ride along with, and sink, everyone else's.
  - **Isolate on failure.** This is how merge queues treat a failed batch. If a failed canary introduced one change, that change is at fault and stays quarantined. If it introduced several, none is known to be at fault: each is retried in a canary of its own, one at a time, in a stable order. Those that pass are released, and only those that fail alone stay quarantined. While waiting, a change's status says how many retries are ahead of it.
  - A quarantined pipeline that ran before keeps running its *last good spec*. The operator records it on each LogPipeline (`spillway.dev/last-good-spec`) when a config is promoted, like `kubectl`'s last-applied annotation.
  - Only if a failed canary introduced no pipeline changes (the operator's first config on a fresh aggregator) is the config hash itself recorded as rejected, so it isn't retried in a loop.
- **Drop rates:** the plan defers drop-rate checks until M3 policies exist. The comparison uses Vector's unintentional discards (`intentional!="true"`) alongside errors, so sampling and budget drops never count against a canary.

**State on the StatefulSet.** Annotations on the StatefulSet's own metadata (not its pod template) hold:
- `stable-config`: the stable hash;
- `stable-pipelines`: the pipelines it contains, with their generations;
- `canary-config`, `canary-pipelines` and `canary-baseline`: the canary in progress, its pipelines and its starting counters;
- `quarantined-pipelines`;
- `rejected-config` and `rejected-reason`.

A restarted operator resumes from these.

**Status.**
- **Ready=True:** a pipeline whose current generation is in the stable config stays Ready, even while a canary for someone else's change bakes.
- **During a canary:** changed pipelines are `RollingOut`, with the canary's progress in the message.
- **After a rollback:** the quarantined pipelines are `CanaryFailed`, with the reason, and say which generation keeps running.

## Consequences

Good:
- **Runtime failures reach one pod.** A config that validates but fails against the real cluster reaches only one pod and part of the traffic, and is rolled back automatically. The other pod keeps serving with the last good config throughout.
- **No config drift on restart.** A stable pod that restarts mid-canary comes back with stable config, because its file is pinned by its template revision, not by what the ConfigMap holds now.
- **No Helm conflicts.** The operator owns only the pod annotation, `partition` and its own StatefulSet annotations, none of which Helm sets.
- **Resumable.** State survives operator restarts.

Bad:
- **More resources:** two aggregator replicas double the aggregator's footprint locally, at 2 × 4 Gi volumes. Two is also the minimum for availability, and a PodDisruptionBudget (`minAvailable: 1`) keeps node drains from taking both down at once.
- **Slower changes:** every config change now takes at least the bake window plus two pod restarts before it's fully live, roughly 2–3 minutes instead of seconds. "Time to onboard a team" grows by that much.
- **Orphaned buffers:** a rolled-back config can leave buffered events behind. If the failed config added a sink with a disk buffer, events it accepted during the canary stay in that buffer on the canary pod's volume, undelivered. Kafka offsets were committed once those events were buffered. In testing, two failed canaries of a cold-storage sink left 37 MB on pod 1.
  - Deleting the buffer would make the loss silent, so it's made visible instead: the operator logs a warning, counts `spillway_operator_orphaned_buffers_total{sink}`, and names the sink in the failed pipeline's status.
  - Draining or replaying the buffer is [issue #43](https://github.com/Andrew-Hinson/spillway/issues/43). If the fixed pipeline later rolls out with the same sink ID, Vector picks the buffer up again.
- **Rollback isn't instant:** reverting the canary pod takes up to its termination grace period (60 s), because Vector tries to drain sinks on shutdown, including the failing one.
- **A proxy for health:** the error comparison is a proxy. A config that silently routes events to the wrong place, without errors, passes. Drop-rate checks (M3) and per-team volume checks narrow that.
- **Limited sample:** the canary's share of traffic depends on how Kafka partitions and agent connections land. With 3 partitions and 2 pods it's about a third to two thirds of feeder traffic, and with few agents it may receive no pod logs at all.
- **Vector-specific selection:** the `$(SPILLWAY_CONFIG_SUFFIX)` file selection relies on Kubernetes expanding environment variables in container args, and on Helm values the operator can't enforce. If someone overrides `args` in the values, the operator can't select config. The operator checks the StatefulSet's container args and refuses to roll out if the selector is missing.

## What testing changed

The first version compared only `vector_component_errors_total`, and quarantined config hashes rather than pipelines. Two end-to-end runs in kind showed why that wasn't enough:
- **Retries aren't errors.** A cold-storage sink pointed at an endpoint that doesn't exist passed its canary and was promoted. Vector retries the failing requests: each attempt is counted only in `vector_http_client_errors_total`, and the sink takes events in while delivering none. With HTTP errors and stalled sinks added, the same config is rolled back about 15 s into the bake.
- **Hashes let bad changes ride along.** With hash-level rejection, an unrelated team's later change produced a new config that still contained the broken pipeline, and was rolled back too. Quarantining pipeline generations fixed that: in the same scenario, the other team's change rolls out while the broken pipeline stays held back.
- **Isolating works, at a cost in time.** With the broken cold-storage pipeline and a good one applied together, the combined canary failed, both were retried alone, the good one was promoted and the broken one quarantined, in about 7.5 minutes. With the traffic-scaled allowance, the cold-storage sink's retry errors (a handful against roughly 10,000 events) stayed within budget. It was the stalled-sink check that caught it, at the end of the bake rather than about 15 s in. The checks are layered for exactly this.

## Review decisions (2026-10-09)

Settled after review, following the practice of established canary tools (Argo Rollouts, Flagger, Spinnaker's canary analysis):
1. **Bake:** keep 2 minutes, with the floor of twice the longest sink batch timeout.
2. **Isolation:** retry the changes from a failed batch one at a time; only those that fail alone stay quarantined.
3. **Orphaned buffers:** make them visible now (warning, metric, status) and drain them in issue #43, never delete them silently.
4. **Replicas:** keep two, with a PodDisruptionBudget.
5. **Error allowance:** make it relative to traffic, with an absolute floor. Throughput and drop-rate comparisons come with M3.

Alternatives considered:
- **A second, operator-owned canary StatefulSet:** it would isolate the canary fully, but needs its own Service routing and volumes, and the operator would own a whole workload Helm doesn't manage.
- **One ConfigMap per revision, mounted by name:** the pod template's volume would change, and Helm owns that field (constraint 2).
- **Vector's `--watch-config` hot reload:** there's no per-pod control. Every pod reloads when the ConfigMap changes.
