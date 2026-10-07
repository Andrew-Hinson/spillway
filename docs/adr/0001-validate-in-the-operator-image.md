# ADR 0001: Validate rendered config with Vector bundled in the operator image

Status: accepted · 2026-10-07 · M2.4

## Context

The operator renders every LogPipeline into one aggregator config and rolls the aggregator when that config changes (M2.3). The CRD schema and the renderer's own checks catch malformed specs, but neither runs Vector. A renderer bug, or a spec combination nobody anticipated, could still produce config Vector rejects. If that config reached the aggregator, the pod would fail to start, and every team's logs would stop at once.

So rendered config has to pass `vector validate` before it's applied. The plan left open where that runs:

1. **A Kubernetes Job per config:** the operator writes the config to a ConfigMap and starts a Job running the aggregator's own image. It then waits for the Job and reads the result from its logs or exit code.
2. **Vector bundled in the operator image:** the operator writes the config to a temporary file and runs `vector validate` in its own container.

## Decision

Bundle Vector in the operator image (option 2).

- **Version:** the operator image copies `/usr/local/bin/vector` from `timberio/vector:<VECTOR_VERSION>-distroless-static`. `VECTOR_VERSION` is pinned once in the Makefile, which also uses it for the aggregator image and for the binary the tests run. The validator and the aggregator therefore can't drift apart.
- **Where the gate runs:** the reconciler validates before it touches the ConfigMap. If the combined config is rejected, it validates each pipeline on its own. Pipelines Vector rejects alone are marked `Invalid` with Vector's error and left out, and the rest are re-rendered and re-validated. If the rest still fail together, nothing is applied: pipelines are marked `ValidationFailed`, and the aggregator keeps its last valid config.
- **Caching:** verdicts are cached by config hash, so the reconciles that follow every status and rollout event don't re-run Vector.
- **No silent bypass:** the operator refuses to start without a working vector binary, so the gate can't be skipped by accident.

## Consequences

Good:
- **Fast and synchronous.** `vector validate` takes about 20 ms on these configs, against several seconds to schedule a Job's pod. The verdict is ready within the same reconcile.
- **No new permissions or moving parts.** The operator needs no rights to create Jobs or pods, and there's no Job cleanup, timeout handling or log scraping.
- **Same check everywhere.** Tests (`./bin/vector`) and `make vector-check` (the aggregator image) run the same pinned Vector version as the operator.

Bad:
- **Bigger image:** the operator image grows from 48 MB to 242 MB, almost all of it Vector.
- **Coupled upgrades:** upgrading Vector means rebuilding the operator. The Makefile's single `VECTOR_VERSION` makes that one change, but the two must ship together.
- **Shared resources:** validation runs inside the operator's container, so a pathological config costs the operator memory. That's bounded by `--validation-timeout`, and the memory limit rises to 512 Mi.
- **No environment checks:** `--no-environment` skips connecting to Kafka, Loki and S3. The gate answers "will Vector accept this config?", not "is the cluster healthy?". Runtime failures are what the canary rollout (M2.7) is for.

Revisit if the operator's memory becomes a problem, or if aggregators ever run different Vector versions per environment. A Job running each aggregator's own image would handle both.
