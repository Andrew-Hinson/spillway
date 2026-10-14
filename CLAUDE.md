# Spillway: notes for Claude Code sessions

Spillway is a Kubernetes log platform: one `LogPipeline` spec per team becomes a running, tested Vector pipeline (collection, PII redaction, sampling, budgets, hot/cold routing). It runs on live public data, and every claim in the README is backed by a measurement in `docs/results`.

## Where the state lives

- **Progress:** [`docs/plan.md`](docs/plan.md) has the milestones, the gate for each, and a checkbox per work item. Each item is a GitHub issue titled `[Mx.y] …` with its acceptance criteria. `gh issue list` shows what's open.
- **Decisions:** [`docs/adr/`](docs/adr). 0001: validation with Vector bundled in the operator image. 0002: canary rollout via StatefulSet partition, quarantine and last-good specs. 0003: redaction on by default, with boundary-free patterns. 0004: pod logs parsed, with only the feeder stamp trusted from an app's `spillway`. 0005: budgets split evenly across the aggregators, over a 10 s window, with pod logs through Kafka so they spread evenly. 0006: Silo (a maintained MinIO fork) for local cold storage, since MinIO no longer publishes images. 0007: the cloud environment, EKS from Terraform and the stack from Argo CD.
- **Measurements:** [`docs/results/`](docs/results): the M1 baseline, the M2 and M3 gate runs, M3.2's redaction false positives and leak check, M3.3's sampling, dedupe and per-team drops, M3.4's budgets, M3.5's cold path, M3.6's cost attribution, M3.7's GitHub feeder, M4.1's stepped-load benchmark, and M4.2's chaos experiments.
- **Why something is the way it is:** the PR descriptions (`gh pr view N`) record the design, what testing found, and known limitations.

## Workflow for a work item

1. **Branch** from an up-to-date `main`: `git checkout main && git pull --ff-only && git checkout -b mX.Y-short-name`.
2. **Read the issue** (`gh issue view N`) and the plan's gate for the milestone, then build to the acceptance criteria.
3. **Check locally:** `make lint`, `make test`, `make generate` (must leave no diff; CI checks), and `make vector-check`. `make generate` also writes the image tags in `deploy/cloud/apps/generated.yaml`, so any change to the operator's or a feeder's source needs it.
4. **Test end to end in kind** for anything that touches the running system: `make up` from scratch, then apply real specs and query Loki and Prometheus. Unit tests alone have repeatedly missed real bugs here: VRL type errors, Vector's retry behaviour, the SSN matcher.
5. **Update docs:** tick the plan's checkbox, update the README's results table if a headline number changed (keep the README short: details belong in `docs/results`, the ADRs and this file), and add or update an ADR for a significant design decision.
6. **PR:** the title ends in `(Mx.y)`, and the body starts with `Closes #N`. Then a criteria → "how it's met" table, the design, notes and limitations, and a test plan with honest results (say what failed and what was fixed). CI must pass before merging.
7. **Merge** with `gh pr merge N --merge --delete-branch`. Then `git checkout main && git pull`, and confirm CI passes on `main`.
8. **`make down`** when finished testing.

`main` is protected by a ruleset: changes go through a PR, and both CI checks are required. Their names must match the job names in `.github/workflows/ci.yml` exactly ("Go lint, generate and test" and "Vector validate and test"). Rename a job only together with the ruleset.

## Commands

| | |
|---|---|
| `make up` / `make down` | Whole stack on kind (~5–6 min) / delete it. The kubeconfig is `./.kubeconfig` |
| `make test` | Go tests. It sets `KUBEBUILDER_ASSETS` (envtest API server) and `VECTOR_BIN` (pinned vector). Running `go test` directly skips those tests, except in CI, where they fail instead |
| `make generate` | CRD, RBAC and deepcopy from the Go type markers. Never hand-edit `deploy/operator/crd` or `rbac` |
| `make vector-check` | `vector validate` and `vector test` on every golden config |
| `go test ./internal/render -update` | Regenerate renderer goldens: `internal/render/testdata/*.yaml`, their `.tests.yaml`, `vector/aggregator/vector.yaml` and `vector/tests/aggregator.yaml`. Review the diff |
| `make spillwayctl` | CLI: `bin/spillwayctl validate --vector-bin bin/vector examples/` |
| `make fixtures RATE=N` | Fictional-PII injector (opt-in): N/s each into the Wikimedia topic, the GitHub topic and its pod's stdout |
| `make m2-gate`, `make m3-gate`, `make baseline` | Recorded measurements; see `docs/results`. The gates run against a fresh `make up` (`m3-gate`: ~20 min with the default `WINDOW=15m`) |
| `make volume WINDOW=15m` | Hot-storage bytes against bytes in, overall and per feeder team, next to the M1 baseline's 102.9% |
| `make leak-check WINDOW=15m` | Search every hot-sink stream for fixture values; fails on a leak or if no fixtures arrived |
| `make cost-report WINDOW=30m` | Volume and estimated cost per team, hot vs cold (the cost dashboard's numbers as Markdown) |
| `make sampling-check WINDOW=10m REF=edits=wiki-all` | Each team's dedupe, sampling and budget drops; per-level kept share against an unsampled reference team |
| `make cold-check` | Sync the cold bucket and check every object: `team=…/date=…/` keys matching their events, gzip, no fixture values |
| `make storage` | Local S3 (Silo, a MinIO fork, in `storage`), the `spillway-cold` bucket, and generated credentials in the `cold-storage` Secret (also copied to `vector`) |
| `make redaction-fp DURATION=600` | Capture live Wikimedia events and count what each redaction pattern masks |
| `make bench STEPS=… STEP=4m` | Stepped-load benchmark (M4.1): `feeders/loadgen` into `bench.load`, through the `bench` team (`deploy/bench`), reported by `bench/throughput.py`. Against a fresh `make up`; don't run CPU-heavy work alongside it |
| `make chaos SCENARIOS=…` | Chaos experiments (M4.2): aggregator kill, Loki and Silo frozen, disk full, pod-log control, agent kill. Every event of each run counted in Loki and S3 by event ID (`bench/eventcheck.py`). Against a fresh `make up`, about 90 min |
| `make cloud-plan` | What the EKS environment would create (`deploy/cloud/terraform`). Creates nothing, but needs AWS credentials (the default profile) |
| `make cloud-up` / `make cloud-down` | Create the EKS environment and deploy the stack with Argo CD from `main` / delete it all and check nothing tagged `spillway` is left. **Billed by the hour: only with the owner's go-ahead** |
| `make cluster kind-images argocd REVISION=<branch>` | The cloud's Argo CD setup on kind (profile `kind`), from a pushed branch: how to test `deploy/cloud/apps` without AWS |
| `make tools` | Pinned kind, kubectl, helm, golangci-lint, controller-gen, setup-envtest and vector, in `./bin` |

## How the system fits together

- **Renderer** (`internal/render`): all LogPipelines → one aggregator config, deterministically.
  - Components are named `<team>_<stage>`; sinks (`hot_loki`, `cold_s3`) are shared. `metrics_by_team` tags each team component's internal metrics with `team`, so drops and throughput sum per team.
  - Stages: `in` → `ids`/`dedupe` (on `spillway.event_id`; events without one bypass it) → `redact` → `cold` (a pass-through to `cold_s3`, so each team's cold volume is counted) and `sample` → `budget` → `hot`. Cost attribution (`dashboards/cost.json`, `bench/cost.py`) reads bytes at `<team>_ids`, `<team>_hot` and `<team>_cold`.
  - `budget` is a throttle per aggregator pod, with no shared state: each gets `floor(maxEventsPerSec / replicas)` over a 10 s window (`BudgetPerPod`). The operator renders with the StatefulSet's replica count; `render.DefaultOptions` (and so `spillwayctl` and the goldens) assumes 2.
  - Data no team claims takes the platform paths (`wikimedia, github → redact_feed → stamp → loki`, `agents → pod_logs → redact_pods → loki_pods`), which mask every pattern. `Options.Platform.Feeds` lists the feeder topics; a claimed topic drops just its feed, and the path goes when every feed is claimed. Claimed namespaces leave the pod path.
  - With no pipelines, it renders the platform config (`vector/aggregator/vector.yaml`): M1 plus redaction.
  - It also generates `vector test` cases for every transform it emits (`tests.go`), and a Go test enforces that every transform is covered.
- **Operator** (`internal/controller`): renders → applies quarantine → `vector validate` (cached) → writes `vector-<hash>.yaml` to the ConfigMap → canaries via partition → promotes or rolls back.
  - State lives in StatefulSet annotations (`spillway.dev/*`).
  - Pods select their config file through `$(SPILLWAY_CONFIG_SUFFIX)` from a pod annotation.
  - Cold storage is on in kind: `deploy/operator/operator.yaml` passes `--cold-bucket=spillway-cold --cold-endpoint=http://s3.storage.svc:9000`. Without `--cold-bucket`, cold-routed pipelines are `Invalid`. `bench/m2_gate.sh` swaps the endpoint for a broken one and restores the manifest's args afterwards.
- **Field ownership:** the operator owns only fields Helm never sets: the config-suffix pod annotation, `updateStrategy.partition`, and its own annotations. Never `kubectl set`/`patch` a Helm-owned field, or the next `helm upgrade` fails with a server-side-apply conflict. If it happens, set the field back to Helm's value.
- **Pod logs** travel agent → Kafka topic `spillway.pods` → the aggregators' shared `agents` source (consumer group `spillway-pods`, from the oldest offset). The agent produces with no key, so batches spread over the partitions. Parsing (`internal/render/podlogs.go`): a JSON line's fields become the event's fields; any other line stays in `message`. Every line gets a normalized `level` when one is recognizable (JSON, klog, a leading level word, logfmt). An app's `spillway` object is trusted only in the exact feeder-stamp shapes, because redaction skips `.spillway`; the rest lands in `app_spillway`, which is redacted. `TestPodLogSpillwayCantCarryPII` guards this.
- **Redaction** (`internal/redact`): VRL `redact()` filters, on by default (a spec opts out with `redaction: {disabled: true}`). The patterns have no `\b`: escaped JSON in pod logs (`\n123-45-6789`) defeats word boundaries, and VRL's regex has no lookbehind. `testdata/corpus.yaml` holds the positive and negative cases. Run a candidate pattern against live data with `go run ./bench/redactfp -filter name=r'…'` before changing one.
- **Canary stall check:** a sink is stalled only if it had received events by a checkpoint, the longest batch timeout plus 15 s before the end of the bake, and delivered none by the end. Bursty, low-rate sources (GitHub) can deliver their first events late in a bake (ADR 0002 amendment).
- **Canary timing:** each config change takes ~2.5–4 min to reach both aggregators (2 min bake plus two restarts). Bake ≥ 2× the longest sink batch timeout. Error allowance is max(5, 0.1% of events).

- **Cloud** (`deploy/cloud`, ADR 0007): Terraform makes the EKS cluster, the cold bucket and the Pod Identity role the aggregators write to it with. Argo CD deploys everything else from `deploy/cloud/apps`, an app of apps that wraps the same charts, values and manifests `make up` uses. The differences are a `profile` value (`aws` or `kind`). Images come from GHCR (`.github/workflows/images.yml` publishes them on every push to `main`), tagged with the source hashes in `generated.yaml`. Argo CD uses server-side apply and server-side diff, so it owns only the fields its manifests set and leaves the operator's alone. Verified in kind: a canary rollout under Argo CD caused no OutOfSync and no self-heal sync.

## Testing expectations

- **Prove tests can fail:** break the code on purpose (e.g. drop a redaction filter) and check that the test catches it.
- **Confirm CI ran the tests:** check that CI logs show the envtest and vector tests actually running (`gh run view --log`), not skipped.
- **Read real output:** for Vector behaviour, run the real binary (`bin/vector test`, `vector validate`) or scrape a live pod's `:9598/metrics`. Don't assume what Vector reports.
- **`vector test` can't count:** an `outputs` check passes if *any* output event meets it, so it can't show that something was dropped while something else got through. For counts, run the rendered components in the real binary from Go (`internal/render/vector_test.go`).
- **Clean up after controller tests:** envtest tests share one API server, and the reconciler sees LogPipelines in every namespace, so tests must delete what they create.

## Environment quirks

- **kubectl:** use `export KUBECONFIG=$PWD/.kubeconfig`. Shells here don't activate mise, and `bin/kubectl` isn't on PATH.
- **python3 is 3.9:** backslashes aren't allowed inside f-string expressions. Build strings outside the f-string, or write a script file.
- **zsh doesn't word-split** an unquoted `$VAR` containing spaces: use arrays or a script.
- **Distroless images have no shell:** inspect a pod with `kubectl debug <pod> --image=busybox:1.37 --target=<container> --profile=sysadmin`, and read metrics via `kubectl port-forward`.
- **Cold storage is Silo, not MinIO:** `minio/minio` is gone from Docker Hub and `quay.io/minio/minio` doesn't pull, so don't reach for either (or `minio/mc`). Silo (`pgsty/silo`, pinned in `deploy/platform/storage/s3.yaml`) is Pigsty's maintained MinIO fork: same S3 API, `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`, `/minio/health/{ready,live}`, port 9000, service `s3.storage.svc:9000`. Its binary is `silo` (`silo server /data`), not `minio`. Use `amazon/aws-cli` with `--endpoint-url` for bucket work; credentials are in the `cold-storage` Secret. versitygw is the tested fallback (ADR 0006). Check for a newer Silo release before bumping the pin.
- **Vector at stdin EOF:** with a `stdin` source, closing stdin and letting vector exit can lose the last event on a loaded machine (CI, `-race`). Run vector from Go with `vectorrun.Stream`, which keeps stdin open until the expected output has arrived.
- **Vector `dedupe`:** a missing match field counts as a value, so every event without it is a "duplicate" of the first. Route those around it.
- **Vector timing:** its metrics endpoint comes up a few seconds after the pod is Ready. On shutdown it waits up to the 60 s grace period to drain sinks, which makes rolling back a stuck sink slow.
- **GitHub feeder:** unauthenticated, 60 requests an hour, and the API caches for 5 minutes, so it polls every few minutes and gets up to 300 events each time. It paces each poll's events over the wait before the next. Without pacing, a team's budget throttle kept the start of each burst, and push, create and delete events (later in GitHub's order) never reached hot. Don't spend the rate limit on manual `curl`s while it runs; a token goes in the optional `github-feeder` Secret (key `token`).
- **Vector metrics under load:** buffer depth is `vector_buffer_size_events` (not `vector_buffer_events`). Idle components' series expire, so for exact totals take each series' `max_over_time` rather than its value at the end. A histogram whose flow stalls (backpressure) is expired and republished from a different base, so `increase()` over it is wrong: `bench/throughput.py` refuses latency for a step with a reset. A sink's 429s are retried, not counted in `component_errors_total`; read `vector_http_client_responses_total` by status. Kafka lag can read slightly negative.
- **Counting events by ID:** a named loadgen run (`-run`) tags events with `loadgen.seq` and derives `event_id` from (run, seq), so `bench/eventcheck.py` can name every missing or duplicated event. Loki drops a byte-identical line (same stream, time and content), so a disk buffer's replay is invisible in hot; a re-read from Kafka is reprocessed (new `sink_at`) and shows. Run names must be unique: cold storage keeps old runs.
- **Drained** means committed lag 0 in Kafka (`kafka-consumer-groups.sh --describe` in the broker pod) and empty sink buffers. Vector's lag gauge keeps a partition's last value after a rebalance moves it away, so it can read a backlog forever.
- **Disk-full tests:** Vector refuses to start when its disk buffers' total `max_size` exceeds the mountpoint's capacity (3 GiB on an aggregator), so a small tmpfs can't stand in for a full disk; a big tmpfs with a filler file can.
- **Faults without touching Helm-owned fields:** `docker exec <node> pkill -STOP -x loki` (or `silo`) freezes a process from its kind node; `-CONT` resumes it.
- **Loki sizing:** stock limits (3 MB/s per stream) capped one team's hot path near 2,700 events/s, since `hot_loki` makes one stream per team and source; `deploy/platform/loki/values.yaml` raises them to 64 MB/s (≈40,000 events/s, where the benchmark saw 429s). With 1 GiB Loki was OOM-killed at 24,000 events/s and crash-looped replaying its WAL; it has 4 GiB and a 2 GB `replay_memory_ceiling`.
- **`pkill -f` matches its own shell:** a pattern like `pkill -f "kubectl.*port-forward"` is in the Bash tool's own command line, so it kills that shell (exit 144). Use `pkill -f "[k]ubectl.*port-forward"`, and in a command of its own: if the same command line also starts a `kubectl port-forward`, the pattern matches that text and kills the shell anyway.
- **Kafka source counts:** a new team's Kafka source has its own consumer group, so its events flow from the canary pod before promotion.
- **Generated files:** a new example in `examples/` changes the goldens and the pinned counts in `cmd/spillwayctl/main_test.go`.

## Open threads

- **The cloud environment hasn't been stood up yet** (M4.3): `make cloud-plan` needs AWS credentials, and `cloud-up` needs the owner's go-ahead on cost. Before then: the images of `main` must be published (`cloud-up` checks they're pullable anonymously; the packages are public). The benchmark and chaos scripts load images into kind and freeze processes through `docker exec` on kind's nodes, so they need changes to run on EKS.

- **A full disk loses acknowledged events:** Vector crashes on ENOSPC (exiting 0, so Kubernetes shows `Completed`), then truncates each disk buffer's torn tail, events whose Kafka offsets were committed: 47 lost across the sinks in the M4.2 run. Spillway's defence is a volume only Vector writes, sized for its buffers (Vector checks capacity at startup). A fix belongs upstream (an M4.5 candidate).
- **Cold storage is at-least-once:** an aggregator crash leaves duplicates in S3 (483 in the M4.2 kill); Loki drops byte-identical replays. Readers of cold storage dedupe by `spillway.event_id`.
- **Aggregator memory under overload:** at 48,000 events/s both aggregators were OOM-killed (1 GiB): the Kafka sources fetch faster than the transforms drain, in memory. They recover, but redeliver up to 5 s each (143,199 duplicates in Loki).
- **Latency knee at 3–4,000 events/s:** p99 goes from 29 ms to ~0.5–1 s while throughput keeps up to 32,000/s. Queues build through the team's transforms; disk buffers, the metrics exporter and CPU are ruled out (docs/results/m4.1-benchmark.md). Root cause open.
- **#43:** a rolled-back canary leaves new sinks' disk buffers orphaned. It's visible (metric, warning, status) but not drained.
- **False positives on other data:** measured on Wikimedia (0.015% of events). GitHub events carry real PII, so their rate can't be measured the same way. Spot checks found package specifiers (`pixi.js@8.22.0`) left unmasked, correctly, and 0 email addresses left in cold storage.
- **Dedupe is per pod and in memory:** it catches feeder replays (same `event_id` and Kafka key, so same partition and aggregator), but not duplicates from a Kafka rebalance or an aggregator restart, which land on a pod with an empty cache. The chaos checks count by event ID.
- **Budgets assume an even spread:** each aggregator enforces an equal share, so a team only gets its whole budget if its events spread evenly. Kafka partitions do that (`spillway.pods` has 6, to divide evenly among 1, 2, 3 or 6 aggregators). A 3-partition topic on 2 aggregators splits 2:1.
- **Level from free text:** a text line whose logfmt part says `level=debug` is sampled as debug even if the app meant something else; JSON and klog lines are unambiguous.
- **Canary checks:** per-team drop-rate checks join the comparison in M3. The operator's gate runs `vector validate`, not the generated tests, which run in CI and `spillwayctl`.
