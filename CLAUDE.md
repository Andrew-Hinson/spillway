# Spillway: notes for Claude Code sessions

Spillway is a Kubernetes log platform: one `LogPipeline` spec per team becomes a running, tested Vector pipeline (collection, PII redaction, sampling, budgets, hot/cold routing). It runs on live public data, and every claim in the README is backed by a measurement in `docs/results`.

## Where the state lives

- **Progress:** [`docs/plan.md`](docs/plan.md) has the milestones, the gate for each, and a checkbox per work item. Each item is a GitHub issue titled `[Mx.y] …` with its acceptance criteria. `gh issue list` shows what's open.
- **Decisions:** [`docs/adr/`](docs/adr). 0001: validation with Vector bundled in the operator image. 0002: canary rollout via StatefulSet partition, quarantine and last-good specs. 0003: redaction on by default, with boundary-free patterns. 0004: pod logs parsed, with only the feeder stamp trusted from an app's `spillway`. 0005: budgets split evenly across the aggregators, over a 10 s window, with pod logs through Kafka so they spread evenly. 0006: versitygw, not MinIO, for local cold storage (MinIO's images are no longer published).
- **Measurements:** [`docs/results/`](docs/results): the M1 baseline, the M2 gate run, M3.2's redaction false positives and leak check, M3.3's sampling, dedupe and per-team drops, M3.4's budgets, and M3.5's cold path.
- **Why something is the way it is:** the PR descriptions (`gh pr view N`) record the design, what testing found, and known limitations.

## Workflow for a work item

1. **Branch** from an up-to-date `main`: `git checkout main && git pull --ff-only && git checkout -b mX.Y-short-name`.
2. **Read the issue** (`gh issue view N`) and the plan's gate for the milestone, then build to the acceptance criteria.
3. **Check locally:** `make lint`, `make test`, `make generate` (must leave no diff; CI checks), and `make vector-check`.
4. **Test end to end in kind** for anything that touches the running system: `make up` from scratch, then apply real specs and query Loki and Prometheus. Unit tests alone have repeatedly missed real bugs here: VRL type errors, Vector's retry behaviour, the SSN matcher.
5. **Update docs:** tick the plan's checkbox, update the README status and layout if they changed, and add or update an ADR for a significant design decision.
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
| `make fixtures RATE=N` | Fictional-PII injector (opt-in) |
| `make m2-gate`, `make baseline` | Recorded measurements; see `docs/results` |
| `make leak-check WINDOW=15m` | Search every hot-sink stream for fixture values; fails on a leak or if no fixtures arrived |
| `make sampling-check WINDOW=10m REF=edits=wiki-all` | Each team's dedupe, sampling and budget drops; per-level kept share against an unsampled reference team |
| `make cold-check` | Sync the cold bucket and check every object: `team=…/date=…/` keys matching their events, gzip, no fixture values |
| `make storage` | Local S3 (versitygw in `storage`), the `spillway-cold` bucket, and generated credentials in the `cold-storage` Secret (also copied to `vector`) |
| `make redaction-fp DURATION=600` | Capture live Wikimedia events and count what each redaction pattern masks |
| `make tools` | Pinned kind, kubectl, helm, golangci-lint, controller-gen, setup-envtest and vector, in `./bin` |

## How the system fits together

- **Renderer** (`internal/render`): all LogPipelines → one aggregator config, deterministically.
  - Components are named `<team>_<stage>`; sinks (`hot_loki`, `cold_s3`) are shared. `metrics_by_team` tags each team component's internal metrics with `team`, so drops and throughput sum per team.
  - Stages: `in` → `ids`/`dedupe` (on `spillway.event_id`; events without one bypass it) → `redact` → cold split → `sample` → `budget` → `hot`.
  - `budget` is a throttle per aggregator pod, with no shared state: each gets `floor(maxEventsPerSec / replicas)` over a 10 s window (`BudgetPerPod`). The operator renders with the StatefulSet's replica count; `render.DefaultOptions` (and so `spillwayctl` and the goldens) assumes 2.
  - Data no team claims takes the platform paths (`wikimedia → redact_feed → stamp → loki`, `agents → pod_logs → redact_pods → loki_pods`), which mask every pattern. Claimed topics and namespaces leave them.
  - With no pipelines, it renders the platform config (`vector/aggregator/vector.yaml`): M1 plus redaction.
  - It also generates `vector test` cases for every transform it emits (`tests.go`), and a Go test enforces that every transform is covered.
- **Operator** (`internal/controller`): renders → applies quarantine → `vector validate` (cached) → writes `vector-<hash>.yaml` to the ConfigMap → canaries via partition → promotes or rolls back.
  - State lives in StatefulSet annotations (`spillway.dev/*`).
  - Pods select their config file through `$(SPILLWAY_CONFIG_SUFFIX)` from a pod annotation.
  - Cold storage is on in kind: `deploy/operator/operator.yaml` passes `--cold-bucket=spillway-cold --cold-endpoint=http://s3.storage.svc:7070`. Without `--cold-bucket`, cold-routed pipelines are `Invalid`. `bench/m2_gate.sh` swaps the endpoint for a broken one and restores the manifest's args afterwards.
- **Field ownership:** the operator owns only fields Helm never sets: the config-suffix pod annotation, `updateStrategy.partition`, and its own annotations. Never `kubectl set`/`patch` a Helm-owned field, or the next `helm upgrade` fails with a server-side-apply conflict. If it happens, set the field back to Helm's value.
- **Pod logs** travel agent → Kafka topic `spillway.pods` → the aggregators' shared `agents` source (consumer group `spillway-pods`, from the oldest offset). The agent produces with no key, so batches spread over the partitions. Parsing (`internal/render/podlogs.go`): a JSON line's fields become the event's fields; any other line stays in `message`. Every line gets a normalized `level` when one is recognizable (JSON, klog, a leading level word, logfmt). An app's `spillway` object is trusted only in the exact feeder-stamp shapes, because redaction skips `.spillway`; the rest lands in `app_spillway`, which is redacted. `TestPodLogSpillwayCantCarryPII` guards this.
- **Redaction** (`internal/redact`): VRL `redact()` filters, on by default (a spec opts out with `redaction: {disabled: true}`). The patterns have no `\b`: escaped JSON in pod logs (`\n123-45-6789`) defeats word boundaries, and VRL's regex has no lookbehind. `testdata/corpus.yaml` holds the positive and negative cases. Run a candidate pattern against live data with `go run ./bench/redactfp -filter name=r'…'` before changing one.
- **Canary timing:** each config change takes ~2.5–4 min to reach both aggregators (2 min bake plus two restarts). Bake ≥ 2× the longest sink batch timeout. Error allowance is max(5, 0.1% of events).

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
- **Vector at stdin EOF:** with a `stdin` source, closing stdin and letting vector exit can lose the last event on a loaded machine (CI, `-race`). Run vector from Go with `vectorrun.Stream`, which keeps stdin open until the expected output has arrived.
- **Vector `dedupe`:** a missing match field counts as a value, so every event without it is a "duplicate" of the first. Route those around it.
- **Vector timing:** its metrics endpoint comes up a few seconds after the pod is Ready. On shutdown it waits up to the 60 s grace period to drain sinks, which makes rolling back a stuck sink slow.
- **Kafka source counts:** a new team's Kafka source has its own consumer group, so its events flow from the canary pod before promotion.
- **Generated files:** a new example in `examples/` changes the goldens and the pinned counts in `cmd/spillwayctl/main_test.go`.

## Open threads

- **#43:** a rolled-back canary leaves new sinks' disk buffers orphaned. It's visible (metric, warning, status) but not drained.
- **False positives on other data:** measured only on Wikimedia (0.015% of events). Re-measure on GitHub events in M3.7.
- **Dedupe is per pod and in memory:** it catches feeder replays (same `event_id` and Kafka key, so same partition and aggregator), but not duplicates from a Kafka rebalance or an aggregator restart, which land on a pod with an empty cache. M4's chaos runs still dedupe by event ID when they count.
- **Budgets assume an even spread:** each aggregator enforces an equal share, so a team only gets its whole budget if its events spread evenly. Kafka partitions do that (`spillway.pods` has 6, to divide evenly among 1, 2, 3 or 6 aggregators). A 3-partition topic on 2 aggregators splits 2:1.
- **Level from free text:** a text line whose logfmt part says `level=debug` is sampled as debug even if the app meant something else; JSON and klog lines are unambiguous.
- **Canary checks:** per-team drop-rate checks join the comparison in M3. The operator's gate runs `vector validate`, not the generated tests, which run in CI and `spillwayctl`.
