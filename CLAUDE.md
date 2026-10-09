# Spillway: notes for Claude Code sessions

Spillway is a Kubernetes log platform: one `LogPipeline` spec per team becomes a running, tested Vector pipeline (collection, PII redaction, sampling, budgets, hot/cold routing). It runs on live public data, and every claim in the README is backed by a measurement in `docs/results`.

## Where the state lives

- **Progress:** [`docs/plan.md`](docs/plan.md) has the milestones, the gate for each, and a checkbox per work item. Each item is a GitHub issue titled `[Mx.y] …` with its acceptance criteria. `gh issue list` shows what's open.
- **Decisions:** [`docs/adr/`](docs/adr). 0001: validation with Vector bundled in the operator image. 0002: canary rollout via StatefulSet partition, quarantine and last-good specs.
- **Measurements:** [`docs/results/`](docs/results): the M1 baseline and the M2 gate run.
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
| `make tools` | Pinned kind, kubectl, helm, golangci-lint, controller-gen, setup-envtest and vector, in `./bin` |

## How the system fits together

- **Renderer** (`internal/render`): all LogPipelines → one aggregator config, deterministically.
  - Components are named `<team>_<stage>`; sinks (`hot_loki`, `cold_s3`) are shared.
  - Data no team claims takes the M1 platform paths (`wikimedia → stamp → loki`, `agents → pod_logs → loki_pods`). Claimed topics and namespaces leave them, so redaction can't be bypassed.
  - With no pipelines, it renders exactly the M1 config.
  - It also generates `vector test` cases for every transform it emits (`tests.go`), and a Go test enforces that every transform is covered.
- **Operator** (`internal/controller`): renders → applies quarantine → `vector validate` (cached) → writes `vector-<hash>.yaml` to the ConfigMap → canaries via partition → promotes or rolls back.
  - State lives in StatefulSet annotations (`spillway.dev/*`).
  - Pods select their config file through `$(SPILLWAY_CONFIG_SUFFIX)` from a pod annotation.
  - Cold storage is **off** unless `--cold-bucket` is set: MinIO arrives in M3.5, and until then cold-routed pipelines are `Invalid`.
- **Field ownership:** the operator owns only fields Helm never sets: the config-suffix pod annotation, `updateStrategy.partition`, and its own annotations. Never `kubectl set`/`patch` a Helm-owned field, or the next `helm upgrade` fails with a server-side-apply conflict. If it happens, set the field back to Helm's value.
- **Redaction** (`internal/redact`): VRL `redact()` filters. SSN is a deliberately broad `\b\d{3}-\d{2}-\d{4}\b`, because VRL's built-in SSN matcher misses valid SSNs.
- **Canary timing:** each config change takes ~2.5–4 min to reach both aggregators (2 min bake plus two restarts). Bake ≥ 2× the longest sink batch timeout. Error allowance is max(5, 0.1% of events).

## Testing expectations

- **Prove tests can fail:** break the code on purpose (e.g. drop a redaction filter) and check that the test catches it.
- **Confirm CI ran the tests:** check that CI logs show the envtest and vector tests actually running (`gh run view --log`), not skipped.
- **Read real output:** for Vector behaviour, run the real binary (`bin/vector test`, `vector validate`) or scrape a live pod's `:9598/metrics`. Don't assume what Vector reports.
- **Clean up after controller tests:** envtest tests share one API server, and the reconciler sees LogPipelines in every namespace, so tests must delete what they create.

## Environment quirks

- **kubectl:** use `export KUBECONFIG=$PWD/.kubeconfig`. Shells here don't activate mise, and `bin/kubectl` isn't on PATH.
- **python3 is 3.9:** backslashes aren't allowed inside f-string expressions. Build strings outside the f-string, or write a script file.
- **zsh doesn't word-split** an unquoted `$VAR` containing spaces: use arrays or a script.
- **Distroless images have no shell:** inspect a pod with `kubectl debug <pod> --image=busybox:1.37 --target=<container> --profile=sysadmin`, and read metrics via `kubectl port-forward`.
- **Vector timing:** its metrics endpoint comes up a few seconds after the pod is Ready. On shutdown it waits up to the 60 s grace period to drain sinks, which makes rolling back a stuck sink slow.
- **Kafka source counts:** a new team's Kafka source has its own consumer group, so its events flow from the canary pod before promotion.
- **Generated files:** a new example in `examples/` changes the goldens and the pinned counts in `cmd/spillwayctl/main_test.go`.

## Open threads

- **#43:** a rolled-back canary leaves new sinks' disk buffers orphaned. It's visible (metric, warning, status) but not drained.
- **SSN false positives:** the broad SSN pattern's false-positive rate is unmeasured; M3.2 should measure it.
- **Pod logs aren't parsed:** they arrive as an unparsed `message` string, so their levels can't be sampled and fixture tags sit inside the message.
- **Fixtures through the platform path:** the M1 platform path doesn't redact, so fixtures there land in Loki unredacted. That's why `make fixtures` is opt-in.
- **Canary checks:** per-team drop-rate checks join the comparison in M3. The operator's gate runs `vector validate`, not the generated tests, which run in CI and `spillwayctl`.
