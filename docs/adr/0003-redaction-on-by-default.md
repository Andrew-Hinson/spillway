# ADR 0003: Redaction on by default, with boundary-free patterns

Status: proposed · 2026-10-09 · M3.2

## Context

M2 shipped redaction as opt-in: a LogPipeline masked PII only if it listed `redaction.patterns`, and the platform paths for unclaimed data (the feeder topic and pod logs from unclaimed namespaces) never masked anything. M3's gate asks for zero injected fixture values in the hot sink. Under opt-in, that holds only while every team reading a fixture-carrying source remembers to turn redaction on, and while nothing reaches Loki through the platform paths. M3.1 had to make `make fixtures` opt-in for that reason.

Testing the patterns against a corpus (`internal/redact/testdata/corpus.yaml`) found a second problem. All four used `\b` word boundaries, and PII in logs often sits next to a letter or digit:

- **Escaped JSON:** pod logs arrive as unparsed strings, so `{"note":"x\n123-45-6789"}` puts an `n` right before the SSN.
- **Glued to a label or extension:** `SSN123-45-6789`, `555-867-5309x12`, `id\tMBR-12345678`.
- **Unicode-escaped:** ` 123-45-6789` puts a digit right before the SSN.

`\b` fails in each case, so the value passes through unmasked. The old patterns let 18 of the corpus's mask cases through on the SSN and member-ID patterns alone. VRL's regex engine (Rust's `regex` crate) has no lookbehind, so the patterns can't say "not next to a digit" either.

## Decision

1. **Teams get redaction by default.** A LogPipeline without `redaction`, or with `redaction: {}`, masks every pattern. `redaction.patterns` narrows that to a list. `redaction: {disabled: true}` turns it off, and it's the only way to do so. A CEL rule rejects `disabled` combined with `patterns`.
2. **Platform paths redact everything.** Unclaimed data gets the strictest policy, because no team has said it's PII-free: `wikimedia → redact_feed → stamp → loki` and `agents → pod_logs → redact_pods → loki_pods`. That's why `make fixtures` is now safe whether or not a team claims the injector's data.
3. **No word boundaries.** Anything PII-shaped is masked, even inside a longer run of digits (`1123-45-67890`). The email pattern also accepts letters in any script and `%40` (URL-encoded `@`). The corpus lists these over-masks (`overMasked`) and the known misses (`notMasked`), and the test fails if either list stops being true.

## Consequences

Good:
- **Compliance holds without anyone remembering it.** A new team, a forgotten field or an unclaimed namespace can't leak, and turning redaction off is visible in the spec and in review.
- **Measured cost.** On 20,004 live Wikimedia events, the patterns changed 3 events (0.015%). The M3.1 `\b` patterns changed 1. The extra two were SSN-shaped digit runs inside file names; [`docs/results/m3.2-redaction.md`](../results/m3.2-redaction.md) has the details.
- **Escaped-JSON pod logs are covered** before M3 starts parsing them.
- **Zero leaks, end to end:** 5,054 fixtures through team and platform paths in kind, 0 values in Loki. With one team's redaction disabled, the same check caught all 234 of its fixtures.

Bad:
- **More over-masking:** digit runs with a PII-shaped middle (`12345-67-89012`, `1.234.567.8900`) and anything address-shaped (`git@github.com`) get masked. In a log platform, a masked ID costs a debugging detour, and a leaked SSN costs an incident.
- **CPU on every path:** four regexes run over every string of every event, including unclaimed data. At Wikimedia's ~30 events/s that's negligible. M4's benchmark will measure it at load.
- **Behaviour change:** existing specs without `redaction` start masking on upgrade. That's acceptable in v1alpha1 with no external users, and it's the point.
- **M1's config is no longer the zero-pipeline render.** The platform paths gain a redact step. The M1 baseline was measured without it.

Alternatives considered:
- **Keep opt-in, and lint or warn on pipelines without redaction:** warnings get ignored, and it doesn't cover the platform paths.
- **A "consumed" boundary instead of `\b`** (`(^|\D)\d{3}-…`): the masked value swallows a neighbouring character, and two values separated by one character mask only the first, so it still leaks.
- **A recursive VRL walk with `replace()` and capture groups** to put the boundary character back: the same adjacency problem, more VRL to maintain, and it loses `redact()`'s handling of nested objects and arrays.

Revisit if the false-positive rate on a second feeder (M3.7, GitHub events) is much higher, or if a team needs per-field exemptions rather than all-or-nothing.
