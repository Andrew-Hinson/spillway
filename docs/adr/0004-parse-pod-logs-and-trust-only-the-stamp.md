# ADR 0004: Parse pod logs, and trust only the stamp in an app's `spillway`

Status: accepted · 2026-10-10 · M3.3

## Context

Pod logs reached the aggregator as one unparsed string in `.message`. Sampling reads a level from a field (`levelField`, default `level`), so on pod logs it never found one and kept everything: the `payments` example's `debug: 0, info: 25` did nothing, and no test noticed, because the generated tests insert events that already have a `level` field.

The kind stack's own pods log in at least seven formats: JSON from Go's slog and from Python, logfmt (Loki, Grafana, Prometheus), klog (`I1010 …`, from Kubernetes components), a leading level word (`2026-10-10 13:03:41 INFO …` from Kafka and Strimzi, `[INFO]` from CoreDNS, `…Z  INFO vector::…` from Vector), and plain text.

Parsing JSON lines into fields raises a trust problem. Spillway keeps its own metadata in `.spillway` (team, source, `event_id`, `produced_at`, the fixture tag), and redaction deliberately skips `.spillway` so it can't damage that metadata. If an app's own `"spillway"` key were merged in, anything the app put there would skip redaction too.

## Decision

1. **Parse JSON lines into fields.** A line that is a JSON object becomes the event's fields, and the raw string is dropped. Any other line stays in `.message`.
2. **Normalize a level on every line.** It comes from JSON `level`, `lvl` or `severity` (pino's numbers included), a klog prefix, an upper-case level word after at most two timestamp tokens, or logfmt `level=`. It's lowercased and aliased (`warning` → `warn`, `err`/`eror` → `error`, `dbug` → `debug`). A line with no recognizable level gets none.
3. **The pod's identity wins collisions.** An app field named `namespace`, `pod`, `container`, `node`, `stream` or `_timestamp` is kept as `app_<name>`.
4. **Trust only the stamp, and only in its exact shapes.** From an app's `spillway` object, only these move into `.spillway`: `event_id` (a UUID), `produced_at` (a timestamp, reformatted), `feeder` (lower-case words) and `fixture` (exactly `{id: int, pattern: <a built-in pattern>}`). None of those shapes can hold an SSN, email, phone number or member ID. Everything else, including a `spillway` value that isn't an object, goes to `app_spillway`, which redaction sees.
5. The same parser runs on the team paths and on the platform path for unclaimed pods.

## Consequences

Good:
- **Sampling works on pod logs:** in kind, `checkout`'s info lines were kept at 24% (JSON 24.5%, logfmt 22.8%) against a spec of 25, with debug dropped and every klog warning kept ([results](../results/m3.3-sampling.md)).
- **Pod-log fields are queryable** in Loki with `| json`, without first unpacking an escaped string. Fixture tags are real fields.
- **Guarded by tests:** `TestPodLogSpillwayCantCarryPII` sends all four PII patterns through every `spillway` shape an app could try, parses and redacts them in the real Vector binary, and fails if any value survives. Loosening the `event_id` check or trusting the whole object both fail it.

Bad:
- **The event shape changes** for JSON pod logs: fields at the top level, no `message` unless the app logged one. Queries written against the old escaped string need updating.
- **A level in free text can mislead:** a text line containing ` level=debug` is sampled as debug.
- **Multi-line messages aren't joined.** Continuation lines (stack traces, config dumps) carry no level and are always kept.
- **Regex cost** on every non-JSON pod log line: three anchored regexes at most.

Alternatives considered:
- **Extract only the level and leave the line unparsed:** smaller change, but fields stay buried in an escaped string, and the fixture tag stays unreadable to anything but a regex.
- **Nest the parsed object under `.log`:** no collisions, but the default `levelField: level` would still miss it, and every query would need the prefix.
- **Drop an app's `spillway` key entirely:** simplest and safe, but then fixture tags on pod logs (the leak check's denominator) and producer timestamps (pod-log latency) are lost.

Revisit if a team needs a different level field per source, or if multi-line joining (Vector's `reduce` or the agent's `auto_partial_merge`) becomes necessary for stack traces.
