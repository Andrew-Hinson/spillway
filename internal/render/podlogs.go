package render

// podLogVRL turns a pod log, as the agents send it, into an event: the line
// plus the pod's identity, with the rest of the Kubernetes metadata dropped.
//
// A line that is a JSON object is parsed, and its fields become the event's
// fields. Any other line stays in .message. Either way, the line's level is
// found and normalized into .level, so sampling (levelField: level, the
// default) works on pod logs:
//
//	JSON        "level", then "lvl", then "severity"; pino's numbers too
//	klog        I1010 12:00:00 …  (I, W, E, F)
//	leading     [INFO] …, 2026-10-10 13:03:41 INFO …, 2026-…Z  WARN vector::…
//	            (up to two timestamp tokens, then an upper-case level word)
//	logfmt      … level=info …
//
// Levels are lowercased and aliased (warning → warn, err and eror → error,
// dbug → debug), and a line with no recognizable level gets none.
//
// Pod logs reach the aggregators through Kafka as JSON, so the log time
// arrives as a string; it's parsed back into a timestamp for the Loki sinks.
//
// The pod's identity always wins: an app field that collides with one
// (namespace, pod, container, node, stream, _timestamp) is kept as app_<name>.
//
// .spillway is Spillway's own metadata, and redaction skips it, so an app
// must not be able to write arbitrary values there. Only the feeder stamp
// fields are taken from a line's "spillway" object, and only in their exact
// shapes: event_id a UUID, produced_at a timestamp, feeder a lowercase word,
// fixture {id: int, pattern: a built-in pattern}. Anything else stays in the
// event as app_spillway, where redaction sees it.
const podLogVRL = `k = object(.kubernetes) ?? {}
ts = ._timestamp
if is_string(ts) { ts = parse_timestamp(string!(ts), "%+") ?? ts }
line = to_string(.message) ?? ""
fields = {}
if starts_with(line, "{") {
  fields = object(parse_json(line) ?? null) ?? {}
}
stamp = {}
level = null
if length(fields) > 0 {
  if exists(fields._timestamp) { fields.app__timestamp = del(fields._timestamp) }
  if exists(fields.stream) { fields.app_stream = del(fields.stream) }
  if exists(fields.node) { fields.app_node = del(fields.node) }
  if exists(fields.namespace) { fields.app_namespace = del(fields.namespace) }
  if exists(fields.pod) { fields.app_pod = del(fields.pod) }
  if exists(fields.container) { fields.app_container = del(fields.container) }
  sp = del(fields.spillway)
  if sp != null && !is_object(sp) {
    fields.app_spillway = sp
    sp = {}
  }
  sp = object(sp) ?? {}
  id = string(sp.event_id) ?? ""
  if match(id, r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') {
    stamp.event_id = id
    del(sp.event_id)
  }
  produced_at, err = parse_timestamp(string(sp.produced_at) ?? "", "%+")
  if err == null {
    stamp.produced_at = format_timestamp!(produced_at, "%+")
    del(sp.produced_at)
  }
  feeder = string(sp.feeder) ?? ""
  if match(feeder, r'^[a-z]{1,16}(-[a-z]{1,16})?$') {
    stamp.feeder = feeder
    del(sp.feeder)
  }
  fixture_id = int(sp.fixture.id) ?? null
  fixture_pattern = string(sp.fixture.pattern) ?? ""
  if fixture_id != null && length(object(sp.fixture) ?? {}) == 2 && includes(["ssn", "email", "phone", "memberId"], fixture_pattern) {
    stamp.fixture = {"id": fixture_id, "pattern": fixture_pattern}
    del(sp.fixture)
  }
  if length(sp) > 0 { fields.app_spillway = sp }
  level = fields.level
  if level == null { level = fields.lvl }
  if level == null { level = fields.severity }
} else {
  fields.message = .message
  found = parse_regex(line, r'^(?P<level>[IWEF])\d{4} ') ??
    parse_regex(line, r'^(?:\d\S*\s+){0,2}\[?(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL)\]?(?:\s|$)') ??
    parse_regex(line, r'(?:^|\s)(?:level|lvl)="?(?P<level>[A-Za-z]+)') ??
    {}
  level = found.level
}
level = downcase(to_string(level) ?? "")
aliases = {
  "i": "info", "w": "warn", "e": "error", "f": "fatal",
  "warning": "warn", "err": "error", "eror": "error", "dbug": "debug", "crit": "critical",
  "10": "trace", "20": "debug", "30": "info", "40": "warn", "50": "error", "60": "fatal"
}
level = string(get(aliases, [level]) ?? null) ?? level
if match(level, r'^[a-z]{1,16}$') { fields.level = level }
. = merge(fields, {
  "_timestamp": ts,
  "stream": .stream,
  "node": k.pod_node_name,
  "namespace": k.pod_namespace,
  "pod": k.pod_name,
  "container": k.container_name
})
if length(stamp) > 0 { .spillway = stamp }
`
