package render

// Tests that run rendered components in the real vector binary, for
// behaviour `vector test` can't check (how many events come out) or that
// needs many cases (the pod-log formats).

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/redact"
	"github.com/Andrew-Hinson/spillway/internal/redact/vectorrun"
)

func vectorBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("VECTOR_BIN")
	if bin == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("VECTOR_BIN is not set; run the tests with `make test`")
		}
		t.Skip("VECTOR_BIN is not set (run `make test`)")
	}
	return bin
}

// runTransforms feeds events (JSON objects) from stdin into the transform
// entry, and returns what reaches a console sink reading outputs, in order
// per path. Each event in sentinels is sent last and must come out: one per
// path through the transforms, so that once all of them have arrived, so has
// everything sent before them. They're left out of the result.
func runTransforms(t *testing.T, transforms map[string]any, entry string, outputs, events, sentinels []string) []map[string]any {
	t.Helper()
	dir := t.TempDir()
	transforms[entry].(map[string]any)["inputs"] = []string{"in"}
	cfg, err := yaml.Marshal(map[string]any{
		"data_dir":   dir,
		"sources":    map[string]any{"in": map[string]any{"type": "stdin", "decoding": map[string]any{"codec": "json"}}},
		"transforms": transforms,
		"sinks": map[string]any{"out": map[string]any{
			"type": "console", "inputs": outputs, "encoding": map[string]any{"codec": "json"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "vector.yaml")
	if err := os.WriteFile(path, cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	waiting := len(sentinels)
	input := strings.Join(append(slices.Clone(events), sentinels...), "\n") + "\n"
	err = vectorrun.Stream(context.Background(), vectorBin(t), path, []byte(input), func(line []byte) (bool, error) {
		var e map[string]any
		if err := json.Unmarshal(line, &e); err != nil {
			return false, err
		}
		if e["sentinel"] == true {
			waiting--
		} else {
			out = append(out, e)
		}
		return waiting == 0, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// renderedTransforms renders one pipeline and returns its transforms.
func renderedTransforms(t *testing.T, p spillwayv1alpha1.LogPipeline) map[string]any {
	t.Helper()
	b, err := Render([]spillwayv1alpha1.LogPipeline{p}, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct{ Transforms map[string]any }
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg.Transforms
}

func TestDedupeDropsRepeatedIDs(t *testing.T) {
	all := renderedTransforms(t, loadExamples(t)["edits"])
	transforms := map[string]any{"edits_ids": all["edits_ids"], "edits_dedupe": all["edits_dedupe"]}
	out := runTransforms(t, transforms, "edits_ids", []string{"edits_dedupe", "edits_ids._unmatched"}, []string{
		`{"message": "a", "spillway": {"event_id": "id-a"}}`,
		`{"message": "a replayed", "spillway": {"event_id": "id-a"}}`,
		`{"message": "b", "spillway": {"event_id": "id-b"}}`,
		// Events without an ID are never duplicates, even of each other.
		`{"message": "x"}`,
		`{"message": "y"}`,
		`{"message": "x"}`,
		`{"message": "z", "spillway": {"team": "t"}}`,
	}, []string{
		`{"sentinel": true, "spillway": {"event_id": "id-sentinel"}}`,
		`{"sentinel": true}`,
	})
	var got []string
	for _, e := range out {
		got = append(got, e["message"].(string))
	}
	want := map[string]int{"a": 1, "b": 1, "x": 2, "y": 1, "z": 1}
	counts := map[string]int{}
	for _, m := range got {
		counts[m]++
	}
	if len(got) != 6 || len(counts) != len(want) {
		t.Fatalf("got %q, want a, b, x, y, x, z in any order", got)
	}
	for m, n := range want {
		if counts[m] != n {
			t.Errorf("%q came out %d times, want %d (all: %q)", m, counts[m], n, got)
		}
	}
}

// podLog is a pod log as the agents send it.
func podLog(line string) map[string]any {
	return map[string]any{
		"message": line, "stream": "stdout", "_timestamp": "2026-10-10T13:00:00Z",
		"kubernetes": map[string]any{
			"pod_namespace": "ns", "pod_name": "app-0", "container_name": "app", "pod_node_name": "node-1",
			"pod_ip": "10.0.0.1", "pod_labels": map[string]any{"app": "x"},
		},
	}
}

// The first lines of each group are verbatim from the kind stack's own pods.
var podLogLevels = []struct{ line, level string }{
	// JSON: slog (feeders), Python, pino numbers, GCP severity
	{`{"time":"2026-10-10T13:00:39.119704582Z","level":"INFO","msg":"stream connected","resumed":true}`, "info"},
	{`{"time": "2026-10-10T11:24:25.021136+00:00", "level": "INFO", "msg": "Loading incluster config..."}`, "info"},
	{`{"level":30,"msg":"pino"}`, "info"},
	{`{"level":50,"msg":"pino"}`, "error"},
	{`{"lvl":"dbug","msg":"x"}`, "debug"}, // log15
	{`{"severity":"WARNING","message":"gcp"}`, "warn"},
	{`{"msg":"no level"}`, ""},
	{`{"level":{"nested":true},"msg":"odd"}`, ""},
	// klog (kube-apiserver, local-path-provisioner)
	{`I1010 13:01:34.612300       1 cidrallocator.go:278] updated ClusterIP allocator for Service CIDR 10.96.0.0/16`, "info"},
	{`W1010 13:01:34.612300       1 x.go:1] careful`, "warn"},
	{`E1010 13:01:34.612300       1 x.go:1] boom`, "error"},
	// A leading level word (Kafka, Strimzi, CoreDNS, Vector)
	{`2026-10-10 13:03:41 INFO  [quorum-controller-0-event-handler] EventPerformanceMonitor:174 - [QuorumController id=0] ok`, "info"},
	{`[INFO] plugin/ready: Plugins not ready: "kubernetes"`, "info"},
	{`2026-10-08T21:42:16.215345Z ERROR source{component_kind="source" component_id=wikimedia}: vector::internal_events::kafka: x`, "error"},
	{`2026-10-08T21:41:36.105982Z  WARN vector::app: x`, "warn"},
	// logfmt (Loki, Grafana, Prometheus)
	{`level=info ts=2026-10-10T13:03:36.193905183Z caller=index_set.go:186 msg="cleaning up unwanted indexes"`, "info"},
	{`logger=plugins.update.checker t=2026-10-10T13:01:48.542693181Z level=info msg="flag evaluation succeeded"`, "info"},
	{`time=2026-10-10T13:00:04.397Z level=INFO source=head.go:1601 msg="WAL checkpoint complete"`, "info"},
	{`time="2026-10-08T21:42:17Z" level=debug msg="Provisioner started"`, "debug"},
	// No level
	{`GET /search?level=debug 200`, ""},
	{`Information about INFO levels`, ""},
	{`{not json`, ""},
	{`[1, 2, 3]`, ""},
	{``, ""},
}

func TestPodLogLevels(t *testing.T) {
	events := make([]map[string]any, len(podLogLevels))
	for i, c := range podLogLevels {
		events[i] = podLog(c.line)
	}
	out, err := vectorrun.Remap(context.Background(), vectorBin(t), podLogVRL, events)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range podLogLevels {
		e := out[i]
		got, _ := e["level"].(string)
		if c.level == "" && e["level"] != nil && !strings.HasPrefix(c.line, "{") {
			t.Errorf("%q: got level %v, want none", c.line, e["level"])
		} else if c.level != "" && got != c.level {
			t.Errorf("%q: got level %v, want %q", c.line, e["level"], c.level)
		}
		if e["namespace"] != "ns" || e["pod"] != "app-0" || e["container"] != "app" || e["node"] != "node-1" || e["kubernetes"] != nil {
			t.Errorf("%q: pod identity not kept: %v", c.line, e)
		}
		if _, isJSON := e["message"]; !isJSON && !strings.HasPrefix(c.line, "{") {
			t.Errorf("%q: a text line lost its message: %v", c.line, e)
		}
	}
}

func TestPodLogJSONFields(t *testing.T) {
	fixture := `{"time":"2026-10-10T13:00:00Z","level":"warn","msg":"account update failed","account":{"contact":"x"},` +
		`"spillway":{"event_id":"0b0e7f5e-0c55-4c43-9a51-2d4e4f8c1b0a","produced_at":"2026-10-10T13:00:00.123Z","feeder":"fixtures","fixture":{"id":7,"pattern":"ssn"}}}`
	collide := `{"msg":"m","namespace":"spoofed","pod":"p","container":"c","node":"n","stream":"s","_timestamp":"t","spillway":"just a string"}`
	out, err := vectorrun.Remap(context.Background(), vectorBin(t), podLogVRL, []map[string]any{podLog(fixture), podLog(collide)})
	if err != nil {
		t.Fatal(err)
	}

	f := out[0]
	sp, _ := f["spillway"].(map[string]any)
	if sp["event_id"] != "0b0e7f5e-0c55-4c43-9a51-2d4e4f8c1b0a" || sp["feeder"] != "fixtures" || sp["produced_at"] == nil {
		t.Errorf("fixture stamp not kept: %v", sp)
	}
	if fx, _ := sp["fixture"].(map[string]any); fx["id"] != 7.0 || fx["pattern"] != "ssn" {
		t.Errorf("fixture tag not kept: %v", sp)
	}
	if f["message"] != nil || f["msg"] != "account update failed" || f["app_spillway"] != nil {
		t.Errorf("fixture line not lifted cleanly: %v", f)
	}

	c := out[1]
	for _, k := range []string{"namespace", "pod", "container", "node", "stream", "_timestamp"} {
		if c["app_"+k] == nil {
			t.Errorf("colliding app field %s not kept as app_%s: %v", k, k, c)
		}
	}
	if c["namespace"] != "ns" || c["app_spillway"] != "just a string" || c["spillway"] != nil {
		t.Errorf("collisions mishandled: %v", c)
	}
}

// An app can't use .spillway, which redaction skips, to get PII past it: a
// pod log run through the pod-log step and redaction carries no PII value
// anywhere, whatever shape the app gave its "spillway" object.
func TestPodLogSpillwayCantCarryPII(t *testing.T) {
	pii := []string{"123-45-6789", "jo.doe@example.com", "(555) 867-5309", "MBR-12345678"}
	var lines []string
	for _, v := range pii {
		for _, sp := range []string{
			`{"event_id": %q}`, `{"feeder": %q}`, `{"produced_at": %q}`, `{"note": %q}`, `{"team": %q}`,
			`{"fixture": {"id": 1, "pattern": "ssn", "extra": %q}}`, `{"fixture": {"id": %q, "pattern": "ssn"}}`,
			`[%q]`, `%q`,
		} {
			lines = append(lines, `{"msg":"m","spillway":`+strings.ReplaceAll(sp, "%q", `"`+v+`"`)+`}`)
		}
	}
	vrl, err := redact.VRL(redact.All())
	if err != nil {
		t.Fatal(err)
	}
	events := make([]map[string]any, len(lines))
	for i, l := range lines {
		events[i] = podLog(l)
	}
	out, err := vectorrun.Remap(context.Background(), vectorBin(t), podLogVRL+vrl, events)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range out {
		b, _ := json.Marshal(e)
		for _, v := range pii {
			if strings.Contains(string(b), v) {
				t.Errorf("%s leaked %q: %s", lines[i], v, b)
			}
		}
	}
}
