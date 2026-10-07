package render

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
)

// Regenerate the golden files with: go test ./internal/render -update
var update = flag.Bool("update", false, "rewrite the golden files in testdata/")

func loadExamples(t *testing.T) map[string]spillwayv1alpha1.LogPipeline {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	out := map[string]spillwayv1alpha1.LogPipeline{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var lp spillwayv1alpha1.LogPipeline
		if err := yaml.UnmarshalStrict(b, &lp); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		lp.Namespace = "default"
		out[strings.TrimSuffix(filepath.Base(f), ".yaml")] = lp
	}
	return out
}

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	goldenAt(t, filepath.Join("testdata", name+".yaml"), got)
}

func goldenAt(t *testing.T, path string, got []byte) {
	t.Helper()
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run `go test ./internal/render -update` to create it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("rendered config differs from %s; if the change is intended, run `go test ./internal/render -update` and review the diff.\n--- got ---\n%s", path, got)
	}
}

func TestExamplesMatchGoldenFiles(t *testing.T) {
	examples := loadExamples(t)
	var all []spillwayv1alpha1.LogPipeline
	for name, lp := range examples {
		t.Run(name, func(t *testing.T) {
			got, err := Render([]spillwayv1alpha1.LogPipeline{lp}, DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			golden(t, name, got)
		})
		all = append(all, lp)
	}
	t.Run("all-examples", func(t *testing.T) {
		got, err := Render(all, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		golden(t, "all-examples", got)
	})
}

func TestRenderIsDeterministic(t *testing.T) {
	var all []spillwayv1alpha1.LogPipeline
	for _, lp := range loadExamples(t) {
		all = append(all, lp)
	}
	first, err := Render(all, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	// Reverse the input order: the output must not change.
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	for range 5 {
		again, err := Render(all, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(first, again) {
			t.Fatal("rendering the same pipelines twice gave different output")
		}
	}
}

func TestConflictsAreRejected(t *testing.T) {
	pipeline := func(name, team string, namespaces ...string) spillwayv1alpha1.LogPipeline {
		lp := spillwayv1alpha1.LogPipeline{}
		lp.Namespace, lp.Name = "default", name
		lp.Spec.Team = team
		lp.Spec.Sources = []spillwayv1alpha1.Source{{
			Name:       "pods",
			Kubernetes: &spillwayv1alpha1.KubernetesSource{Namespaces: namespaces},
		}}
		return lp
	}
	yes := true
	coldOnly := pipeline("archive", "archive", "archive")
	coldOnly.Spec.Routing = &spillwayv1alpha1.Routing{Cold: &yes}
	noCold := DefaultOptions()
	noCold.Cold = nil

	cases := []struct {
		name      string
		pipelines []spillwayv1alpha1.LogPipeline
		opts      Options
		want      string
	}{
		{"same team twice", []spillwayv1alpha1.LogPipeline{pipeline("a", "search", "s1"), pipeline("b", "search", "s2")},
			DefaultOptions(), `team "search" is claimed by both default/a and default/b`},
		{"same namespace twice", []spillwayv1alpha1.LogPipeline{pipeline("a", "one", "shared"), pipeline("b", "two", "shared")},
			DefaultOptions(), `namespace "shared" is claimed by both default/a and default/b`},
		{"cold without storage", []spillwayv1alpha1.LogPipeline{coldOnly},
			noCold, `default/archive routes to cold storage, but no cold storage is configured`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Render(tc.pipelines, tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got error %v, want one containing %q", err, tc.want)
			}
		})
	}
}

// With no pipelines the renderer produces the M1 pipeline, which is also the
// aggregator's bootstrap config before the operator takes over.
func TestNoPipelinesRendersBootstrapConfig(t *testing.T) {
	got, err := Render(nil, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	goldenAt(t, filepath.Join("..", "..", "vector", "aggregator", "vector.yaml"), got)
}

func TestClaimedDataLeavesThePlatformPath(t *testing.T) {
	examples := loadExamples(t)
	cfg := func(pipelines ...spillwayv1alpha1.LogPipeline) map[string]map[string]any {
		t.Helper()
		out, err := Render(pipelines, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		var c struct {
			Sources, Transforms, Sinks map[string]any
		}
		if err := yaml.Unmarshal(out, &c); err != nil {
			t.Fatal(err)
		}
		return map[string]map[string]any{"sources": c.Sources, "transforms": c.Transforms, "sinks": c.Sinks}
	}

	// payments claims two namespaces: the platform pod-log path filters them out.
	c := cfg(examples["payments"])
	f, ok := c["transforms"]["unclaimed_pods"].(map[string]any)
	if !ok {
		t.Fatal("no unclaimed_pods filter with a namespace claimed")
	}
	if want := `!includes(["payments", "payments-batch"], .kubernetes.pod_namespace)`; f["condition"] != want {
		t.Errorf("unclaimed_pods condition = %q, want %q", f["condition"], want)
	}
	if _, ok := c["sources"]["wikimedia"]; !ok {
		t.Error("platform feeder path missing although no team reads the feeder topic")
	}

	// content reads the feeder topic: the platform feeder path goes away.
	c = cfg(examples["content"])
	for _, id := range []string{"wikimedia"} {
		if _, ok := c["sources"][id]; ok {
			t.Errorf("platform source %q still rendered with the feeder topic claimed", id)
		}
	}
	for _, id := range []string{"stamp", "latency_metrics"} {
		if _, ok := c["transforms"][id]; ok {
			t.Errorf("platform transform %q still rendered with the feeder topic claimed", id)
		}
	}
	if _, ok := c["sinks"]["loki"]; ok {
		t.Error("platform loki sink still rendered with the feeder topic claimed")
	}
	if _, ok := c["transforms"]["unclaimed_pods"]; ok {
		t.Error("unclaimed_pods rendered although no namespace is claimed")
	}
}
