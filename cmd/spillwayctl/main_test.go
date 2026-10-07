package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var coldFlags = []string{"--cold-bucket", "spillway-cold", "--cold-endpoint", "http://minio.storage.svc:9000"}

func spillwayctl(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(context.Background(), args, &out, &errOut)
	return code, out.String(), errOut.String()
}

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

const examples = "../../examples"

// render uses the operator's renderer: with the same cold storage settings it
// reproduces the renderer's golden file for all the examples.
func TestRenderMatchesTheRenderersGoldenFile(t *testing.T) {
	code, out, errOut := spillwayctl(t, append(append([]string{"render"}, coldFlags...), examples)...)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	want, err := os.ReadFile("../../internal/render/testdata/all-examples.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if out != string(want) {
		t.Error("rendered config differs from internal/render/testdata/all-examples.yaml")
	}
	if strings.Contains(out, ": ok") {
		t.Error("status lines leaked into the rendered config on stdout")
	}
}

func TestRenderWritesToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "aggregator.yaml")
	if code, _, errOut := spillwayctl(t, "render", "-o", path, examples+"/minimal.yaml"); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	b, err := os.ReadFile(path)
	if err != nil || !bytes.Contains(b, []byte("search_hot:")) {
		t.Fatalf("rendered file missing or incomplete: %v", err)
	}
}

func TestValidateAcceptsExamples(t *testing.T) {
	code, out, errOut := spillwayctl(t, append(append([]string{"validate", "--vector-bin", vectorBin(t)}, coldFlags...), examples)...)
	if code != 0 {
		t.Fatalf("exit %d\n%s%s", code, out, errOut)
	}
	for _, want := range []string{"examples/payments.yaml (payments): ok", "render: ok (3 pipelines)", "vector validate: ok", "vector test: ok (31 tests)"} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	dir := t.TempDir()
	multi := filepath.Join(dir, "two-teams.yaml")
	if err := os.WriteFile(multi, []byte(`# two pipelines, one team
apiVersion: spillway.dev/v1alpha1
kind: LogPipeline
metadata: {name: first}
spec: {team: search, sources: [{name: a, kubernetes: {namespaces: [a]}}]}
---
apiVersion: spillway.dev/v1alpha1
kind: LogPipeline
metadata: {name: second}
spec: {team: search, sources: [{name: b, kubernetes: {namespaces: [b]}}]}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		args []string
		want []string
	}{
		{"schema error", []string{"validate", "--no-vector", "../../api/v1alpha1/testdata/malformed/zero-budget.yaml"},
			[]string{"(zero-budget): INVALID", "should be greater than or equal to 1"}},
		{"conflict across documents", []string{"validate", "--no-vector", multi},
			[]string{"two-teams.yaml[0] (first): ok", "two-teams.yaml[1] (second): ok", `render: FAIL: team "search" is claimed by both`}},
		{"cold without cold storage, as in the operator", []string{"validate", "--no-vector", examples + "/payments.yaml"},
			[]string{"render: FAIL: default/payments routes to cold storage, but no cold storage is configured"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, _ := spillwayctl(t, tc.args...)
			if code != 1 {
				t.Errorf("exit %d, want 1", code)
			}
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("output lacks %q:\n%s", w, out)
				}
			}
		})
	}
}

// Every fixture the API server rejects, spillwayctl rejects too.
func TestValidateRejectsEveryMalformedFixture(t *testing.T) {
	files, _ := filepath.Glob("../../api/v1alpha1/testdata/malformed/*.yaml")
	if len(files) == 0 {
		t.Fatal("no fixtures")
	}
	for _, f := range files {
		if code, out, _ := spillwayctl(t, "validate", "--no-vector", f); code != 1 || !strings.Contains(out, "INVALID") {
			t.Errorf("%s: exit %d, want 1 with INVALID:\n%s", filepath.Base(f), code, out)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{{}, {"frobnicate", "x"}, {"validate"}, {"render", "--no-such-flag", "x"}} {
		if code, _, _ := spillwayctl(t, args...); code != 2 {
			t.Errorf("spillwayctl %v: exit %d, want 2", args, code)
		}
	}
	if code, _, errOut := spillwayctl(t, "validate", "--vector-bin", "/nonexistent/vector", examples+"/minimal.yaml"); code != 1 || !strings.Contains(errOut, "--no-vector") {
		t.Errorf("missing vector: exit %d, stderr %q; want 1 with a hint", code, errOut)
	}
}
