package v1alpha1_test

// These tests load the generated CRD into a real API server (envtest) and
// check that it accepts the example specs and rejects malformed ones at apply
// time. Run them with `make test`, which downloads the API server binaries and
// sets KUBEBUILDER_ASSETS.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
)

var k8s client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("CI") != "" {
			fmt.Fprintln(os.Stderr, "KUBEBUILDER_ASSETS is not set; run the tests with `make test`")
			os.Exit(1)
		}
		fmt.Println("skipping CRD validation tests: KUBEBUILDER_ASSETS is not set (run `make test`)")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "deploy", "operator", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting envtest:", err)
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	if err := spillwayv1alpha1.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, "creating client:", err)
		os.Exit(1)
	}
	// Reject unknown fields the way `kubectl apply` does, so typos fail.
	k8s = client.WithFieldValidation(c, metav1.FieldValidationStrict)

	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stopping envtest:", err)
	}
	os.Exit(code)
}

// create applies a LogPipeline given as YAML, under a unique name.
func create(t *testing.T, manifest string) (*unstructured.Unstructured, error) {
	t.Helper()
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal([]byte(manifest), &obj.Object); err != nil {
		t.Fatalf("parsing manifest: %v", err)
	}
	obj.SetNamespace("default")
	obj.SetName(strings.ToLower(strings.NewReplacer(" ", "-", "_", "-").Replace(t.Name()[strings.LastIndex(t.Name(), "/")+1:])))
	return obj, k8s.Create(context.Background(), obj)
}

func TestExamplesAreAccepted(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := create(t, string(b)); err != nil {
				t.Fatalf("example rejected: %v", err)
			}
		})
	}
}

func TestDefaults(t *testing.T) {
	obj, err := create(t, `
apiVersion: spillway.dev/v1alpha1
kind: LogPipeline
spec:
  team: search
  sources:
    - name: pods
      kubernetes: {namespaces: [search]}
  sampling:
    keepPercent: {debug: 5}
`)
	if err != nil {
		t.Fatal(err)
	}
	var lp spillwayv1alpha1.LogPipeline
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &lp); err != nil {
		t.Fatal(err)
	}
	r := lp.Spec.Routing
	if r == nil || r.Hot == nil || !*r.Hot || r.Cold == nil || *r.Cold {
		t.Errorf("routing defaults: got %+v, want hot=true cold=false", r)
	}
	if got := lp.Spec.Sampling.LevelField; got != "level" {
		t.Errorf("sampling.levelField default: got %q, want %q", got, "level")
	}
}

// MalformedFixtures returns the malformed specs in testdata/malformed and the
// error each must be rejected with (its "# want:" line).
func malformedFixtures(t *testing.T) map[string]struct{ manifest, want string } {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "malformed", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no malformed fixtures: %v", err)
	}
	out := map[string]struct{ manifest, want string }{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		first, _, _ := strings.Cut(string(b), "\n")
		want, ok := strings.CutPrefix(first, "# want: ")
		if !ok {
			t.Fatalf("%s: first line must be \"# want: <error text>\"", f)
		}
		out[strings.TrimSuffix(filepath.Base(f), ".yaml")] = struct{ manifest, want string }{string(b), want}
	}
	return out
}

func TestMalformedSpecsAreRejected(t *testing.T) {
	for name, tc := range malformedFixtures(t) {
		t.Run(name, func(t *testing.T) {
			_, err := create(t, tc.manifest)
			if err == nil {
				t.Fatalf("accepted, want rejection containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("rejected with %q, want it to contain %q", err, tc.want)
			}
		})
	}
}
