package schema

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newValidator(t *testing.T) *Validator {
	t.Helper()
	v, err := New()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// The fixtures api/v1alpha1 checks against a real API server: the offline
// check must reject each with the same error.
func TestRejectsTheSameMalformedSpecsAsTheAPIServer(t *testing.T) {
	v := newValidator(t)
	files, err := filepath.Glob(filepath.Join("..", "..", "api", "v1alpha1", "testdata", "malformed", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no malformed fixtures: %v", err)
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			first, _, _ := strings.Cut(string(b), "\n")
			want := strings.TrimPrefix(first, "# want: ")
			_, errs := v.Validate(context.Background(), b)
			if len(errs) == 0 {
				t.Fatalf("accepted, want rejection containing %q", want)
			}
			if !strings.Contains(errs.ToAggregate().Error(), want) {
				t.Fatalf("rejected with %q, want it to contain %q", errs.ToAggregate(), want)
			}
		})
	}
}

func TestAcceptsExamplesAndAppliesDefaults(t *testing.T) {
	v := newValidator(t)
	files, _ := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	if len(files) == 0 {
		t.Fatal("no examples")
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		obj, errs := v.Validate(context.Background(), b)
		if len(errs) > 0 {
			t.Errorf("%s rejected: %v", f, errs.ToAggregate())
			continue
		}
		routing, _ := obj["spec"].(map[string]any)["routing"].(map[string]any)
		if routing["hot"] == nil || routing["cold"] == nil {
			t.Errorf("%s: routing defaults not applied: %v", f, routing)
		}
	}
}

func TestRejectsWrongKindAndMissingName(t *testing.T) {
	v := newValidator(t)
	cases := map[string]struct{ manifest, want string }{
		"other kind":   {"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\n", "want apiVersion spillway.dev/v1alpha1"},
		"missing name": {"apiVersion: spillway.dev/v1alpha1\nkind: LogPipeline\nspec: {team: a, sources: [{name: p, kafka: {topic: t}}]}\n", "metadata.name: Required value"},
		"bad name":     {"apiVersion: spillway.dev/v1alpha1\nkind: LogPipeline\nmetadata: {name: Bad_Name}\nspec: {team: a, sources: [{name: p, kafka: {topic: t}}]}\n", "metadata.name: Invalid value"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, errs := v.Validate(context.Background(), []byte(tc.manifest))
			if !strings.Contains(errs.ToAggregate().Error(), tc.want) {
				t.Fatalf("got %v, want an error containing %q", errs.ToAggregate(), tc.want)
			}
		})
	}
}
