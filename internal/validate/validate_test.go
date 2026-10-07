package validate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// vectorBin is the pinned vector binary `make test` downloads into ./bin.
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

func TestVectorAcceptsRenderedConfig(t *testing.T) {
	cfg, err := os.ReadFile(filepath.Join("..", "render", "testdata", "all-examples.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := (Vector{Bin: vectorBin(t)}).Validate(context.Background(), cfg); err != nil {
		t.Fatalf("rendered example config rejected: %v", err)
	}
}

func TestVectorRejectsBrokenConfig(t *testing.T) {
	bin := vectorBin(t)
	cases := []struct {
		name, config, want string
	}{
		{"VRL that doesn't compile", `
sources: {in: {type: demo_logs, format: json}}
transforms: {t: {type: remap, inputs: [in], source: ".x = upcase(42)"}}
sinks: {out: {type: blackhole, inputs: [t]}}
`, "upcase"},
		{"input that doesn't exist", `
sources: {in: {type: demo_logs, format: json}}
sinks: {out: {type: blackhole, inputs: [missing]}}
`, "missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := (Vector{Bin: bin}).Validate(context.Background(), []byte(tc.config))
			var verdict *Error
			if !errors.As(err, &verdict) {
				t.Fatalf("got %v, want a validation error", err)
			}
			if !strings.Contains(verdict.Output, tc.want) {
				t.Errorf("output doesn't mention %q:\n%s", tc.want, verdict.Output)
			}
			if strings.Contains(verdict.Output, os.TempDir()) || strings.Contains(verdict.Output, "\x1b[") {
				t.Errorf("output leaks the temp file name or colour codes:\n%s", verdict.Output)
			}
		})
	}
}

func TestMissingBinaryIsNotAVerdict(t *testing.T) {
	err := (Vector{Bin: "/nonexistent/vector"}).Validate(context.Background(), []byte("sources: {}"))
	var verdict *Error
	if err == nil || errors.As(err, &verdict) {
		t.Fatalf("got %v, want an error that isn't a verdict on the config", err)
	}
}

type counting struct {
	calls int
	err   error
}

func (c *counting) Validate(context.Context, []byte) error {
	c.calls++
	return c.err
}

func TestCachedValidatesEachConfigOnce(t *testing.T) {
	inner := &counting{err: &Error{Output: "bad"}}
	c := &Cached{Validator: inner, Size: 2}
	ctx := context.Background()
	for range 3 {
		if err := c.Validate(ctx, []byte("a")); err == nil {
			t.Fatal("cached verdict lost")
		}
	}
	if inner.calls != 1 {
		t.Errorf("validated the same config %d times, want 1", inner.calls)
	}
	// Two more configs evict "a" (size 2), so it's validated again.
	_ = c.Validate(ctx, []byte("b"))
	_ = c.Validate(ctx, []byte("c"))
	_ = c.Validate(ctx, []byte("a"))
	if inner.calls != 4 {
		t.Errorf("calls = %d, want 4 after eviction", inner.calls)
	}
}

func TestCachedDoesNotRememberRunFailures(t *testing.T) {
	inner := &counting{err: errors.New("vector not found")}
	c := &Cached{Validator: inner, Size: 4}
	_ = c.Validate(context.Background(), []byte("a"))
	_ = c.Validate(context.Background(), []byte("a"))
	if inner.calls != 2 {
		t.Errorf("a failure to run Vector was cached (calls = %d, want 2)", inner.calls)
	}
}

func TestVectorRunsGeneratedTests(t *testing.T) {
	v := Vector{Bin: vectorBin(t)}
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("..", "render", "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cfg, tests := read("payments.yaml"), read("payments.tests.yaml")
	if err := v.Test(context.Background(), cfg, tests); err != nil {
		t.Fatalf("generated tests fail on the config they were generated for: %v", err)
	}

	// Sampling that keeps debug events must fail the test that says it drops them.
	broken := strings.Replace(string(cfg), `"debug": 0`, `"debug": 100`, 1)
	if broken == string(cfg) {
		t.Fatal("mutation didn't apply")
	}
	err := v.Test(context.Background(), []byte(broken), tests)
	var verdict *Error
	if !errors.As(err, &verdict) {
		t.Fatalf("got %v, want a failing test", err)
	}
	if !strings.Contains(verdict.Output, "payments_sample drops every debug event") || strings.Contains(verdict.Output, "... passed") {
		t.Errorf("output should name the failing test and omit passing ones:\n%s", verdict.Output)
	}
}
