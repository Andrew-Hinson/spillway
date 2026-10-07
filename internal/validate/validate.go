// Package validate checks rendered Vector config with `vector validate` before
// the operator applies it, so a config Vector would reject never reaches a
// running aggregator. See docs/adr/0001-validate-in-the-operator-image.md.
package validate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Validator checks a rendered config. A nil error means Vector accepts it.
type Validator interface {
	Validate(ctx context.Context, config []byte) error
}

// Error is a config Vector rejected, with what Vector said about it.
type Error struct {
	Output string
}

func (e *Error) Error() string { return "vector validate: " + e.Output }

var (
	validations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "spillway_operator_validations_total",
		Help: "Rendered configs checked with vector validate, by result (valid, invalid or error).",
	}, []string{"result"})
	duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "spillway_operator_validation_duration_seconds",
		Help:    "Time taken by vector validate.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	})
)

func init() {
	metrics.Registry.MustRegister(validations, duration)
}

// Vector runs a vector binary of the same version as the aggregator.
type Vector struct {
	Bin     string
	Timeout time.Duration
}

// Validate writes config to a temporary file and runs `vector validate` on it.
// Environment checks (connecting to Kafka, Loki and so on) are skipped: the
// question is whether Vector accepts the config, not whether the cluster is
// healthy right now.
func (v Vector) Validate(ctx context.Context, config []byte) error {
	f, err := os.CreateTemp("", "spillway-*.yaml")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.Write(config); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	timeout := v.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, v.Bin, "validate", "--no-environment", f.Name())
	cmd.Stdout, cmd.Stderr = &out, &out
	start := time.Now()
	err = cmd.Run()
	duration.Observe(time.Since(start).Seconds())

	var exit *exec.ExitError
	switch {
	case err == nil:
		validations.WithLabelValues("valid").Inc()
		return nil
	case errors.As(err, &exit) && ctx.Err() == nil:
		validations.WithLabelValues("invalid").Inc()
		return &Error{Output: summarize(out.String(), f.Name())}
	default:
		// Vector couldn't run (missing binary, timeout): not a verdict on the config.
		validations.WithLabelValues("error").Inc()
		return fmt.Errorf("running %s validate: %w", v.Bin, err)
	}
}

// Test runs `vector test` with the rendered config and its generated unit
// tests (render.RenderWithTests). A failing test is reported as an *Error.
func (v Vector) Test(ctx context.Context, config, tests []byte) error {
	dir, err := os.MkdirTemp("", "spillway-test-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	cfgPath, testsPath := dir+"/config.yaml", dir+"/tests.yaml"
	if err := os.WriteFile(cfgPath, config, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(testsPath, tests, 0o600); err != nil {
		return err
	}
	timeout := v.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, v.Bin, "test", cfgPath, testsPath)
	cmd.Stdout, cmd.Stderr = &out, &out
	err = cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &exit) && ctx.Err() == nil:
		// Keep the failures, not the list of passing tests.
		var keep []string
		for _, line := range strings.Split(ansi.ReplaceAllString(out.String(), ""), "\n") {
			if !strings.HasSuffix(line, "... passed") && line != "Running tests" {
				keep = append(keep, line)
			}
		}
		return &Error{Output: summarize(strings.Join(keep, "\n"), dir)}
	default:
		return fmt.Errorf("running %s test: %w", v.Bin, err)
	}
}

var ansi = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// summarize keeps Vector's error lines, without colour, progress lines or the
// temporary file's name, and short enough for a status condition.
func summarize(out, file string) string {
	var keep []string
	for _, line := range strings.Split(ansi.ReplaceAllString(out, ""), "\n") {
		line = strings.TrimRight(strings.ReplaceAll(line, file, "<rendered config>"), " ")
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "√") || strings.Trim(t, "-~") == "" {
			continue
		}
		keep = append(keep, line)
	}
	s := strings.Join(keep, "\n")
	if len(s) > 2000 {
		s = s[:2000] + "\n…"
	}
	return s
}

// Cached remembers verdicts by config hash. The operator reconciles on every
// status and rollout event, usually with an unchanged config, and Vector takes
// a moment to start, so each distinct config is validated once. Only verdicts
// on the config are cached, not failures to run Vector.
type Cached struct {
	Validator Validator
	Size      int

	mu      sync.Mutex
	results map[[32]byte]error
	order   [][32]byte
}

// Validate returns the remembered verdict for config, or validates it.
func (c *Cached) Validate(ctx context.Context, config []byte) error {
	key := sha256.Sum256(config)
	c.mu.Lock()
	if err, ok := c.results[key]; ok {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	err := c.Validator.Validate(ctx, config)
	var verdict *Error
	if err != nil && !errors.As(err, &verdict) {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.results == nil {
		c.results = map[[32]byte]error{}
	}
	if _, ok := c.results[key]; !ok {
		c.results[key] = err
		c.order = append(c.order, key)
		if size := max(c.Size, 1); len(c.order) > size {
			delete(c.results, c.order[0])
			c.order = c.order[1:]
		}
	}
	return err
}
