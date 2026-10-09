// Package vectorrun pushes events through a remap program in the real vector
// binary, for tests and measurements that must see what Vector does rather
// than what a Go reimplementation of the regexes would do (Rust's \d and \b
// are Unicode-aware; Go's are not).
package vectorrun

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"sigs.k8s.io/yaml"
)

// Remap runs each event (a JSON object) through vrl and returns the results
// in input order. Events must not have a .spillway_run field: it carries
// each event's position through Vector.
func Remap(ctx context.Context, vectorBin, vrl string, events []map[string]any) ([]map[string]any, error) {
	dir, err := os.MkdirTemp("", "vectorrun-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// The stdin source adds host, source_type and timestamp; they're
	// removed before the program runs, so it sees only the event.
	cfg, err := yaml.Marshal(map[string]any{
		"data_dir": dir,
		"sources":  map[string]any{"in": map[string]any{"type": "stdin", "decoding": map[string]any{"codec": "json"}}},
		"transforms": map[string]any{"run": map[string]any{
			"type":   "remap",
			"inputs": []string{"in"},
			"source": "del(.host)\ndel(.source_type)\ndel(.timestamp)\nrun = del(.spillway_run)\n" + vrl + "\n.spillway_run = run\n",
		}},
		"sinks": map[string]any{"out": map[string]any{
			"type": "console", "inputs": []string{"run"}, "encoding": map[string]any{"codec": "json"},
		}},
	})
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "vector.yaml")
	if err := os.WriteFile(path, cfg, 0o600); err != nil {
		return nil, err
	}

	var in bytes.Buffer
	for i, e := range events {
		e = shallowCopy(e)
		e["spillway_run"] = i
		b, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		in.Write(b)
		in.WriteByte('\n')
	}

	// Vector exits once stdin is drained and every event is written.
	cmd := exec.CommandContext(ctx, vectorBin, "--quiet", "--config", path)
	cmd.Stdin = &in
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("vector: %w\n%s", err, stderr.String())
	}

	out := make([]map[string]any, len(events))
	sc := bufio.NewScanner(&stdout)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		var e map[string]any
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("decoding vector output: %w", err)
		}
		i, ok := e["spillway_run"].(float64)
		if !ok || int(i) < 0 || int(i) >= len(out) || out[int(i)] != nil {
			return nil, fmt.Errorf("vector output has a missing or bad position: %s", sc.Bytes())
		}
		delete(e, "spillway_run")
		out[int(i)] = e
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	for i, e := range out {
		if e == nil {
			return nil, fmt.Errorf("vector dropped event %d\n%s", i, stderr.String())
		}
	}
	return out, nil
}

func shallowCopy(m map[string]any) map[string]any {
	c := make(map[string]any, len(m)+1)
	for k, v := range m {
		c[k] = v
	}
	return c
}
