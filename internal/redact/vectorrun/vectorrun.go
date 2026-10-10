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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

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

	out := make([]map[string]any, len(events))
	seen := 0
	err = Stream(ctx, vectorBin, path, in.Bytes(), func(line []byte) (bool, error) {
		var e map[string]any
		if err := json.Unmarshal(line, &e); err != nil {
			return false, fmt.Errorf("decoding vector output: %w", err)
		}
		i, ok := e["spillway_run"].(float64)
		if !ok || int(i) < 0 || int(i) >= len(out) || out[int(i)] != nil {
			return false, fmt.Errorf("vector output has a missing or bad position: %s", line)
		}
		delete(e, "spillway_run")
		out[int(i)] = e
		seen++
		return seen == len(out), nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// streamTimeout bounds a run whose expected output never arrives.
const streamTimeout = time.Minute

// Stream runs vector with the config at path, writes input to its stdin, and
// passes each line it prints to line until line returns true. Only then is
// stdin closed. Closing it straight after writing, and letting vector exit at
// EOF, loses the last event now and then on a loaded machine: it's still in
// flight when the topology shuts down.
func Stream(ctx context.Context, vectorBin, path string, input []byte, line func([]byte) (done bool, err error)) error {
	ctx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, vectorBin, "--quiet", "--config", path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() {
		_, _ = stdin.Write(input)
	}()

	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	done := false
	for !done && sc.Scan() {
		if done, err = line(sc.Bytes()); err != nil {
			break
		}
	}
	_ = stdin.Close()
	if err == nil {
		err = sc.Err()
	}
	// Drain what's left so vector can exit.
	_, _ = io.Copy(io.Discard, stdout)
	waitErr := cmd.Wait()
	switch {
	case err != nil:
		return err
	case !done && ctx.Err() != nil:
		return fmt.Errorf("vector: expected output didn't arrive within %s\n%s", streamTimeout, stderr.String())
	case !done:
		return fmt.Errorf("vector exited before the expected output arrived: %v\n%s", waitErr, stderr.String())
	case waitErr != nil:
		return fmt.Errorf("vector: %w\n%s", waitErr, stderr.String())
	}
	return nil
}

func shallowCopy(m map[string]any) map[string]any {
	c := make(map[string]any, len(m)+1)
	for k, v := range m {
		c[k] = v
	}
	return c
}
