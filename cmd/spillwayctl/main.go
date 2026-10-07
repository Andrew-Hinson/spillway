// Command spillwayctl checks and renders LogPipeline specs without a cluster,
// with the same schema, renderer and validator the operator uses.
//
//	spillwayctl validate [flags] FILE|DIR...   check specs; exit 1 if any are invalid
//	spillwayctl render   [flags] FILE|DIR...   print the aggregator config they render to
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/render"
	"github.com/Andrew-Hinson/spillway/internal/schema"
	"github.com/Andrew-Hinson/spillway/internal/validate"
)

const usage = `spillwayctl checks and renders LogPipeline specs without a cluster.

Usage:
  spillwayctl validate [flags] FILE|DIR...   check specs the way the cluster would; exit 1 if any are invalid
  spillwayctl render   [flags] FILE|DIR...   print the aggregator config the specs render to

validate runs four checks, in order:
  1. each spec against the LogPipeline CRD schema, as kubectl apply would
  2. all specs rendered together (conflicts such as a team claimed twice)
  3. the rendered config with vector validate
  4. the unit tests generated for that config with vector test
Steps 3 and 4 need the vector binary; skip them with --no-vector.

Directories are read for *.yaml and *.yml files; a file may hold several
documents separated by ---.
`

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

// run is main without the process: it returns the exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(stderr, usage)
		return 2
	}
	cmd, args := args[0], args[1:]
	if cmd != "validate" && cmd != "render" {
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}

	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	// Cold storage is off unless configured, as in the operator.
	var cold render.ColdStorage
	fs.StringVar(&cold.Bucket, "cold-bucket", "", "S3 bucket for cold storage; empty means cold routing is invalid, as in the operator")
	fs.StringVar(&cold.Endpoint, "cold-endpoint", "", "S3 endpoint, e.g. http://minio.storage.svc:9000 (empty for AWS)")
	fs.StringVar(&cold.Region, "cold-region", "us-east-1", "S3 region")
	out := fs.String("o", "", "render: write the config to this file instead of stdout")
	vectorBin := fs.String("vector-bin", "vector", "validate: the vector binary (use the aggregator's version)")
	noVector := fs.Bool("no-vector", false, "validate: skip the vector validate and vector test steps")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fmt.Fprintf(stderr, "%s: no files or directories given\n", cmd)
		return 2
	}
	opts := render.DefaultOptions()
	opts.Cold = nil
	if cold.Bucket != "" {
		opts.Cold = &cold
	}

	docs, err := load(fs.Args())
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	// render's stdout is the config itself, so its status lines go to stderr.
	status := stdout
	if cmd == "render" {
		status = stderr
	}
	pipelines, ok := checkSchema(ctx, docs, status)

	if cmd == "render" {
		if !ok {
			fmt.Fprintln(stderr, "not rendering: fix the specs above first")
			return 1
		}
		cfg, err := render.Render(pipelines, opts)
		if err != nil {
			fmt.Fprintln(stderr, "render:", err)
			return 1
		}
		if *out == "" {
			_, _ = stdout.Write(cfg)
			return 0
		}
		if err := os.WriteFile(*out, cfg, 0o644); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	cfg, tests, err := render.RenderWithTests(pipelines, opts)
	if err != nil {
		fmt.Fprintln(stdout, "render: FAIL:", err)
		return 1
	}
	fmt.Fprintf(stdout, "render: ok (%s)\n", plural(len(pipelines), "pipeline"))
	if !ok {
		return 1
	}
	if *noVector {
		fmt.Fprintln(stdout, "vector validate, vector test: skipped (--no-vector)")
		return 0
	}
	bin, err := exec.LookPath(*vectorBin)
	if err != nil {
		fmt.Fprintf(stderr, "vector validate: can't find %q (install Vector, pass --vector-bin, or skip with --no-vector)\n", *vectorBin)
		return 1
	}
	vector := validate.Vector{Bin: bin}
	if err := vector.Validate(ctx, cfg); err != nil {
		fmt.Fprintln(stdout, "vector validate: FAIL:", err)
		return 1
	}
	fmt.Fprintln(stdout, "vector validate: ok")
	if err := vector.Test(ctx, cfg, tests); err != nil {
		fmt.Fprintln(stdout, "vector test: FAIL:", err)
		return 1
	}
	var suite struct{ Tests []any }
	_ = yaml.Unmarshal(tests, &suite)
	fmt.Fprintf(stdout, "vector test: ok (%s)\n", plural(len(suite.Tests), "test"))
	return 0
}

// doc is one YAML document and where it came from.
type doc struct {
	where string // file, plus the document number if the file has several
	body  []byte
}

var separator = regexp.MustCompile(`(?m)^---\s*$`)

// load reads every document in the given files and directories.
func load(paths []string) ([]doc, error) {
	var files []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			files = append(files, p)
			continue
		}
		for _, ext := range []string{"*.yaml", "*.yml"} {
			m, _ := filepath.Glob(filepath.Join(p, ext))
			files = append(files, m...)
		}
	}
	sort.Strings(files)
	var docs []doc
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		var parts [][]byte
		for _, part := range separator.Split(string(b), -1) {
			if strings.TrimSpace(stripComments(part)) != "" {
				parts = append(parts, []byte(part))
			}
		}
		for i, part := range parts {
			where := f
			if len(parts) > 1 {
				where = fmt.Sprintf("%s[%d]", f, i)
			}
			docs = append(docs, doc{where: where, body: part})
		}
	}
	if len(docs) == 0 {
		return nil, errors.New("no LogPipeline documents found")
	}
	return docs, nil
}

func stripComments(s string) string {
	var b bytes.Buffer
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			b.WriteString(line + "\n")
		}
	}
	return b.String()
}

// checkSchema validates each document against the CRD, prints a line per
// document, and returns the valid ones with defaults applied.
func checkSchema(ctx context.Context, docs []doc, w io.Writer) ([]spillwayv1alpha1.LogPipeline, bool) {
	v, err := schema.New()
	if err != nil {
		fmt.Fprintln(w, "loading the CRD schema:", err)
		return nil, false
	}
	ok := true
	var pipelines []spillwayv1alpha1.LogPipeline
	for _, d := range docs {
		obj, errs := v.Validate(ctx, d.body)
		name := d.where
		if meta, _ := obj["metadata"].(map[string]any); meta != nil && meta["name"] != nil {
			name = fmt.Sprintf("%s (%v)", d.where, meta["name"])
		}
		if len(errs) > 0 {
			ok = false
			fmt.Fprintf(w, "%s: INVALID\n", name)
			for _, e := range errs {
				fmt.Fprintf(w, "  * %s\n", e.Error())
			}
			continue
		}
		var lp spillwayv1alpha1.LogPipeline
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj, &lp); err != nil {
			ok = false
			fmt.Fprintf(w, "%s: INVALID\n  * %v\n", name, err)
			continue
		}
		if lp.Namespace == "" {
			lp.Namespace = "default"
		}
		fmt.Fprintf(w, "%s: ok\n", name)
		pipelines = append(pipelines, lp)
	}
	return pipelines, ok
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}
