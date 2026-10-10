package redact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/redact/vectorrun"
)

type cases struct {
	Mask, Keep, OverMasked, NotMasked []string
}

type corpus struct {
	Patterns map[spillwayv1alpha1.RedactionPattern]cases
	KeepAll  []string
}

func loadCorpus(t *testing.T) corpus {
	t.Helper()
	b, err := os.ReadFile("testdata/corpus.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := yaml.UnmarshalStrict(b, &raw); err != nil {
		t.Fatal(err)
	}
	c := corpus{Patterns: map[spillwayv1alpha1.RedactionPattern]cases{}}
	for k, v := range raw {
		var err error
		if k == "keepAll" {
			err = json.Unmarshal(v, &c.KeepAll)
		} else {
			var cs cases
			d := json.NewDecoder(bytes.NewReader(v))
			d.DisallowUnknownFields()
			err = d.Decode(&cs)
			c.Patterns[spillwayv1alpha1.RedactionPattern(k)] = cs
		}
		if err != nil {
			t.Fatalf("corpus: %s: %v", k, err)
		}
	}
	for _, p := range All() {
		if len(c.Patterns[p].Mask) == 0 || len(c.Patterns[p].Keep) == 0 {
			t.Fatalf("corpus: pattern %s needs mask and keep cases", p)
		}
	}
	if len(c.Patterns) != len(All()) {
		t.Fatalf("corpus has %d patterns, the library %d", len(c.Patterns), len(All()))
	}
	return c
}

// contexts are the surroundings each mask value is tested in. Several defeat
// a \b-bounded pattern: pod logs are unparsed strings of escaped JSON, so a
// value often follows a letter (\n, \t) or a digit ( ).
var contexts = []string{
	"%s",
	"contact: %s.",
	`{\"note\":\"line one\nline two %s\"}`, // escaped JSON in a pod log's message
	`\t%s\t`,
	` %s`,
	"id%sx", // glued to a label and a suffix
	"<td>%s</td>",
	"https://example.org/p?q=%s&next=1",
	"%s,%s", // two in a row
	"\t%s\n",
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

type check struct {
	desc, in string
	ok       func(out string) bool
}

// run sends every check's input through the VRL for patterns, in one vector
// process, and reports each check that fails.
func run(t *testing.T, bin string, patterns []spillwayv1alpha1.RedactionPattern, checks []check) {
	t.Helper()
	vrl, err := VRL(patterns)
	if err != nil {
		t.Fatal(err)
	}
	events := make([]map[string]any, len(checks))
	for i, c := range checks {
		events[i] = map[string]any{"text": c.in}
	}
	out, err := vectorrun.Remap(context.Background(), bin, vrl, events)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range checks {
		got, _ := out[i]["text"].(string)
		if !c.ok(got) {
			t.Errorf("%s: %q -> %q", c.desc, c.in, got)
		}
	}
}

func TestCorpus(t *testing.T) {
	bin := vectorBin(t)
	c := loadCorpus(t)
	for _, p := range All() {
		t.Run(string(p), func(t *testing.T) {
			cases := c.Patterns[p]
			var checks []check
			for _, v := range cases.Mask {
				for _, ctx := range contexts {
					in := strings.ReplaceAll(ctx, "%s", v)
					checks = append(checks, check{"must mask", in, func(out string) bool {
						return !strings.Contains(out, v) && strings.Contains(out, "[REDACTED]")
					}})
				}
			}
			unchanged := func(in string) func(string) bool { return func(out string) bool { return out == in } }
			for _, v := range cases.Keep {
				checks = append(checks, check{"must keep", v, unchanged(v)})
			}
			for _, v := range cases.NotMasked {
				checks = append(checks, check{"listed as notMasked, but masked", v, unchanged(v)})
			}
			for _, v := range cases.OverMasked {
				checks = append(checks, check{"listed as overMasked, but kept", v, func(out string) bool { return out != v }})
			}
			run(t, bin, []spillwayv1alpha1.RedactionPattern{p}, checks)
		})
	}
	t.Run("keepAll", func(t *testing.T) {
		var checks []check
		for _, v := range c.KeepAll {
			checks = append(checks, check{"must survive every pattern", v, func(out string) bool { return out == v }})
		}
		run(t, bin, All(), checks)
	})
}

// Values are masked however deeply they're nested, and Spillway's own
// metadata is never touched, so fixture tags and event IDs survive.
func TestNestedValuesAndMetadata(t *testing.T) {
	bin := vectorBin(t)
	vrl, err := VRL(All())
	if err != nil {
		t.Fatal(err)
	}
	var events []map[string]any
	for _, p := range All() {
		v := Example(p)
		events = append(events, map[string]any{
			"a":        []any{map[string]any{"b": []any{"x " + v}}},
			"n":        42,
			"spillway": map[string]any{"event_id": v, "fixture": map[string]any{"id": 7, "pattern": string(p)}},
		})
	}
	events = append(events, map[string]any{"message": "no metadata " + Example(spillwayv1alpha1.RedactSSN)})
	out, err := vectorrun.Remap(context.Background(), bin, vrl, events)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range All() {
		got := fmt.Sprint(out[i]["a"])
		if strings.Contains(got, Example(p)) || !strings.Contains(got, "x [REDACTED]") {
			t.Errorf("%s: nested value not masked: %v", p, got)
		}
		meta := out[i]["spillway"].(map[string]any)
		if meta["event_id"] != Example(p) || meta["fixture"].(map[string]any)["pattern"] != string(p) {
			t.Errorf("%s: metadata changed: %v", p, meta)
		}
		if out[i]["n"] != float64(42) {
			t.Errorf("%s: number changed: %v", p, out[i]["n"])
		}
	}
	last := out[len(out)-1]
	if last["message"] != "no metadata [REDACTED]" {
		t.Errorf("message = %v", last["message"])
	}
	if _, ok := last["spillway"]; ok {
		t.Errorf("an event without metadata gained a spillway field: %v", last)
	}
}

func TestFiltersRejectUnknownPatterns(t *testing.T) {
	if _, err := Filters([]spillwayv1alpha1.RedactionPattern{"creditCard"}); err == nil {
		t.Fatal("want an error for an unknown pattern")
	}
	// Every pattern the CRD accepts has a filter and an example.
	for _, p := range []spillwayv1alpha1.RedactionPattern{spillwayv1alpha1.RedactSSN, spillwayv1alpha1.RedactEmail, spillwayv1alpha1.RedactPhone, spillwayv1alpha1.RedactMemberID} {
		if _, err := Filters([]spillwayv1alpha1.RedactionPattern{p}); err != nil || Example(p) == "" {
			t.Errorf("%s: filter err %v, example %q", p, err, Example(p))
		}
	}
}
