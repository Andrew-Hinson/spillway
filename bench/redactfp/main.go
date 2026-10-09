// Command redactfp measures how often each redaction pattern fires on real
// events, through the real vector binary. Fed public data (the Wikimedia
// stream carries no PII by design), every match is a candidate false
// positive: the report lists them so each can be checked by eye.
//
//	curl -sN https://stream.wikimedia.org/v2/stream/recentchange \
//	  | sed -un 's/^data: //p' > sample.ndjson
//	go run ./bench/redactfp -vector-bin bin/vector < sample.ndjson
//
// -filter name=expr (repeatable) measures candidate filters instead of the
// library's, e.g. -filter "ssn-old=r'\b\d{3}-\d{2}-\d{4}\b'".
package main

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/Andrew-Hinson/spillway/internal/redact"
	"github.com/Andrew-Hinson/spillway/internal/redact/vectorrun"
)

type filter struct{ name, expr string }

type filterFlags []filter

func (f *filterFlags) String() string { return "" }
func (f *filterFlags) Set(v string) error {
	name, expr, ok := strings.Cut(v, "=")
	if !ok || !strings.HasPrefix(expr, "r'") {
		return fmt.Errorf("want name=r'regex', got %q", v)
	}
	*f = append(*f, filter{name, expr})
	return nil
}

func main() {
	bin := flag.String("vector-bin", "bin/vector", "vector binary")
	top := flag.Int("top", 15, "distinct matches to list per pattern")
	var filters filterFlags
	flag.Var(&filters, "filter", "name=r'regex' to measure instead of the library (repeatable)")
	flag.Parse()

	if len(filters) == 0 {
		for _, p := range redact.All() {
			f, err := redact.Filters(append(redact.All()[:0:0], p))
			if err != nil {
				fail(err)
			}
			filters = append(filters, filter{string(p), f[0]})
		}
	}
	events, err := read(os.Stdin)
	if err != nil {
		fail(err)
	}
	if len(events) == 0 {
		fail(fmt.Errorf("no events on stdin"))
	}

	var strs, bytes int
	for _, e := range events {
		walk(e, e, "", func(_, v, _ string) { strs++; bytes += len(v) })
	}
	fmt.Printf("%d events, %d string fields, %.1f MB of text.\n\n", len(events), strs, float64(bytes)/1e6)
	fmt.Println("| Pattern | Events with a match | Share | Matches | Fields matched (top 3) |")
	fmt.Println("|---|---:|---:|---:|---|")

	anyHit := make([]bool, len(events))
	var details strings.Builder
	for _, f := range filters {
		out, err := vectorrun.Remap(context.Background(), *bin, redact.Program(f.expr), events)
		if err != nil {
			fail(err)
		}
		re := goRegexp(f.expr)
		hits, matches := 0, 0
		fields, values := map[string]int{}, map[string]int{}
		for i := range events {
			hit := false
			walk(events[i], out[i], "", func(path, before, after string) {
				if before == after {
					return
				}
				hit = true
				fields[path]++
				found := re.FindAllString(before, -1)
				if len(found) == 0 { // Go's regexp disagrees with Rust's: show the field
					found = []string{"(field) " + truncate(before, 80)}
				}
				for _, m := range found {
					values[m]++
					matches++
				}
			})
			if hit {
				hits++
				anyHit[i] = true
			}
		}
		fmt.Printf("| %s | %d | %.3f%% | %d | %s |\n", f.name, hits, 100*float64(hits)/float64(len(events)), matches, topN(fields, 3, ", "))
		fmt.Fprintf(&details, "\n### %s\n\n`%s`\n\n", f.name, f.expr)
		if len(values) == 0 {
			details.WriteString("No matches.\n")
			continue
		}
		details.WriteString("| Matched text | Count |\n|---|---:|\n")
		for _, kv := range sorted(values)[:min(*top, len(values))] {
			fmt.Fprintf(&details, "| `%s` | %d |\n", strings.ReplaceAll(kv.k, "|", `\|`), kv.n)
		}
		if len(values) > *top {
			fmt.Fprintf(&details, "\n…and %d more distinct values.\n", len(values)-*top)
		}
	}
	n := 0
	for _, h := range anyHit {
		if h {
			n++
		}
	}
	fmt.Printf("| **any** | %d | %.3f%% | | |\n", n, 100*float64(n)/float64(len(events)))
	fmt.Print(details.String())
}

func read(r io.Reader) ([]map[string]any, error) {
	var events []map[string]any
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for sc.Scan() {
		var e map[string]any
		if json.Unmarshal(sc.Bytes(), &e) == nil && e != nil {
			delete(e, "spillway") // never redacted, so never measured
			events = append(events, e)
		}
	}
	return events, sc.Err()
}

// walk calls fn for each string leaf of before with its counterpart in
// after. Array indices are collapsed, so paths aggregate across events.
func walk(before, after any, path string, fn func(path, before, after string)) {
	switch b := before.(type) {
	case string:
		a, _ := after.(string)
		fn(path, b, a)
	case map[string]any:
		a, _ := after.(map[string]any)
		for k, v := range b {
			p := k
			if path != "" {
				p = path + "." + k
			}
			walk(v, a[k], p, fn)
		}
	case []any:
		a, _ := after.([]any)
		for i, v := range b {
			var av any
			if i < len(a) {
				av = a[i]
			}
			walk(v, av, path+"[]", fn)
		}
	}
}

// goRegexp approximates a VRL filter in Go, to show what matched. Vector
// decides what's masked; this only extracts the text for the report. Rust's
// \d is any Unicode digit, Go's only ASCII.
func goRegexp(expr string) *regexp.Regexp {
	src := strings.TrimSuffix(strings.TrimPrefix(expr, "r'"), "'")
	return regexp.MustCompile(strings.ReplaceAll(src, `\d`, `\p{Nd}`))
}

type kv struct {
	k string
	n int
}

func sorted(m map[string]int) []kv {
	out := make([]kv, 0, len(m))
	for k, n := range m {
		out = append(out, kv{k, n})
	}
	slices.SortFunc(out, func(a, b kv) int { return cmp.Or(b.n-a.n, strings.Compare(a.k, b.k)) })
	return out
}

func topN(m map[string]int, n int, sep string) string {
	var parts []string
	for _, kv := range sorted(m)[:min(n, len(m))] {
		parts = append(parts, fmt.Sprintf("`%s` (%d)", kv.k, kv.n))
	}
	return strings.Join(parts, sep)
}

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "redactfp:", err)
	os.Exit(1)
}
