// Package redact maps the LogPipeline redaction patterns to filters for
// VRL's redact() function, and builds the VRL that applies them. The renderer
// embeds it in each team's redaction transform and in the platform paths.
//
// The patterns have no word boundaries (\b). Logs carry PII next to letters
// and digits more often than it seems: pod logs arrive as unparsed strings of
// escaped JSON ("x\n123-45-6789" has an "n" before the SSN), values get glued
// to labels ("SSN123-45-6789") and phone numbers to extensions
// ("555-867-5309x12"). A \b pattern lets all of those through, and VRL's
// regex engine has no lookbehind to say "not next to a digit" instead. So
// anything PII-shaped is masked, even inside a longer run of digits: for
// compliance an over-masked ID beats a leaked SSN. docs/results/m3.2-redaction.md
// measures what that costs on live data; testdata/corpus.yaml holds the cases.
package redact

import (
	"fmt"
	"slices"
	"strings"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
)

// filters holds one VRL redact() filter expression per pattern.
var filters = map[spillwayv1alpha1.RedactionPattern]string{
	// Anything SSN-shaped (NNN-NN-NNNN). Deliberately broader than valid
	// SSNs: VRL's built-in us_social_security_number matcher lets some
	// valid-format SSNs through (234-56-7890, 123-45-0013).
	spillwayv1alpha1.RedactSSN: `r'\d{3}-\d{2}-\d{4}'`,
	// Letters in any script, so internationalized addresses are masked whole,
	// and %40 as well as @, so URL-encoded addresses (in Wikimedia's
	// meta.uri, for one) are too.
	spillwayv1alpha1.RedactEmail: `r'[\p{L}\p{N}._%+-]+(?:@|%40)[\p{L}\p{N}.-]+\.\p{L}{2,}'`,
	// North American numbers with separators, optionally with +1: 555-867-5309,
	// (555) 867-5309, +1 555.867.5309. Separators are required so that IDs and
	// timestamps made of bare digits aren't masked.
	spillwayv1alpha1.RedactPhone: `r'(?:\+1[ .-]?)?(?:\(\d{3}\) ?|\d{3}[ .-])\d{3}[ .-]\d{4}'`,
	// Spillway's (fictional) member ID format: MBR- and eight digits.
	spillwayv1alpha1.RedactMemberID: `r'MBR-\d{8}'`,
}

// All returns every pattern, in a stable order. It's what a pipeline masks
// unless its spec says otherwise, and what the platform paths mask.
func All() []spillwayv1alpha1.RedactionPattern {
	all := make([]spillwayv1alpha1.RedactionPattern, 0, len(filters))
	for p := range filters {
		all = append(all, p)
	}
	slices.Sort(all)
	return all
}

// Filters returns the VRL filter expressions for patterns, in a stable order.
func Filters(patterns []spillwayv1alpha1.RedactionPattern) ([]string, error) {
	sorted := slices.Clone(patterns)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	out := make([]string, 0, len(sorted))
	for _, p := range sorted {
		f, ok := filters[p]
		if !ok {
			return nil, fmt.Errorf("unknown redaction pattern %q", p)
		}
		out = append(out, f)
	}
	return out, nil
}

// VRL returns a remap program that masks patterns in every string of the
// event, however deeply nested, except Spillway's own metadata under
// .spillway (event IDs, fixture tags), which must survive to be counted.
func VRL(patterns []spillwayv1alpha1.RedactionPattern) (string, error) {
	f, err := Filters(patterns)
	if err != nil {
		return "", err
	}
	return Program(f...), nil
}

// Program is VRL for any redact() filter expressions, for trying a candidate
// filter against real data before it joins the library.
func Program(filters ...string) string {
	return "# Redact every string in the event except Spillway's own metadata.\n" +
		"meta = .spillway\n" +
		fmt.Sprintf(". = redact(., filters: [%s])\n", strings.Join(filters, ", ")) +
		"if meta != null { .spillway = meta }\n"
}

// examples holds a value each pattern must mask, for generated tests.
var examples = map[spillwayv1alpha1.RedactionPattern]string{
	spillwayv1alpha1.RedactSSN:      "123-45-6789",
	spillwayv1alpha1.RedactEmail:    "jo.doe@example.com",
	spillwayv1alpha1.RedactPhone:    "(555) 867-5309",
	spillwayv1alpha1.RedactMemberID: "MBR-12345678",
}

// Example returns a value the pattern must mask.
func Example(p spillwayv1alpha1.RedactionPattern) string {
	return examples[p]
}

// LookAlike is a log line full of things that resemble PII but that no
// pattern may mask, for generated tests: timestamps, bare digit runs, IPs,
// versions, an address with no domain, a member ID one digit short.
const LookAlike = "2026-10-09T12:34:56.789Z rev 1234567890 from 10.0.0.1 v1.24.3 user@localhost MBR-1234567 pod app-5d8f9-x2x7q"
