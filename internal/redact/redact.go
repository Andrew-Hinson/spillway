// Package redact maps the LogPipeline redaction patterns to filters for
// VRL's redact() function. The renderer embeds them in each team's redaction
// transform.
package redact

import (
	"fmt"
	"slices"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
)

// filters holds one VRL redact() filter expression per pattern.
var filters = map[spillwayv1alpha1.RedactionPattern]string{
	// Anything SSN-shaped (NNN-NN-NNNN). Deliberately broader than valid
	// SSNs: VRL's built-in us_social_security_number matcher lets some
	// valid-format SSNs through (234-56-7890, 123-45-0013), and for
	// compliance an over-masked ID beats a leaked SSN.
	spillwayv1alpha1.RedactSSN:   `r'\b\d{3}-\d{2}-\d{4}\b'`,
	spillwayv1alpha1.RedactEmail: `r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'`,
	// North American numbers with separators, optionally with +1: 555-867-5309,
	// (555) 867-5309, +1 555.867.5309. Separators are required so that IDs and
	// timestamps made of bare digits aren't masked.
	spillwayv1alpha1.RedactPhone: `r'(?:\+1[ .-]?)?(?:\(\d{3}\) ?|\b\d{3}[ .-])\d{3}[ .-]\d{4}\b'`,
	// Spillway's (fictional) member ID format: MBR- and eight digits.
	spillwayv1alpha1.RedactMemberID: `r'\bMBR-\d{8}\b'`,
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
