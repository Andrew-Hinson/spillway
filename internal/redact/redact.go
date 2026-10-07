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
	// VRL's built-in US SSN matcher (NNN-NN-NNNN, excluding invalid ranges).
	spillwayv1alpha1.RedactSSN:   `"us_social_security_number"`,
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
