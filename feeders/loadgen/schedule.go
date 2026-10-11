package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseSteps reads a comma-separated list of rates in events per second,
// e.g. "250,500,1000".
func ParseSteps(s string) ([]int, error) {
	var steps []int
	for f := range strings.SplitSeq(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("step %q: want a positive number of events per second", f)
		}
		steps = append(steps, n)
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("no steps")
	}
	return steps, nil
}

// Due is how many events a step at rate events/s should have sent elapsed
// into the step. Pacing against the step's start, rather than a ticker per
// event, keeps the rate exact over the step even if a send runs late.
func Due(rate int, elapsed time.Duration) int64 {
	if elapsed <= 0 {
		return 0
	}
	return int64(float64(rate) * elapsed.Seconds())
}
