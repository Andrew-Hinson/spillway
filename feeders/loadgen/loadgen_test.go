package main

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"
)

func TestEventsCarryTheFeederStamp(t *testing.T) {
	g := NewGenerator(1, "loadgen", "bench.load")
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	uuidRE := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for range 1000 {
		raw, err := g.Next(now)
		if err != nil {
			t.Fatal(err)
		}
		var ev map[string]any
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatalf("not JSON: %v", err)
		}
		sp, ok := ev["spillway"].(map[string]any)
		if !ok {
			t.Fatalf("no spillway stamp: %s", raw)
		}
		id, _ := sp["event_id"].(string)
		if !uuidRE.MatchString(id) {
			t.Fatalf("event_id %q isn't a UUID", id)
		}
		if seen[id] {
			t.Fatalf("event_id %s repeated", id)
		}
		seen[id] = true
		if sp["feeder"] != "loadgen" {
			t.Fatalf("feeder = %v", sp["feeder"])
		}
		if got, err := time.Parse(time.RFC3339Nano, sp["produced_at"].(string)); err != nil || !got.Equal(now) {
			t.Fatalf("produced_at = %v (%v), want %v", sp["produced_at"], err, now)
		}
		if _, ok := ev["type"].(string); !ok {
			t.Fatalf("no type: %s", raw)
		}
	}
}

// The benchmark's events should cost the pipeline about what live Wikimedia
// events do: M1 measured an average of 1,524 B read from Kafka.
func TestEventSizeMatchesTheLiveFeed(t *testing.T) {
	g := NewGenerator(7, "loadgen", "bench.load")
	var total int
	const n = 5000
	types := map[string]int{}
	for range n {
		raw, err := g.Next(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		total += len(raw)
		var ev struct{ Type string }
		_ = json.Unmarshal(raw, &ev)
		types[ev.Type]++
	}
	if avg := total / n; avg < 1300 || avg > 1700 {
		t.Errorf("average event is %d B, want about 1,500", avg)
	}
	for _, typ := range []string{"edit", "categorize", "log", "new"} {
		if types[typ] == 0 {
			t.Errorf("no %s events in %d", typ, n)
		}
	}
}

func TestParseSteps(t *testing.T) {
	got, err := ParseSteps(" 250, 500,1000,")
	if err != nil || len(got) != 3 || got[0] != 250 || got[2] != 1000 {
		t.Fatalf("ParseSteps = %v, %v", got, err)
	}
	for _, bad := range []string{"", "0", "-5", "fast", "100,x"} {
		if _, err := ParseSteps(bad); err == nil {
			t.Errorf("ParseSteps(%q) accepted", bad)
		}
	}
}

func TestDue(t *testing.T) {
	for _, tc := range []struct {
		rate    int
		elapsed time.Duration
		want    int64
	}{
		{1000, 0, 0},
		{1000, -time.Second, 0},
		{1000, 1500 * time.Millisecond, 1500},
		{250, 3 * time.Minute, 45000},
		{3, 500 * time.Millisecond, 1},
	} {
		if got := Due(tc.rate, tc.elapsed); got != tc.want {
			t.Errorf("Due(%d, %v) = %d, want %d", tc.rate, tc.elapsed, got, tc.want)
		}
	}
}
