package main

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
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

// bench/eventcheck.py derives the same IDs with Python's uuid.uuid5: this
// value came from it.
func TestEventIDMatchesTheChecker(t *testing.T) {
	if got, want := EventID("agg1", 42), "71d7a17c-c456-55a6-95e9-6f89f729dca0"; got != want {
		t.Fatalf("EventID(agg1, 42) = %s, want %s", got, want)
	}
}

func TestRunEventsAreTaggedAndCountable(t *testing.T) {
	g := NewGenerator(1, "loadgen", "chaos.load").ForRun("agg1")
	for seq := int64(1); seq <= 8; seq++ {
		raw, err := g.Next(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		var ev struct {
			Level    string
			Loadgen  RunTag
			Spillway Stamp
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Loadgen != (RunTag{Run: "agg1", Seq: seq}) {
			t.Errorf("event %d tagged %+v", seq, ev.Loadgen)
		}
		if ev.Spillway.EventID != EventID("agg1", seq) {
			t.Errorf("event %d: event_id %s isn't derived from its run and seq", seq, ev.Spillway.EventID)
		}
		if want := map[bool]string{true: "debug", false: "info"}[seq%4 == 0]; ev.Level != want {
			t.Errorf("event %d: level %q, want %q", seq, ev.Level, want)
		}
	}
	// Without a run, events carry neither (the benchmark's shape is unchanged).
	raw, _ := NewGenerator(1, "loadgen", "bench.load").Next(time.Now())
	if strings.Contains(string(raw), `"loadgen":{`) || strings.Contains(string(raw), `"level":`) {
		t.Errorf("an unnamed run's event is tagged: %s", raw)
	}
}

func TestLineOutputWritesOneEventPerLine(t *testing.T) {
	var buf bytes.Buffer
	out := lineOutput{&buf}
	var errs []error
	for _, v := range []string{`{"a":1}`, `{"b":2}`} {
		out.send(context.Background(), []byte(v), func(err error) { errs = append(errs, err) })
	}
	if buf.String() != "{\"a\":1}\n{\"b\":2}\n" || len(errs) != 2 || errs[0] != nil || errs[1] != nil {
		t.Fatalf("wrote %q, results %v", buf.String(), errs)
	}
}
