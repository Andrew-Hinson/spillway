package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	spillwayv1alpha1 "github.com/Andrew-Hinson/spillway/api/v1alpha1"
	"github.com/Andrew-Hinson/spillway/internal/redact"
)

// Each value is from a range that's fictional by design.
var fictional = map[string]*regexp.Regexp{
	"ssn":      regexp.MustCompile(`^123-45-(000[1-9]|00[1-9]\d|0[1-9]\d\d|[1-9]\d\d\d)$`),
	"email":    regexp.MustCompile(`^fixture-\d+@fixtures\.spillway\.test$`),
	"phone":    regexp.MustCompile(`^\(212\) 555-01\d\d$`),
	"memberId": regexp.MustCompile(`^MBR-9\d{7}$`),
}

func TestValuesAreDeterministicAndFictional(t *testing.T) {
	for id := int64(0); id < 20000; id++ {
		f := Next(id)
		if f.Pattern != Patterns[id%4] {
			t.Fatalf("id %d: pattern %s, want patterns in turn", id, f.Pattern)
		}
		if f.Value != Value(id, f.Pattern) {
			t.Fatalf("id %d: value not deterministic", id)
		}
		if !fictional[f.Pattern].MatchString(f.Value) {
			t.Fatalf("id %d: %s value %q is outside its fictional range", id, f.Pattern, f.Value)
		}
	}
}

func TestEventsCarryTheValueAndATag(t *testing.T) {
	f := Next(6) // phone
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	for name, render := range map[string]func(Fixture, time.Time) ([]byte, error){"kafka": KafkaEvent, "stdout": LogLine} {
		b, err := render(f, now)
		if err != nil {
			t.Fatal(err)
		}
		var ev struct {
			Spillway struct {
				EventID string `json:"event_id"`
				Feeder  string `json:"feeder"`
				Fixture Tag    `json:"fixture"`
			} `json:"spillway"`
		}
		if err := json.Unmarshal(b, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Spillway.Fixture != f.Tag || ev.Spillway.Feeder != "fixtures" || ev.Spillway.EventID == "" {
			t.Errorf("%s: spillway metadata = %+v, want fixture tag %+v", name, ev.Spillway, f.Tag)
		}
		// The value appears twice, in free text and a nested field, and never
		// in the tag, which redaction leaves alone.
		if n := strings.Count(string(b), f.Value); n != 2 {
			t.Errorf("%s: value appears %d times, want 2:\n%s", name, n, b)
		}
	}
}

// Fixtures are only useful if Spillway's redaction library masks every one
// of them. This runs the library's own VRL filters through the real vector
// binary over thousands of values and both event shapes.
func TestSpillwayRedactionMasksEveryFixture(t *testing.T) {
	bin := os.Getenv("VECTOR_BIN")
	if bin == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("VECTOR_BIN is not set; run the tests with `make test`")
		}
		t.Skip("VECTOR_BIN is not set (run `make test`)")
	}
	all := []spillwayv1alpha1.RedactionPattern{spillwayv1alpha1.RedactSSN, spillwayv1alpha1.RedactEmail, spillwayv1alpha1.RedactPhone, spillwayv1alpha1.RedactMemberID}
	filters, err := redact.Filters(all)
	if err != nil {
		t.Fatal(err)
	}

	var values []string
	for id := int64(0); id < 4000; id++ {
		values = append(values, Next(id).Value)
	}
	valuesJSON, _ := json.Marshal(values)
	var events []string
	for id := int64(0); id < 8; id++ {
		k, _ := KafkaEvent(Next(id), time.Now())
		l, _ := LogLine(Next(id), time.Now())
		events = append(events, string(k), string(l))
	}
	eventsJSON, _ := json.Marshal(events)

	config := fmt.Sprintf(`
sources: {in: {type: demo_logs, format: json}}
transforms:
  redact:
    type: remap
    inputs: [in]
    source: |
      meta = .spillway
      . = redact(., filters: [%s])
      .spillway = meta
sinks: {out: {type: blackhole, inputs: [redact]}}
tests:
  - name: every fixture value is masked
    inputs:
      - insert_at: redact
        type: vrl
        source: '. = {"values": %s, "events": %s, "spillway": {"fixture": {"id": 1}}}'
    outputs:
      - extract_from: redact
        conditions:
          - type: vrl
            source: |
              out = encode_json(.)
              assert!(!match(out, r'123-45-\d{4}'), message: "an SSN survived")
              assert!(!contains(out, "@fixtures.spillway.test"), message: "an email survived")
              assert!(!match(out, r'555-01\d\d'), message: "a phone number survived")
              assert!(!match(out, r'MBR-9\d{7}'), message: "a member ID survived")
              assert_eq!(.spillway.fixture.id, 1)
`, strings.Join(filters, ", "), strings.ReplaceAll(string(valuesJSON), "'", "''"), strings.ReplaceAll(string(eventsJSON), "'", "''"))

	path := filepath.Join(t.TempDir(), "redact.yaml")
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(bin, "test", path).CombinedOutput()
	if err != nil || !bytes.Contains(out, []byte("... passed")) {
		t.Fatalf("vector test: %v\n%s", err, out)
	}
}

func TestInjectWritesToKafkaAndStdout(t *testing.T) {
	const topic = "wikimedia.recentchange"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	var stdout bytes.Buffer
	m := newMetrics(prometheus.NewRegistry())
	inj := &injector{kafka: producer, stdout: &stdout, writeStdout: true, metrics: m,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)), now: time.Now}
	ctx := context.Background()
	for id := int64(100); id < 108; id++ {
		inj.inject(ctx, Next(id))
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var recs []*kgo.Record
	for len(recs) < 8 && fetchCtx.Err() == nil {
		recs = append(recs, consumer.PollFetches(fetchCtx).Records()...)
	}
	if len(recs) != 8 {
		t.Fatalf("got %d records, want 8", len(recs))
	}
	seen := map[int64]bool{}
	for _, r := range recs {
		var ev struct {
			Spillway struct{ Fixture Tag } `json:"spillway"`
		}
		if err := json.Unmarshal(r.Value, &ev); err != nil {
			t.Fatal(err)
		}
		seen[ev.Spillway.Fixture.ID] = true
	}
	if len(seen) != 8 {
		t.Errorf("distinct fixture IDs in Kafka = %d, want 8", len(seen))
	}
	if lines := strings.Count(stdout.String(), "\n"); lines != 8 {
		t.Errorf("stdout lines = %d, want 8", lines)
	}
	for _, p := range Patterns {
		for _, output := range []string{"kafka", "stdout"} {
			if got := testutil.ToFloat64(m.injected.WithLabelValues(p, output)); got != 2 {
				t.Errorf("injected{pattern=%s,output=%s} = %v, want 2", p, output, got)
			}
		}
	}
}
