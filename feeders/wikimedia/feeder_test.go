package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestFeederProduces runs events through the feeder into an in-memory Kafka
// cluster and reads them back.
func TestFeederProduces(t *testing.T) {
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

	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	f := &Feeder{
		Kafka:   producer,
		Metrics: newMetrics(prometheus.NewRegistry(), "test"),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:     func() time.Time { return at },
	}
	ctx := context.Background()
	for _, data := range []string{
		`{"meta":{"id":"upstream-1","domain":"en.wikipedia.org"},"title":"A"}`,
		`{"meta":{"id":"canary-1","domain":"canary"},"title":"canary"}`,
		`not json`,
		`{"title":"no meta id"}`,
	} {
		f.Handle(ctx, Event{Data: []byte(data)})
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	var recs []*kgo.Record
	fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for len(recs) < 2 && fetchCtx.Err() == nil {
		recs = append(recs, consumer.PollFetches(fetchCtx).Records()...)
	}
	if len(recs) != 2 {
		t.Fatalf("consumed %d records, want 2", len(recs))
	}

	type event struct {
		Title    string `json:"title"`
		Spillway Stamp  `json:"spillway"`
	}
	var first, second event
	if err := json.Unmarshal(recs[0].Value, &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(recs[1].Value, &second); err != nil {
		t.Fatal(err)
	}
	if first.Title != "A" || first.Spillway.EventID != "upstream-1" || string(recs[0].Key) != "upstream-1" ||
		!first.Spillway.ProducedAt.Equal(at) || first.Spillway.Feeder != "wikimedia" {
		t.Errorf("first record = %s (key %s)", recs[0].Value, recs[0].Key)
	}
	if second.Spillway.EventID == "" || second.Spillway.EventID == first.Spillway.EventID {
		t.Errorf("event without meta.id got ID %q, want a fresh UUID", second.Spillway.EventID)
	}

	m := f.Metrics
	for name, c := range map[string]struct{ got, want float64 }{
		"received": {testutil.ToFloat64(m.received), 4},
		"produced": {testutil.ToFloat64(m.produced), 2},
		"canary":   {testutil.ToFloat64(m.skipped.WithLabelValues("canary")), 1},
		"invalid":  {testutil.ToFloat64(m.skipped.WithLabelValues("invalid")), 1},
		"errors":   {testutil.ToFloat64(m.produceErrors), 0},
	} {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", name, c.got, c.want)
		}
	}
}
