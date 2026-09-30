package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const feederName = "wikimedia"

// Feeder stamps stream events and produces them to Kafka.
type Feeder struct {
	Kafka   *kgo.Client // with a default produce topic set
	Metrics *metrics
	Log     *slog.Logger
	Now     func() time.Time
}

// Handle produces one event. It blocks while the producer's buffer is full,
// which in turn stops the stream being read: backpressure instead of drops.
func (f *Feeder) Handle(ctx context.Context, ev Event) {
	f.Metrics.received.Inc()

	u, err := inspect(ev.Data)
	if err != nil {
		f.Metrics.skipped.WithLabelValues("invalid").Inc()
		f.Log.Debug("skipping invalid event", "err", err)
		return
	}
	// Wikimedia sends synthetic canary events to monitor the stream itself.
	if u.Meta.Domain == "canary" {
		f.Metrics.skipped.WithLabelValues("canary").Inc()
		return
	}

	// Prefer the upstream ID: an event replayed after a reconnect keeps its ID,
	// so duplicates can be removed downstream.
	id := u.Meta.ID
	if id == "" {
		id = uuid.Must(uuid.NewV7()).String()
	}
	now := f.Now().UTC()
	value, err := stamp(ev.Data, Stamp{EventID: id, ProducedAt: now, Feeder: feederName})
	if err != nil {
		f.Metrics.skipped.WithLabelValues("invalid").Inc()
		return
	}

	f.Kafka.Produce(ctx, &kgo.Record{Key: []byte(id), Value: value, Timestamp: now}, f.produced)
}

func (f *Feeder) produced(r *kgo.Record, err error) {
	if err != nil {
		f.Metrics.produceErrors.Inc()
		f.Log.Warn("produce failed", "err", err, "event_id", string(r.Key))
		return
	}
	f.Metrics.produced.Inc()
	f.Metrics.producedBytes.Add(float64(len(r.Value)))
	f.Metrics.lastProduced.SetToCurrentTime()
}
