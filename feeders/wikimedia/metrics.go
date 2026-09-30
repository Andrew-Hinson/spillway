package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	received      prometheus.Counter
	skipped       *prometheus.CounterVec
	produced      prometheus.Counter
	producedBytes prometheus.Counter
	produceErrors prometheus.Counter
	reconnects    prometheus.Counter
	connected     prometheus.Gauge
	lastProduced  prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer, feeder string) *metrics {
	f := promauto.With(prometheus.WrapRegistererWith(prometheus.Labels{"feeder": feeder}, reg))
	return &metrics{
		received: f.NewCounter(prometheus.CounterOpts{
			Name: "spillway_feeder_events_received_total",
			Help: "Events read from the upstream stream.",
		}),
		skipped: f.NewCounterVec(prometheus.CounterOpts{
			Name: "spillway_feeder_events_skipped_total",
			Help: "Events read but not produced, by reason.",
		}, []string{"reason"}),
		produced: f.NewCounter(prometheus.CounterOpts{
			Name: "spillway_feeder_events_produced_total",
			Help: "Events acknowledged by Kafka.",
		}),
		producedBytes: f.NewCounter(prometheus.CounterOpts{
			Name: "spillway_feeder_produced_bytes_total",
			Help: "Bytes of event payload acknowledged by Kafka.",
		}),
		produceErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "spillway_feeder_produce_errors_total",
			Help: "Events Kafka failed to accept.",
		}),
		reconnects: f.NewCounter(prometheus.CounterOpts{
			Name: "spillway_feeder_stream_reconnects_total",
			Help: "Times the upstream stream connection was lost or refused.",
		}),
		connected: f.NewGauge(prometheus.GaugeOpts{
			Name: "spillway_feeder_stream_connected",
			Help: "1 while connected to the upstream stream.",
		}),
		lastProduced: f.NewGauge(prometheus.GaugeOpts{
			Name: "spillway_feeder_last_produced_timestamp_seconds",
			Help: "Unix time of the last event acknowledged by Kafka.",
		}),
	}
}
