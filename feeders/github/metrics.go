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
	requests      *prometheus.CounterVec
	rateRemaining prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer, feeder string) *metrics {
	f := promauto.With(prometheus.WrapRegistererWith(prometheus.Labels{"feeder": feeder}, reg))
	return &metrics{
		received: f.NewCounter(prometheus.CounterOpts{
			Name: "spillway_feeder_events_received_total",
			Help: "Events read from the upstream API, including ones already seen.",
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
			Help: "Polls of the upstream API that failed.",
		}),
		connected: f.NewGauge(prometheus.GaugeOpts{
			Name: "spillway_feeder_stream_connected",
			Help: "1 while the last poll of the upstream API succeeded.",
		}),
		lastProduced: f.NewGauge(prometheus.GaugeOpts{
			Name: "spillway_feeder_last_produced_timestamp_seconds",
			Help: "Unix time of the last event acknowledged by Kafka.",
		}),
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "spillway_feeder_api_requests_total",
			Help: "Requests to the upstream API, by HTTP status.",
		}, []string{"status"}),
		rateRemaining: f.NewGauge(prometheus.GaugeOpts{
			Name: "spillway_feeder_api_rate_remaining",
			Help: "Requests left in the upstream API's rate-limit window.",
		}),
	}
}
