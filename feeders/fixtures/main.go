// Command fixtures injects known PII fixtures into Spillway's live streams at
// a set rate, so redaction can be measured: every fixture carries one SSN,
// email, phone number or member ID from a fictional range, and is tagged
// (spillway.fixture: {id, pattern}) so it can be counted wherever it lands.
//
// It writes recentchange-shaped events to a Kafka topic (the Wikimedia feed,
// by default), so teams reading that topic get fixtures mixed into real
// traffic, and/or JSON log lines to stdout, so a team that claims the
// injector's namespace gets them as pod logs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	brokers    string
	topic      string
	stdout     bool
	rate       float64
	startID    int64
	listenAddr string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.brokers, "brokers", env("KAFKA_BROKERS", ""), "comma-separated Kafka seed brokers; empty disables the Kafka output")
	flag.StringVar(&cfg.topic, "topic", env("KAFKA_TOPIC", "wikimedia.recentchange"), "topic to inject fixtures into")
	flag.BoolVar(&cfg.stdout, "stdout", env("STDOUT", "true") == "true", "also write fixtures to stdout as JSON log lines (for pod-log pipelines)")
	flag.Float64Var(&cfg.rate, "rate", 1, "fixtures per second, per output")
	// Fixture IDs start from the clock, so a restarted injector doesn't reuse them.
	flag.Int64Var(&cfg.startID, "start-id", time.Now().UnixMilli(), "first fixture ID")
	flag.StringVar(&cfg.listenAddr, "listen", env("LISTEN_ADDR", ":8080"), "address for /metrics and /healthz")
	flag.Parse()

	// Logs go to stderr: stdout carries the fixture log lines.
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if cfg.rate <= 0 || (cfg.brokers == "" && !cfg.stdout) {
		log.Error("nothing to do: need -rate > 0 and at least one output (-brokers or -stdout)")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log, os.Stdout); err != nil {
		log.Error("fixture injector failed", "err", err)
		os.Exit(1)
	}
}

type metrics struct {
	injected *prometheus.CounterVec
	errors   *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) metrics {
	m := metrics{
		injected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "spillway_fixtures_injected_total",
			Help: "Fixtures injected, by PII pattern and output (kafka or stdout).",
		}, []string{"pattern", "output"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "spillway_fixtures_errors_total",
			Help: "Fixtures that couldn't be injected, by output.",
		}, []string{"output"}),
	}
	reg.MustRegister(m.injected, m.errors)
	return m
}

func run(ctx context.Context, cfg config, log *slog.Logger, stdout io.Writer) error {
	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	var kafka *kgo.Client
	if cfg.brokers != "" {
		var err error
		kafka, err = kgo.NewClient(
			kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
			kgo.DefaultProduceTopic(cfg.topic),
			kgo.ClientID("spillway-fixtures"),
		)
		if err != nil {
			return err
		}
		defer kafka.Close()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	}()

	log.Info("injecting fixtures", "rate", cfg.rate, "kafka", cfg.brokers != "", "topic", cfg.topic, "stdout", cfg.stdout, "start_id", cfg.startID)
	inj := &injector{kafka: kafka, stdout: stdout, writeStdout: cfg.stdout, metrics: m, log: log, now: time.Now}
	ticker := time.NewTicker(time.Duration(float64(time.Second) / cfg.rate))
	defer ticker.Stop()
	for id := cfg.startID; ; id++ {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if kafka != nil {
				_ = kafka.Flush(flushCtx)
			}
			return srv.Shutdown(flushCtx)
		case <-ticker.C:
			inj.inject(ctx, Next(id))
		}
	}
}

// injector writes one fixture to each configured output.
type injector struct {
	kafka       *kgo.Client
	stdout      io.Writer
	writeStdout bool
	metrics     metrics
	log         *slog.Logger
	now         func() time.Time
}

func (in *injector) inject(ctx context.Context, f Fixture) {
	now := in.now()
	if in.kafka != nil {
		ev, err := KafkaEvent(f, now)
		if err != nil {
			in.metrics.errors.WithLabelValues("kafka").Inc()
			in.log.Error("encoding fixture", "err", err)
		} else {
			in.kafka.Produce(ctx, &kgo.Record{Value: ev}, func(_ *kgo.Record, err error) {
				if err != nil {
					in.metrics.errors.WithLabelValues("kafka").Inc()
					in.log.Warn("producing fixture", "id", f.ID, "err", err)
					return
				}
				in.metrics.injected.WithLabelValues(f.Pattern, "kafka").Inc()
			})
		}
	}
	if in.writeStdout {
		line, err := LogLine(f, now)
		if err == nil {
			_, err = fmt.Fprintf(in.stdout, "%s\n", line)
		}
		if err != nil {
			in.metrics.errors.WithLabelValues("stdout").Inc()
			return
		}
		in.metrics.injected.WithLabelValues(f.Pattern, "stdout").Inc()
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
