// Command wikimedia reads the Wikimedia EventStreams recentchange feed over
// SSE, stamps each event with an ID and produce timestamp, and produces it to
// Kafka.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

type config struct {
	streamURL   string
	userAgent   string
	brokers     string
	topic       string
	listenAddr  string
	idleTimeout time.Duration
	logLevel    string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.streamURL, "stream-url", env("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"), "SSE stream to read")
	// Wikimedia asks clients to identify themselves: https://meta.wikimedia.org/wiki/User-Agent_policy
	flag.StringVar(&cfg.userAgent, "user-agent", env("USER_AGENT", "spillway-wikimedia-feeder/0.1 (https://github.com/Andrew-Hinson/spillway)"), "User-Agent sent upstream")
	flag.StringVar(&cfg.brokers, "brokers", env("KAFKA_BROKERS", "spillway-kafka-bootstrap.kafka:9092"), "comma-separated Kafka seed brokers")
	flag.StringVar(&cfg.topic, "topic", env("KAFKA_TOPIC", "wikimedia.recentchange"), "Kafka topic to produce to")
	flag.StringVar(&cfg.listenAddr, "listen", env("LISTEN_ADDR", ":8080"), "address for /metrics and /healthz")
	flag.DurationVar(&cfg.idleTimeout, "idle-timeout", 60*time.Second, "reconnect if the stream is silent this long")
	flag.StringVar(&cfg.logLevel, "log-level", env("LOG_LEVEL", "info"), "debug, info, warn or error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.logLevel)); err != nil {
		slog.Error("bad -log-level", "err", err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil {
		log.Error("feeder failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, log *slog.Logger) error {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := newMetrics(reg, feederName)

	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.DefaultProduceTopic(cfg.topic),
		kgo.ClientID("spillway-wikimedia-feeder"),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.MaxBufferedRecords(10_000),
	)
	if err != nil {
		return err
	}
	defer kafka.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()

	feeder := &Feeder{Kafka: kafka, Metrics: m, Log: log, Now: time.Now}
	stream := &Stream{
		URL:         cfg.streamURL,
		UserAgent:   cfg.userAgent,
		Client:      &http.Client{}, // no overall timeout: the response is a long-lived stream
		IdleTimeout: cfg.idleTimeout,
		MinBackoff:  time.Second,
		MaxBackoff:  30 * time.Second,
		Metrics:     m,
		Log:         log,
	}

	log.Info("feeder starting", "stream", cfg.streamURL, "brokers", cfg.brokers, "topic", cfg.topic)
	runCtx, cancel := context.WithCancel(ctx)
	go func() {
		if err := <-srvErr; !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
			cancel()
		}
	}()
	stream.Run(runCtx, func(ev Event) { feeder.Handle(runCtx, ev) })
	cancel()

	log.Info("shutting down; flushing producer")
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFlush()
	if err := kafka.Flush(flushCtx); err != nil {
		log.Warn("flush incomplete", "err", err)
	}
	return srv.Shutdown(flushCtx)
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
