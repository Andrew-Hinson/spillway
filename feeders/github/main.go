// Command github polls GitHub's public events API, stamps each new event with
// its ID and a produce timestamp, and produces it to Kafka: a second feeder
// with a different shape (nested actor, repo and payload) and a much lower
// rate than Wikimedia's.
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

const feederName = "github"

type config struct {
	apiURL     string
	userAgent  string
	pages      int
	brokers    string
	topic      string
	listenAddr string
	logLevel   string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.apiURL, "api-url", env("API_URL", "https://api.github.com/events"), "public events endpoint")
	flag.StringVar(&cfg.userAgent, "user-agent", env("USER_AGENT", "spillway-github-feeder/0.1 (https://github.com/Andrew-Hinson/spillway)"), "User-Agent sent upstream")
	flag.IntVar(&cfg.pages, "pages", 3, "pages of 100 events to read per poll at most (the API serves 3)")
	flag.StringVar(&cfg.brokers, "brokers", env("KAFKA_BROKERS", "spillway-kafka-bootstrap.kafka:9092"), "comma-separated Kafka seed brokers")
	flag.StringVar(&cfg.topic, "topic", env("KAFKA_TOPIC", "github.events"), "Kafka topic to produce to")
	flag.StringVar(&cfg.listenAddr, "listen", env("LISTEN_ADDR", ":8080"), "address for /metrics and /healthz")
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
	// The token is optional and read from the environment only, so it never
	// appears in a process listing.
	if err := run(ctx, cfg, os.Getenv("GITHUB_TOKEN"), log); err != nil {
		log.Error("feeder failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, token string, log *slog.Logger) error {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := newMetrics(reg, feederName)

	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.DefaultProduceTopic(cfg.topic),
		kgo.ClientID("spillway-github-feeder"),
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
	poller := &Poller{
		URL:       cfg.apiURL,
		Token:     token,
		UserAgent: cfg.userAgent,
		Pages:     cfg.pages,
		Client:    &http.Client{Timeout: 30 * time.Second},
		Seen:      NewSeen(10_000),
		Metrics:   m,
		Log:       log,
		Now:       time.Now,
		Sleep:     SleepCtx,
	}

	log.Info("feeder starting", "api", cfg.apiURL, "authenticated", token != "", "brokers", cfg.brokers, "topic", cfg.topic)
	runCtx, cancel := context.WithCancel(ctx)
	go func() {
		if err := <-srvErr; !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
			cancel()
		}
	}()
	poller.Run(runCtx, func(id string, ev []byte) { feeder.Handle(runCtx, id, ev) })
	cancel()

	log.Info("shutting down; flushing producer")
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFlush()
	if err := kafka.Flush(flushCtx); err != nil {
		log.Warn("flush incomplete", "err", err)
	}
	return srv.Shutdown(flushCtx)
}

// Feeder stamps events and produces them to Kafka.
type Feeder struct {
	Kafka   *kgo.Client // with a default produce topic set
	Metrics *metrics
	Log     *slog.Logger
	Now     func() time.Time
}

// Handle produces one event, keyed and identified by its GitHub event ID, so
// an event the API serves again after a feeder restart lands on the same
// partition with the same ID and the pipeline's dedupe drops it.
func (f *Feeder) Handle(ctx context.Context, id string, data []byte) {
	value, err := stamp(data, Stamp{EventID: id, ProducedAt: f.Now().UTC(), Feeder: feederName})
	if err != nil {
		f.Metrics.skipped.WithLabelValues("invalid").Inc()
		return
	}
	f.Kafka.Produce(ctx, &kgo.Record{Key: []byte(id), Value: value, Timestamp: f.Now()}, f.produced)
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

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
