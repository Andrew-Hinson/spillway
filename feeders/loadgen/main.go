// Command loadgen produces synthetic feeder events to Kafka at stepped rates,
// for Spillway's benchmark (bench/m4_bench.sh). Each event is shaped like a
// Wikimedia recentchange event and carries the same spillway stamp as the
// live feeders (event_id, produced_at, feeder), so the aggregator's latency
// histogram and dedupe treat it like real traffic.
//
// It runs each rate in -steps for -step-duration, logs each step's start and
// end, and exits once the last step's events are acknowledged (after -hold,
// so Prometheus scrapes the final counters).
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

const feederName = "loadgen"

type config struct {
	brokers      string
	topic        string
	steps        []int
	stepDuration time.Duration
	hold         time.Duration
	seed         uint64
	listenAddr   string
}

func main() {
	var cfg config
	var steps string
	flag.StringVar(&cfg.brokers, "brokers", env("KAFKA_BROKERS", "spillway-kafka-bootstrap.kafka.svc:9092"), "comma-separated Kafka seed brokers")
	flag.StringVar(&cfg.topic, "topic", env("KAFKA_TOPIC", "bench.load"), "Kafka topic to produce to")
	flag.StringVar(&steps, "steps", env("STEPS", "250,500,1000"), "comma-separated rates, in events per second")
	flag.DurationVar(&cfg.stepDuration, "step-duration", 3*time.Minute, "how long each step runs")
	flag.DurationVar(&cfg.hold, "hold", 30*time.Second, "keep serving metrics this long after the last step")
	flag.Uint64Var(&cfg.seed, "seed", 1, "seed for the synthetic events")
	flag.StringVar(&cfg.listenAddr, "listen", env("LISTEN_ADDR", ":8080"), "address for /metrics and /healthz")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var err error
	if cfg.steps, err = ParseSteps(steps); err != nil {
		log.Error("bad -steps", "err", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil {
		log.Error("load generator failed", "err", err)
		os.Exit(1)
	}
}

type metrics struct {
	produced      prometheus.Counter
	producedBytes prometheus.Counter
	produceErrors prometheus.Counter
	target        prometheus.Gauge
}

// newMetrics uses the live feeders' metric names, labelled feeder="loadgen".
func newMetrics(reg prometheus.Registerer) metrics {
	f := promauto.With(prometheus.WrapRegistererWith(prometheus.Labels{"feeder": feederName}, reg))
	return metrics{
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
		target: f.NewGauge(prometheus.GaugeOpts{
			Name: "spillway_loadgen_target_events_per_second",
			Help: "The current step's target rate; 0 between and after steps.",
		}),
	}
}

// stepCount tallies one step's events. Acks arrive after the step ends, so
// they're counted against the step that sent them.
type stepCount struct {
	sent, acked, failed atomic.Int64
}

func run(ctx context.Context, cfg config, log *slog.Logger) error {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := newMetrics(reg)

	// The live feeders' producer settings, so latency is comparable with the
	// M1 baseline. No key: batches spread over every partition, so each
	// aggregator gets an even share.
	kafka, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(cfg.brokers, ",")...),
		kgo.DefaultProduceTopic(cfg.topic),
		kgo.ClientID("spillway-loadgen"),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.MaxBufferedRecords(100_000),
	)
	if err != nil {
		return err
	}
	defer kafka.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "err", err)
		}
	}()
	defer func() { _ = srv.Close() }()

	log.Info("load generator starting", "topic", cfg.topic, "steps", cfg.steps, "step_duration", cfg.stepDuration.String())
	gen := NewGenerator(cfg.seed, feederName, cfg.topic)
	counts := make([]stepCount, len(cfg.steps))
	for i, rate := range cfg.steps {
		if ctx.Err() != nil {
			break
		}
		c := &counts[i]
		m.target.Set(float64(rate))
		start := time.Now()
		log.Info("step started", "step", i, "rate", rate, "start", start.UTC())
		runStep(ctx, kafka, gen, m, c, rate, start, cfg.stepDuration)
		end := time.Now()
		m.target.Set(0)
		log.Info("step finished", "step", i, "rate", rate, "start", start.UTC(), "end", end.UTC(),
			"sent", c.sent.Load(), "achieved_per_sec", float64(c.sent.Load())/end.Sub(start).Seconds())
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := kafka.Flush(flushCtx); err != nil {
		log.Warn("flush incomplete", "err", err)
	}
	for i, rate := range cfg.steps {
		c := &counts[i]
		log.Info("step result", "step", i, "rate", rate, "sent", c.sent.Load(), "acked", c.acked.Load(), "failed", c.failed.Load())
	}
	log.Info("load generator done; holding for the last scrape", "hold", cfg.hold.String())
	select {
	case <-ctx.Done():
	case <-time.After(cfg.hold):
	}
	return nil
}

// runStep produces at rate events/s for d, paced against the step's start.
// If Kafka can't keep up, Produce blocks once 100k records are buffered, and
// the step sends fewer than its target: the result logs say so.
func runStep(ctx context.Context, kafka *kgo.Client, gen *Generator, m metrics, c *stepCount, rate int, start time.Time, d time.Duration) {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		now := time.Now()
		elapsed := now.Sub(start)
		if elapsed >= d {
			elapsed = d
		}
		for due := Due(rate, elapsed); c.sent.Load() < due; {
			value, err := gen.Next(time.Now())
			if err != nil {
				c.failed.Add(1)
				m.produceErrors.Inc()
				c.sent.Add(1)
				continue
			}
			c.sent.Add(1)
			kafka.Produce(ctx, &kgo.Record{Value: value}, func(r *kgo.Record, err error) {
				if err != nil {
					c.failed.Add(1)
					m.produceErrors.Inc()
					return
				}
				c.acked.Add(1)
				m.produced.Inc()
				m.producedBytes.Add(float64(len(r.Value)))
			})
		}
		if elapsed >= d {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
