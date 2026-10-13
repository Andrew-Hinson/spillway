// Command loadgen produces synthetic feeder events to Kafka at stepped rates,
// for Spillway's benchmark (bench/m4_bench.sh). Each event is shaped like a
// Wikimedia recentchange event and carries the same spillway stamp as the
// live feeders (event_id, produced_at, feeder), so the aggregator's latency
// histogram and dedupe treat it like real traffic.
//
// It runs each rate in -steps for -step-duration, logs each step's start and
// end, and exits once the last step's events are acknowledged (after -hold,
// so Prometheus scrapes the final counters).
//
// For the chaos experiments (bench/m4_chaos.sh), -run names the run: every
// event is tagged with it and a sequence number, and its event_id derives
// from both, so bench/eventcheck.py can count each event in the sinks.
// -stdout writes the events as log lines instead of producing to Kafka, so
// they travel as pod logs (agent → Kafka → aggregators); the generator's own
// logs then go to stderr.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
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
	run          string
	stdout       bool
	summary      string
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
	flag.StringVar(&cfg.run, "run", env("RUN", ""), "name the run: tag events with it and a sequence number, and derive their IDs (lowercase letters and digits)")
	flag.BoolVar(&cfg.stdout, "stdout", env("STDOUT", "false") == "true", "write events to stdout as log lines instead of producing to Kafka")
	// With -stdout, kubelet rotates the pod's log so fast that `kubectl logs`
	// can lose the result lines; a termination message survives.
	flag.StringVar(&cfg.summary, "summary", env("SUMMARY", ""), "also write the run's start, step and result records here at exit, e.g. /dev/termination-log")
	flag.Parse()

	logTo := os.Stdout
	if cfg.stdout {
		logTo = os.Stderr // stdout carries the events
	}
	log := slog.New(slog.NewJSONHandler(logTo, nil))
	if !runName.MatchString(cfg.run) {
		log.Error("bad -run: want up to 32 lowercase letters and digits", "run", cfg.run)
		os.Exit(2)
	}
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

	var out output = lineOutput{os.Stdout}
	if !cfg.stdout {
		// The live feeders' producer settings, so latency is comparable with
		// the M1 baseline. No key: batches spread over every partition, so
		// each aggregator gets an even share.
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
		out = kafkaOutput{kafka}
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
	defer func() { _ = srv.Close() }()

	// The records the summary repeats, as JSON lines.
	var summary bytes.Buffer
	sum := slog.New(slog.NewJSONHandler(&summary, nil))
	log.Info("load generator starting", "topic", cfg.topic, "stdout", cfg.stdout, "run", cfg.run,
		"steps", cfg.steps, "step_duration", cfg.stepDuration.String())
	sum.Info("load generator starting", "run", cfg.run)
	gen := NewGenerator(cfg.seed, feederName, cfg.topic).ForRun(cfg.run)
	counts := make([]stepCount, len(cfg.steps))
	for i, rate := range cfg.steps {
		if ctx.Err() != nil {
			break
		}
		c := &counts[i]
		m.target.Set(float64(rate))
		start := time.Now()
		log.Info("step started", "step", i, "rate", rate, "start", start.UTC())
		runStep(ctx, out, gen, m, c, rate, start, cfg.stepDuration)
		end := time.Now()
		m.target.Set(0)
		log.Info("step finished", "step", i, "rate", rate, "start", start.UTC(), "end", end.UTC(),
			"sent", c.sent.Load(), "achieved_per_sec", float64(c.sent.Load())/end.Sub(start).Seconds())
		sum.Info("step finished", "step", i, "rate", rate, "start", start.UTC(), "end", end.UTC(), "sent", c.sent.Load())
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := out.flush(flushCtx); err != nil {
		log.Warn("flush incomplete", "err", err)
	}
	for i, rate := range cfg.steps {
		c := &counts[i]
		log.Info("step result", "step", i, "rate", rate, "sent", c.sent.Load(), "acked", c.acked.Load(), "failed", c.failed.Load())
		sum.Info("step result", "step", i, "rate", rate, "sent", c.sent.Load(), "acked", c.acked.Load(), "failed", c.failed.Load())
	}
	if cfg.summary != "" {
		if err := os.WriteFile(cfg.summary, summary.Bytes(), 0o644); err != nil {
			log.Warn("writing the summary", "path", cfg.summary, "err", err)
		}
	}
	log.Info("load generator done; holding for the last scrape", "hold", cfg.hold.String())
	select {
	case <-ctx.Done():
	case <-time.After(cfg.hold):
	}
	return nil
}

// output delivers one encoded event and reports how it went to done.
type output interface {
	send(ctx context.Context, value []byte, done func(error))
	flush(ctx context.Context) error
}

type kafkaOutput struct{ c *kgo.Client }

func (k kafkaOutput) send(ctx context.Context, value []byte, done func(error)) {
	k.c.Produce(ctx, &kgo.Record{Value: value}, func(_ *kgo.Record, err error) { done(err) })
}

func (k kafkaOutput) flush(ctx context.Context) error { return k.c.Flush(ctx) }

// lineOutput writes each event as a line. A written line counts as
// acknowledged: from there it's the container runtime's and the agent's.
type lineOutput struct{ w io.Writer }

func (l lineOutput) send(_ context.Context, value []byte, done func(error)) {
	_, err := l.w.Write(append(value, '\n'))
	done(err)
}

func (lineOutput) flush(context.Context) error { return nil }

// runStep sends at rate events/s for d, paced against the step's start. If
// Kafka can't keep up, Produce blocks once 100k records are buffered, and
// the step sends fewer than its target: the result logs say so.
func runStep(ctx context.Context, out output, gen *Generator, m metrics, c *stepCount, rate int, start time.Time, d time.Duration) {
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
			size := len(value)
			out.send(ctx, value, func(err error) {
				if err != nil {
					c.failed.Add(1)
					m.produceErrors.Inc()
					return
				}
				c.acked.Add(1)
				m.produced.Inc()
				m.producedBytes.Add(float64(size))
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

// runName keeps run names safe in Loki queries and file names.
var runName = regexp.MustCompile(`^[a-z0-9]{0,32}$`)

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
