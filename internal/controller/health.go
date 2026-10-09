package controller

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// PodHealth is what the canary check compares between an aggregator pod
// running new config and one running stable config.
type PodHealth struct {
	// Errors counts Vector component errors, failed HTTP requests (including
	// ones a sink retries, which component errors don't count), and events
	// discarded unintentionally. Intentional drops (sampling, budgets) don't
	// count.
	Errors float64 `json:"errors"`
	// Restarts is the pod's total container restarts.
	Restarts int32 `json:"restarts"`
	// Sinks holds each sink's received and sent event counters, to spot a
	// sink that takes events in but delivers none.
	Sinks map[string]SinkCounts `json:"sinks,omitempty"`
}

// SinkCounts are a sink's event counters.
type SinkCounts struct {
	Received float64 `json:"received"`
	Sent     float64 `json:"sent"`
}

// HealthChecker reads one aggregator pod's health.
type HealthChecker interface {
	Check(ctx context.Context, namespace, pod string) (PodHealth, error)
}

// MetricsHealth reads restarts from the pod's status and errors from Vector's
// Prometheus exporter on the pod.
type MetricsHealth struct {
	Client client.Reader
	Port   int
	HTTP   *http.Client
}

// Check implements HealthChecker.
func (m MetricsHealth) Check(ctx context.Context, namespace, name string) (PodHealth, error) {
	var pod corev1.Pod
	if err := m.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &pod); err != nil {
		return PodHealth{}, err
	}
	var h PodHealth
	for _, c := range pod.Status.ContainerStatuses {
		h.Restarts += c.RestartCount
	}
	if pod.Status.PodIP == "" {
		return h, fmt.Errorf("pod %s/%s has no IP yet", namespace, name)
	}
	httpClient := m.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	url := "http://" + net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(m.Port)) + "/metrics"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return h, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return h, fmt.Errorf("scraping %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return h, fmt.Errorf("scraping %s: %s", name, resp.Status)
	}
	h.Errors, h.Sinks, err = parseMetrics(resp.Body)
	return h, err
}

// parseMetrics reads the error total and per-sink event counters from
// Vector's metrics output.
func parseMetrics(r io.Reader) (errors float64, sinks map[string]SinkCounts, err error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing Vector metrics: %w", err)
	}
	label := func(m *dto.Metric, name string) string {
		for _, l := range m.GetLabel() {
			if l.GetName() == name {
				return l.GetValue()
			}
		}
		return ""
	}
	each := func(name string, f func(m *dto.Metric, v float64)) {
		for _, m := range families[name].GetMetric() {
			f(m, m.GetCounter().GetValue())
		}
	}
	each("vector_component_errors_total", func(_ *dto.Metric, v float64) { errors += v })
	each("vector_http_client_errors_total", func(_ *dto.Metric, v float64) { errors += v })
	each("vector_component_discarded_events_total", func(m *dto.Metric, v float64) {
		if label(m, "intentional") != "true" {
			errors += v
		}
	})
	sinks = map[string]SinkCounts{}
	each("vector_component_received_events_total", func(m *dto.Metric, v float64) {
		if label(m, "component_kind") == "sink" {
			c := sinks[label(m, "component_id")]
			c.Received += v
			sinks[label(m, "component_id")] = c
		}
	})
	each("vector_component_sent_events_total", func(m *dto.Metric, v float64) {
		if label(m, "component_kind") == "sink" {
			c := sinks[label(m, "component_id")]
			c.Sent += v
			sinks[label(m, "component_id")] = c
		}
	})
	return errors, sinks, nil
}

// stalledSinks returns the sinks that received events between before and
// after but sent none, and that aren't stalled on the stable pod too (a
// shared outage isn't the canary's fault). A sink only the canary has counts.
func stalledSinks(before, after, stableBefore, stableAfter PodHealth) []string {
	stalled := func(b, a PodHealth, id string) bool {
		bc, ac := b.Sinks[id], a.Sinks[id]
		return ac.Received > bc.Received && ac.Sent == bc.Sent
	}
	var out []string
	for id := range after.Sinks {
		if !stalled(before, after, id) {
			continue
		}
		if _, onStable := stableAfter.Sinks[id]; onStable && stalled(stableBefore, stableAfter, id) {
			continue
		}
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
