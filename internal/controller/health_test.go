package controller

import (
	"strings"
	"testing"
)

// A trimmed scrape of a Vector aggregator's /metrics.
const vectorMetrics = `# HELP vector_component_errors_total component_errors_total
# TYPE vector_component_errors_total counter
vector_component_errors_total{component_id="cold_s3",component_kind="sink",component_type="aws_s3",error_type="request_failed",stage="sending"} 7 1791394987196
vector_component_errors_total{component_id="hot_loki",component_kind="sink",component_type="loki",error_type="request_failed",stage="sending"} 2 1791394987196
# HELP vector_component_discarded_events_total component_discarded_events_total
# TYPE vector_component_discarded_events_total counter
vector_component_discarded_events_total{component_id="wiki_sample",component_kind="transform",component_type="remap",intentional="true"} 1416 1791394987196
vector_component_discarded_events_total{component_id="wiki_budget",component_kind="transform",component_type="throttle",intentional="true"} 1163 1791394987196
vector_component_discarded_events_total{component_id="hot_loki",component_kind="sink",component_type="loki",intentional="false"} 3 1791394987196
# HELP vector_http_client_errors_total http_client_errors_total
# TYPE vector_http_client_errors_total counter
vector_http_client_errors_total{component_id="cold_s3",component_kind="sink",component_type="aws_s3",error_kind="io error"} 9 1791394987196
# HELP vector_component_received_events_total component_received_events_total
# TYPE vector_component_received_events_total counter
vector_component_received_events_total{component_id="hot_loki",component_kind="sink",component_type="loki"} 90010 1791394987196
vector_component_received_events_total{component_id="cold_s3",component_kind="sink",component_type="aws_s3"} 3733 1791394987196
vector_component_received_events_total{component_id="wiki_sample",component_kind="transform",component_type="remap"} 5000 1791394987196
# HELP vector_component_sent_events_total component_sent_events_total
# TYPE vector_component_sent_events_total counter
vector_component_sent_events_total{component_id="hot_loki",component_kind="sink",component_type="loki"} 90000 1791394987196
`

func TestParseMetrics(t *testing.T) {
	errors, sinks, err := parseMetrics(strings.NewReader(vectorMetrics))
	if err != nil {
		t.Fatal(err)
	}
	// 7 + 2 component errors, 9 retried HTTP failures, 3 unintentional
	// discards; sampling and budget drops don't count.
	if errors != 21 {
		t.Errorf("errors = %v, want 21", errors)
	}
	want := map[string]SinkCounts{"hot_loki": {Received: 90010, Sent: 90000}, "cold_s3": {Received: 3733}}
	if len(sinks) != len(want) || sinks["hot_loki"] != want["hot_loki"] || sinks["cold_s3"] != want["cold_s3"] {
		t.Errorf("sinks = %v, want %v (transforms excluded)", sinks, want)
	}
}

func TestParseMetricsWithNoErrorsIsZero(t *testing.T) {
	errors, _, err := parseMetrics(strings.NewReader("# TYPE vector_started_total counter\nvector_started_total 1\n"))
	if err != nil || errors != 0 {
		t.Errorf("errors = %v, %v; want 0, nil", errors, err)
	}
}

func TestStalledSinks(t *testing.T) {
	h := func(sinks map[string]SinkCounts) PodHealth { return PodHealth{Sinks: sinks} }
	before := h(map[string]SinkCounts{"hot_loki": {10, 10}, "cold_s3": {0, 0}, "loki_pods": {5, 5}, "quiet": {3, 3}})
	after := h(map[string]SinkCounts{"hot_loki": {50, 49}, "cold_s3": {40, 0}, "loki_pods": {9, 5}, "quiet": {3, 3}})
	// On the stable pod, loki_pods is stalled too (Loki is down for everyone);
	// cold_s3 exists only on the canary.
	stableBefore := h(map[string]SinkCounts{"hot_loki": {10, 10}, "loki_pods": {5, 5}})
	stableAfter := h(map[string]SinkCounts{"hot_loki": {60, 60}, "loki_pods": {8, 5}})
	got := stalledSinks(before, after, stableBefore, stableAfter)
	if len(got) != 1 || got[0] != "cold_s3" {
		t.Errorf("stalled = %v, want [cold_s3]: delivering, idle and shared-outage sinks don't count", got)
	}
}
