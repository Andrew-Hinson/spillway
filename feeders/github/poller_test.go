package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// fakeAPI serves pages of events newest first, as GitHub does, with rate-limit
// headers and an ETag on page 1.
type fakeAPI struct {
	mu        sync.Mutex
	events    []int // IDs, newest first
	remaining int
	reset     time.Time
	status    int // if set, returned instead of events
	retry     string
	requests  []string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.URL.RawQuery+" inm="+r.Header.Get("If-None-Match"))
	if r.Header.Get("User-Agent") == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	f.remaining--
	w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(f.remaining))
	w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(f.reset.Unix(), 10))
	w.Header().Set("X-Poll-Interval", "60")
	if f.status != 0 {
		if f.retry != "" {
			w.Header().Set("Retry-After", f.retry)
		}
		w.WriteHeader(f.status)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	etag := fmt.Sprintf(`W/"%d"`, f.events[0])
	if page == 1 {
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
	}
	from, to := (page-1)*100, page*100
	if from > len(f.events) {
		from = len(f.events)
	}
	if to > len(f.events) {
		to = len(f.events)
	}
	var parts []string
	for _, id := range f.events[from:to] {
		parts = append(parts, fmt.Sprintf(`{"id":"%d","type":"PushEvent","payload":{"n":%d}}`, id, id))
	}
	_, _ = io.WriteString(w, "["+strings.Join(parts, ",")+"]")
}

func newest(from, n int) []int {
	ids := make([]int, n)
	for i := range ids {
		ids[i] = from - i
	}
	return ids
}

func testPoller(t *testing.T, api *fakeAPI, now time.Time) *Poller {
	t.Helper()
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	return &Poller{
		URL: srv.URL, UserAgent: "test", Pages: 3, Client: srv.Client(), Seen: NewSeen(1000),
		Metrics: newMetrics(prometheus.NewRegistry(), "github"),
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }, Sleep: SleepCtx,
	}
}

func ids(es []event) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.id
	}
	return out
}

func TestCycleReadsPagesOldestFirstAndSkipsSeen(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	api := &fakeAPI{events: newest(1250, 250), remaining: 60, reset: now.Add(time.Hour)}
	p := testPoller(t, api, now)

	got, _, err := p.cycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 250 || got[0].id != "1001" || got[249].id != "1250" {
		t.Fatalf("first cycle: %d events, %s…%s; want 250, oldest (1001) first", len(got), got[0].id, got[len(got)-1].id)
	}
	if len(api.requests) != 3 {
		t.Errorf("first cycle made %d requests, want 3 pages", len(api.requests))
	}

	// Five new events arrive: page 1 has them and old ones, so paging stops.
	api.events = append(newest(1255, 5), api.events...)
	api.requests = nil
	got, _, err = p.cycle(context.Background())
	if err != nil || strings.Join(ids(got), ",") != "1251,1252,1253,1254,1255" {
		t.Fatalf("second cycle = %v, %v; want only the 5 new events, oldest first", ids(got), err)
	}
	if len(api.requests) != 1 || !strings.Contains(api.requests[0], `inm=W/"1250"`) {
		t.Errorf("second cycle requests = %v; want page 1 only, sent with the ETag", api.requests)
	}

	// Nothing new: the API answers 304, and nothing is produced.
	api.requests = nil
	got, _, err = p.cycle(context.Background())
	if err != nil || len(got) != 0 || len(api.requests) != 1 {
		t.Fatalf("unchanged feed: %v, %v, %d requests; want nothing after one conditional request", ids(got), err, len(api.requests))
	}
}

func TestWaitSpreadsTheRateLimit(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	for _, c := range []struct {
		name      string
		remaining int
		reset     time.Duration
		requests  int
		want      time.Duration
	}{
		{"plenty left: the poll interval", 4000, time.Hour, 3, 60 * time.Second},
		{"60/hour, 3 requests a cycle: 19 cycles of the 57 left after the reserve", 60, time.Hour, 3, time.Hour / 19},
		{"one request a cycle needs less spacing", 60, time.Hour, 1, time.Hour / 57},
		{"below a full cycle: wait for the reset", 5, 10 * time.Minute, 1, 10*time.Minute + time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := testPoller(t, &fakeAPI{}, now)
			p.limit = rateLimit{remaining: c.remaining, reset: now.Add(c.reset), pollInterval: 60 * time.Second, known: true}
			if got := p.wait(c.requests, nil); got != c.want {
				t.Errorf("wait = %v, want %v", got, c.want)
			}
		})
	}
}

func TestRateLimitedWaitsForRetryAfter(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	api := &fakeAPI{events: newest(10, 10), remaining: 30, reset: now.Add(time.Hour), status: http.StatusForbidden, retry: "120"}
	p := testPoller(t, api, now)
	got, wait, err := p.cycle(context.Background())
	if err == nil || len(got) != 0 {
		t.Fatalf("cycle = %v, %v; want a rate-limit error", ids(got), err)
	}
	if wait != 121*time.Second {
		t.Errorf("wait = %v, want Retry-After plus a second", wait)
	}
}

func TestSeenForgetsTheOldest(t *testing.T) {
	s := NewSeen(2)
	for _, c := range []struct {
		id  string
		new bool
	}{{"a", true}, {"b", true}, {"a", false}, {"c", true}, {"a", true}, {"c", false}} {
		if got := s.Add(c.id); got != c.new {
			t.Errorf("Add(%q) = %v, want %v", c.id, got, c.new)
		}
	}
}

// A cycle's events are spread over the wait before the next poll, not
// produced as one burst.
func TestRunPacesEventsOverTheWait(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	api := &fakeAPI{events: newest(1100, 100), remaining: 4000, reset: now.Add(time.Hour)}
	p := testPoller(t, api, now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := now
	p.Now = func() time.Time { return clock }
	var sleeps []time.Duration
	p.Sleep = func(_ context.Context, d time.Duration) bool {
		sleeps = append(sleeps, d)
		clock = clock.Add(d)
		if len(sleeps) > 100 { // the 100 events, then the rest of the wait: stop
			cancel()
			return false
		}
		return true
	}
	var produced []time.Time
	p.Run(ctx, func(string, []byte) { produced = append(produced, clock) })

	if len(produced) != 100 {
		t.Fatalf("produced %d events, want 100", len(produced))
	}
	// 100 events over 90% of a 60 s wait: one every 0.54 s.
	gap := produced[1].Sub(produced[0])
	if gap != 540*time.Millisecond || produced[99].Sub(produced[0]) != 99*540*time.Millisecond {
		t.Errorf("events %v apart, last at +%v; want 540ms apart, last at +53.46s", gap, produced[99].Sub(produced[0]))
	}
	if last := sleeps[100]; last != 6*time.Second {
		t.Errorf("after the events, slept %v; want the 6 s left of the 60 s wait", last)
	}
}
