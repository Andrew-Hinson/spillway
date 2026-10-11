package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// Poller reads GitHub's public events API. The API has no stream: it serves
// the most recent events, newest first, up to Pages pages of 100, and each
// response is cached for minutes. So each cycle fetches pages until one has
// nothing new, hands over the unseen events oldest first, and waits long
// enough to stay inside the rate limit (60 requests an hour without a token).
type Poller struct {
	URL       string // e.g. https://api.github.com/events
	Token     string // optional; raises the rate limit to 5,000 an hour
	UserAgent string // GitHub rejects requests without one
	Pages     int
	Client    *http.Client
	Seen      *Seen
	Metrics   *metrics
	Log       *slog.Logger
	Now       func() time.Time
	// Sleep waits d, or returns false if ctx is done first.
	Sleep func(ctx context.Context, d time.Duration) bool

	etag  string // of page 1, so an unchanged feed costs a 304
	limit rateLimit
}

// rateLimit is what the last response said about the request budget.
type rateLimit struct {
	remaining    int
	reset        time.Time
	pollInterval time.Duration
	known        bool
}

const (
	defaultPollInterval = 60 * time.Second
	// Requests kept back, so a restart or a manual curl doesn't hit the limit.
	rateReserve = 3
)

// errRateLimited is a 403 or 429: wait for the window to reset.
var errRateLimited = errors.New("rate limited")

// pacedShare is the share of the wait between polls that a cycle's events
// are spread over, so the last is produced before the next poll.
const pacedShare = 0.9

// Run polls until ctx is done, passing each new event to fn. A poll returns
// up to 300 events at once, minutes apart; produced as they come, they'd hit
// a team's budget as bursts, and a throttle keeps the start of each burst and
// drops the rest, which in this feed means whole event types. So each
// cycle's events are paced evenly over the wait before the next poll. An
// event waits at most one poll interval longer; the API caches for minutes
// anyway. On shutdown, events not yet produced are dropped; after a restart
// the API serves them again.
func (p *Poller) Run(ctx context.Context, fn func(id string, event []byte)) {
	for {
		events, wait, err := p.cycle(ctx)
		if err != nil && ctx.Err() == nil {
			p.Metrics.reconnects.Inc()
			p.Metrics.connected.Set(0)
			p.Log.Warn("poll failed", "err", err, "retry_in", wait)
		} else if err == nil {
			p.Metrics.connected.Set(1)
		}
		start := p.Now()
		for i, e := range events {
			fn(e.id, e.raw)
			due := start.Add(time.Duration(float64(wait) * pacedShare * float64(i+1) / float64(len(events))))
			if !p.Sleep(ctx, due.Sub(p.Now())) {
				return
			}
		}
		if !p.Sleep(ctx, start.Add(wait).Sub(p.Now())) {
			return
		}
	}
}

// SleepCtx waits d, or returns false if ctx is done first.
func SleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type event struct {
	id  string
	raw []byte
}

// cycle fetches pages until one holds an event already seen, and returns the new
// events oldest first and how long to wait before the next cycle.
func (p *Poller) cycle(ctx context.Context) ([]event, time.Duration, error) {
	var fresh []event
	requests := 0
	for page := 1; page <= p.Pages; page++ {
		requests++
		events, notModified, err := p.fetch(ctx, page)
		if err != nil {
			return reverse(fresh), p.wait(requests, err), err
		}
		if notModified {
			break
		}
		added := 0
		for _, e := range events {
			p.Metrics.received.Inc()
			if !p.Seen.Add(e.id) {
				p.Metrics.skipped.WithLabelValues("seen").Inc()
				continue
			}
			fresh = append(fresh, e)
			added++
		}
		if added < len(events) || len(events) < 100 {
			break // reached events already seen, so the rest is older; or there is no more
		}
	}
	return reverse(fresh), p.wait(requests, nil), nil
}

// fetch gets one page. Events are returned in the API's order, newest first.
func (p *Poller) fetch(ctx context.Context, page int) ([]event, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s?per_page=100&page=%d", p.URL, page), nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", p.UserAgent)
	if p.Token != "" {
		req.Header.Set("Authorization", "Bearer "+p.Token)
	}
	if page == 1 && p.etag != "" {
		req.Header.Set("If-None-Match", p.etag)
	}
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	p.Metrics.requests.WithLabelValues(strconv.Itoa(resp.StatusCode)).Inc()
	p.readLimit(resp.Header)

	switch {
	case resp.StatusCode == http.StatusNotModified:
		return nil, true, nil
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests:
		if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			p.limit.remaining, p.limit.reset, p.limit.known = 0, p.Now().Add(time.Duration(s)*time.Second), true
		}
		return nil, false, fmt.Errorf("%w: %s", errRateLimited, resp.Status)
	case resp.StatusCode != http.StatusOK:
		return nil, false, fmt.Errorf("unexpected status %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, false, err
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(body, &raws); err != nil {
		return nil, false, fmt.Errorf("decoding events: %w", err)
	}
	events := make([]event, 0, len(raws))
	for _, r := range raws {
		var head struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(r, &head) != nil || head.ID == "" {
			p.Metrics.skipped.WithLabelValues("invalid").Inc()
			continue
		}
		events = append(events, event{id: head.ID, raw: r})
	}
	if page == 1 {
		p.etag = resp.Header.Get("ETag")
	}
	return events, false, nil
}

func (p *Poller) readLimit(h http.Header) {
	if s, err := strconv.Atoi(h.Get("X-Poll-Interval")); err == nil && s > 0 {
		p.limit.pollInterval = time.Duration(s) * time.Second
	}
	rem, err1 := strconv.Atoi(h.Get("X-RateLimit-Remaining"))
	reset, err2 := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64)
	if err1 == nil && err2 == nil {
		p.limit.remaining, p.limit.reset, p.limit.known = rem, time.Unix(reset, 0), true
		p.Metrics.rateRemaining.Set(float64(rem))
	}
}

// wait is how long to sleep after a cycle that made requests requests: at
// least the API's poll interval, and long enough that the same number of
// requests per cycle lasts until the rate-limit window resets.
func (p *Poller) wait(requests int, err error) time.Duration {
	w := p.limit.pollInterval
	if w == 0 {
		w = defaultPollInterval
	}
	if !p.limit.known {
		return w
	}
	untilReset := p.limit.reset.Sub(p.Now())
	if untilReset < 0 {
		untilReset = 0
	}
	if usable := p.limit.remaining - rateReserve; usable < p.Pages || errors.Is(err, errRateLimited) {
		// Not enough left for a full cycle: wait out the window.
		return max(w, untilReset+time.Second)
	} else if spread := untilReset / time.Duration(usable/requests); spread > w {
		return spread
	}
	return w
}

func reverse(es []event) []event {
	for i, j := 0, len(es)-1; i < j; i, j = i+1, j-1 {
		es[i], es[j] = es[j], es[i]
	}
	return es
}

// Seen remembers the most recent IDs, so an event served again by a later
// poll isn't produced twice. A restart forgets them; the pipeline's dedupe
// on spillway.event_id catches those repeats.
type Seen struct {
	ids  map[string]struct{}
	ring []string
	next int
}

// NewSeen remembers up to n IDs.
func NewSeen(n int) *Seen {
	return &Seen{ids: make(map[string]struct{}, n), ring: make([]string, n)}
}

// Add records id and reports whether it was new.
func (s *Seen) Add(id string) bool {
	if _, ok := s.ids[id]; ok {
		return false
	}
	if old := s.ring[s.next]; old != "" {
		delete(s.ids, old)
	}
	s.ring[s.next] = id
	s.ids[id] = struct{}{}
	s.next = (s.next + 1) % len(s.ring)
	return true
}
