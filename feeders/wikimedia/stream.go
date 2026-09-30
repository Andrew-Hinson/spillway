package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"
)

var (
	errIdle        = errors.New("no data received within idle timeout")
	errStreamEnded = errors.New("stream closed by server")
)

// Stream is an SSE client that reconnects whenever the connection drops,
// resuming from the last event ID it delivered.
type Stream struct {
	URL         string
	UserAgent   string
	Client      *http.Client
	IdleTimeout time.Duration // reconnect if the server sends nothing for this long
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
	Metrics     *metrics
	Log         *slog.Logger

	lastEventID string
}

// Run delivers events to fn until ctx is cancelled.
func (s *Stream) Run(ctx context.Context, fn func(Event)) {
	backoff := s.MinBackoff
	for {
		delivered, err := s.connect(ctx, fn)
		if ctx.Err() != nil {
			return
		}
		if delivered > 0 {
			backoff = s.MinBackoff
		}
		// Full jitter over the upper half, so restarts don't reconnect in lockstep.
		wait := backoff/2 + rand.N(backoff/2+1)
		s.Metrics.reconnects.Inc()
		s.Log.Warn("stream disconnected; reconnecting",
			"err", err, "delivered", delivered, "retry_in", wait.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff = min(backoff*2, s.MaxBackoff)
	}
}

// connect runs one connection and returns how many events it delivered and why it ended.
func (s *Stream) connect(ctx context.Context, fn func(Event)) (int, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", s.UserAgent)
	if s.lastEventID != "" {
		req.Header.Set("Last-Event-ID", s.lastEventID)
	}

	// Started before the request so it also bounds a server that accepts the
	// connection but never sends response headers.
	idle := time.AfterFunc(s.IdleTimeout, func() { cancel(errIdle) })
	defer idle.Stop()

	resp, err := s.Client.Do(req)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status %s", resp.Status)
	}

	s.Log.Info("stream connected", "url", s.URL, "resumed", s.lastEventID != "")
	s.Metrics.connected.Set(1)
	defer s.Metrics.connected.Set(0)

	body := &idleReader{r: resp.Body, timer: idle, timeout: s.IdleTimeout}

	delivered := 0
	err = readEvents(body, func(ev Event) {
		if ev.Type != "" && ev.Type != "message" {
			return
		}
		fn(ev)
		delivered++
		if ev.ID != "" {
			s.lastEventID = ev.ID
		}
	})
	if cause := context.Cause(ctx); cause != nil {
		return delivered, cause
	}
	if err == nil {
		err = errStreamEnded
	}
	return delivered, err
}

// idleReader pushes back an idle timer every time data arrives.
type idleReader struct {
	r       io.Reader
	timer   *time.Timer
	timeout time.Duration
}

func (ir *idleReader) Read(p []byte) (int, error) {
	n, err := ir.r.Read(p)
	if n > 0 {
		ir.timer.Reset(ir.timeout)
	}
	return n, err
}
