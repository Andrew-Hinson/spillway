package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func testStream(url string) *Stream {
	return &Stream{
		URL:         url,
		UserAgent:   "spillway-test",
		Client:      &http.Client{},
		IdleTimeout: 5 * time.Second,
		MinBackoff:  time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
		Metrics:     newMetrics(prometheus.NewRegistry(), "test"),
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// TestStreamReconnectResumes drops the connection after each event and checks
// the client reconnects, sending the ID of the last event it received.
func TestStreamReconnectResumes(t *testing.T) {
	var (
		mu      sync.Mutex
		resumes []string
		conns   int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conns++
		n := conns
		resumes = append(resumes, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		if r.Header.Get("User-Agent") != "spillway-test" {
			t.Errorf("User-Agent = %q", r.Header.Get("User-Agent"))
		}
		if n == 2 { // one refused connection in the middle
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: message\nid: id-%d\ndata: {\"n\":%d}\n\n", n, n)
	}))
	defer srv.Close()

	s := testStream(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	var got []string
	s.Run(ctx, func(ev Event) {
		got = append(got, string(ev.Data))
		if len(got) == 3 {
			cancel()
		}
	})

	want := []string{`{"n":1}`, `{"n":3}`, `{"n":4}`}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("events = %q, want %q", got, want)
	}
	// 1st: fresh; 2nd (refused) and 3rd resume after id-1; 4th resumes after id-3.
	wantResumes := []string{"", "id-1", "id-1", "id-3"}
	if fmt.Sprint(resumes) != fmt.Sprint(wantResumes) {
		t.Errorf("Last-Event-ID headers = %q, want %q", resumes, wantResumes)
	}
	if n := testutil.ToFloat64(s.Metrics.reconnects); n != 3 {
		t.Errorf("reconnects = %v, want 3", n)
	}
}

// TestStreamIdleTimeout checks a connection that goes silent is dropped and re-established.
func TestStreamIdleTimeout(t *testing.T) {
	var mu sync.Mutex
	conns := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conns++
		n := conns
		mu.Unlock()
		fmt.Fprintf(w, "data: {\"n\":%d}\n\n", n)
		w.(http.Flusher).Flush()
		<-r.Context().Done() // then hang
	}))
	defer srv.Close()

	s := testStream(srv.URL)
	s.IdleTimeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := 0
	s.Run(ctx, func(Event) {
		if events++; events == 2 {
			cancel()
		}
	})
	if events != 2 {
		t.Fatalf("got %d events before timeout, want 2 (idle connection not replaced)", events)
	}
}

// TestStreamHeaderTimeout checks a server that never sends response headers
// doesn't hang the client.
func TestStreamHeaderTimeout(t *testing.T) {
	var mu sync.Mutex
	conns := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conns++
		n := conns
		mu.Unlock()
		if n == 1 {
			<-r.Context().Done() // never respond
			return
		}
		fmt.Fprint(w, "data: {}\n\n")
	}))
	defer srv.Close()

	s := testStream(srv.URL)
	s.IdleTimeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got := false
	s.Run(ctx, func(Event) { got = true; cancel() })
	if !got {
		t.Fatal("no event: client stuck waiting for response headers")
	}
}
