package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestInspect(t *testing.T) {
	u, err := inspect([]byte(` {"meta":{"id":"abc","domain":"en.wikipedia.org"},"title":"X"} `))
	if err != nil || u.Meta.ID != "abc" || u.Meta.Domain != "en.wikipedia.org" {
		t.Fatalf("inspect = %+v, %v", u, err)
	}
	for _, bad := range []string{``, `null`, `[1]`, `"s"`, `{"meta":`, `{} trailing`} {
		if _, err := inspect([]byte(bad)); err == nil {
			t.Errorf("inspect(%q): want error", bad)
		}
	}
}

func TestStamp(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 123456789, time.UTC)
	s := Stamp{EventID: "id-1", ProducedAt: at, Feeder: "wikimedia"}

	for _, in := range []string{`{"title":"X","n":1}`, "{\n  \"title\": \"X\",\n  \"n\": 1\n}\n", `{}`, ` { } `} {
		out, err := stamp([]byte(in), s)
		if err != nil {
			t.Fatalf("stamp(%q): %v", in, err)
		}
		var got struct {
			Title    string `json:"title"`
			Spillway Stamp  `json:"spillway"`
		}
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("stamp(%q) produced invalid JSON %q: %v", in, out, err)
		}
		if got.Spillway.EventID != "id-1" || !got.Spillway.ProducedAt.Equal(at) || got.Spillway.Feeder != "wikimedia" {
			t.Errorf("stamp(%q) = %s", in, out)
		}
	}
}
