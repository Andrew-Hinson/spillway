package main

import (
	"strings"
	"testing"
)

func TestReadEvents(t *testing.T) {
	in := strings.Join([]string{
		": keepalive comment",
		"event: message",
		"id: 1",
		`data: {"a":1}`,
		"",
		"data: line one",
		"data:line two",
		"",
		"id: 2",
		"retry: 5000",
		"",              // no data: nothing dispatched, but the ID sticks
		`data: {"b":2}`, // inherits ID 2
		"",
		"data: unterminated", // dropped: no blank line before EOF
	}, "\r\n")

	var got []Event
	if err := readEvents(strings.NewReader(in), func(ev Event) { got = append(got, ev) }); err != nil {
		t.Fatal(err)
	}
	want := []Event{
		{ID: "1", Type: "message", Data: []byte(`{"a":1}`)},
		{ID: "1", Data: []byte("line one\nline two")},
		{ID: "2", Data: []byte(`{"b":2}`)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %q", len(got), len(want), got)
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Type != want[i].Type || string(got[i].Data) != string(want[i].Data) {
			t.Errorf("event %d = %+q, want %+q", i, got[i], want[i])
		}
	}
}
