package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Patterns are the PII patterns fixtures carry, one per fixture, in turn.
var Patterns = []string{"ssn", "email", "phone", "memberId"}

// Value returns the fixture value for id and pattern. Values are
// deterministic and drawn from ranges that are fictional by design, so a
// value found in a sink is a fixture, never real data:
//
//	ssn       123-45-NNNN (never 0000)
//	email     fixture-<id>@fixtures.spillway.test
//	phone     (212) 555-01NN, the NANP range reserved for fiction
//	memberId  MBR-9NNNNNNN
func Value(id int64, pattern string) string {
	switch pattern {
	case "ssn":
		return fmt.Sprintf("123-45-%04d", id%9999+1)
	case "email":
		return fmt.Sprintf("fixture-%d@fixtures.spillway.test", id)
	case "phone":
		return fmt.Sprintf("(212) 555-01%02d", id%100)
	case "memberId":
		return fmt.Sprintf("MBR-9%07d", id%10_000_000)
	}
	panic("unknown pattern " + pattern)
}

// Tag marks an event as a fixture. It lives under .spillway, which redaction
// never touches, so fixtures can be counted wherever they land.
type Tag struct {
	ID      int64  `json:"id"`
	Pattern string `json:"pattern"`
}

// stamp is the metadata every Spillway feeder adds.
type stamp struct {
	EventID    string    `json:"event_id"`
	ProducedAt time.Time `json:"produced_at"`
	Feeder     string    `json:"feeder"`
	Fixture    Tag       `json:"fixture"`
}

// Fixture is one injected record and the value it carries.
type Fixture struct {
	Tag
	Value string
}

// Next returns the fixture with the given id; patterns rotate with the id.
func Next(id int64) Fixture {
	p := Patterns[id%int64(len(Patterns))]
	return Fixture{Tag: Tag{ID: id, Pattern: p}, Value: Value(id, p)}
}

// KafkaEvent renders f as a recentchange-shaped event, so pipelines reading
// the Wikimedia topic treat it like any other edit. The value appears in free
// text and in a nested field.
func KafkaEvent(f Fixture, now time.Time) ([]byte, error) {
	return json.Marshal(map[string]any{
		"type":        "edit",
		"wiki":        "fixtures",
		"server_name": "fixtures.invalid", // .invalid is reserved; the email domain stays unique to PII values
		"title":       fmt.Sprintf("Fixture %d", f.ID),
		"user":        "fixture-bot",
		"comment":     fmt.Sprintf("please contact %s about this edit", f.Value),
		"detail":      map[string]any{"contact": f.Value},
		"spillway": stamp{
			EventID: uuid.NewString(), ProducedAt: now.UTC(), Feeder: "fixtures", Fixture: f.Tag,
		},
	})
}

// LogLine renders f as an application log line, for pipelines that read the
// injector's pod logs. It's a warning, so level-based sampling keeps it.
func LogLine(f Fixture, now time.Time) ([]byte, error) {
	return json.Marshal(map[string]any{
		"time":     now.UTC(),
		"level":    "warn",
		"msg":      fmt.Sprintf("account update failed for %s", f.Value),
		"account":  map[string]any{"contact": f.Value},
		"spillway": stamp{EventID: uuid.NewString(), ProducedAt: now.UTC(), Feeder: "fixtures", Fixture: f.Tag},
	})
}
