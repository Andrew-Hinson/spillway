package main

import (
	"bytes"
	"encoding/json"
	"time"
)

// Stamp is the metadata the feeder adds to every event, under the "spillway"
// key: the same shape as the Wikimedia feeder's.
type Stamp struct {
	EventID    string    `json:"event_id"`
	ProducedAt time.Time `json:"produced_at"`
	Feeder     string    `json:"feeder"`
}

// stamp returns data, which must be a JSON object, with s added as a top-level
// "spillway" field. The upstream bytes are kept as-is rather than re-encoded.
func stamp(data []byte, s Stamp) ([]byte, error) {
	extra, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	body := bytes.TrimSpace(data)
	body = bytes.TrimSpace(body[:len(body)-1]) // drop the closing brace

	out := make([]byte, 0, len(body)+len(extra)+16)
	out = append(out, body...)
	if len(body) > 1 { // not an empty object
		out = append(out, ',')
	}
	out = append(out, `"spillway":`...)
	out = append(out, extra...)
	out = append(out, '}')
	return out, nil
}
