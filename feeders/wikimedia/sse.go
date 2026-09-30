package main

import (
	"bufio"
	"bytes"
	"io"
)

// maxLineBytes bounds a single SSE line. Recentchange events are a few KB.
const maxLineBytes = 8 << 20

// Event is one Server-Sent Event.
type Event struct {
	ID   string // last event ID seen on the stream, used to resume after a reconnect
	Type string
	Data []byte
}

// readEvents parses a text/event-stream from r and calls fn for each event,
// following the WHATWG SSE parsing rules. It returns nil when r reaches EOF.
func readEvents(r io.Reader, fn func(Event)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), maxLineBytes)

	var (
		lastID  string
		evType  string
		data    bytes.Buffer
		hasData bool
	)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			if hasData {
				fn(Event{ID: lastID, Type: evType, Data: bytes.Clone(data.Bytes())})
			}
			evType, hasData = "", false
			data.Reset()
			continue
		}
		if line[0] == ':' { // comment / keepalive
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			evType = string(value)
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.Write(value)
			hasData = true
		case "id":
			if !bytes.ContainsRune(value, 0) {
				lastID = string(value)
			}
		}
	}
	return sc.Err()
}
