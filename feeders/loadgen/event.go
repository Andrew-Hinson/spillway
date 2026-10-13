package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Stamp is the metadata every Spillway feeder adds, under "spillway".
type Stamp struct {
	EventID    string    `json:"event_id"`
	ProducedAt time.Time `json:"produced_at"`
	Feeder     string    `json:"feeder"`
}

// Event is a synthetic Wikimedia recentchange event: the same fields, nesting
// and free text as the live feed, so the aggregator parses, redacts and
// encodes about the same work per event. It holds no PII.
type Event struct {
	Schema           string   `json:"$schema"`
	Meta             Meta     `json:"meta"`
	ID               int64    `json:"id"`
	Type             string   `json:"type"`
	Namespace        int      `json:"namespace"`
	Title            string   `json:"title"`
	TitleURL         string   `json:"title_url"`
	Comment          string   `json:"comment"`
	Timestamp        int64    `json:"timestamp"`
	User             string   `json:"user"`
	Bot              bool     `json:"bot"`
	NotifyURL        string   `json:"notify_url"`
	Minor            bool     `json:"minor"`
	Patrolled        bool     `json:"patrolled"`
	Length           OldNew   `json:"length"`
	Revision         OldNew   `json:"revision"`
	ServerURL        string   `json:"server_url"`
	ServerName       string   `json:"server_name"`
	ServerScriptPath string   `json:"server_script_path"`
	Wiki             string   `json:"wiki"`
	ParsedComment    string   `json:"parsedcomment"`
	Spillway         Stamp    `json:"spillway"`
	Tags             []string `json:"tags,omitempty"`
	// Set only for a named run (-run), so the chaos checks can count every
	// event by ID: see RunTag.
	Level   string  `json:"level,omitempty"`
	Loadgen *RunTag `json:"loadgen,omitempty"`
}

// RunTag identifies an event within a named run. Its event_id is derived
// from the run and seq (EventID), so a checker can tell which events are
// missing or duplicated, not only how many.
type RunTag struct {
	Run string `json:"run"`
	Seq int64  `json:"seq"`
}

// idSpace is the UUID namespace for run event IDs (bench/eventcheck.py
// derives the same IDs).
var idSpace = uuid.MustParse("6f1c2a8e-3b5d-4c7a-9e21-5d8b0f4a7c63")

// EventID is the event_id of event seq in run: a name-based (SHA-1) UUID of
// "run/seq".
func EventID(run string, seq int64) string {
	return uuid.NewSHA1(idSpace, fmt.Appendf(nil, "%s/%d", run, seq)).String()
}

// LevelOf gives every fourth event of a run the level debug, so a team that
// drops debug drops a known set.
func LevelOf(seq int64) string {
	if seq%4 == 0 {
		return "debug"
	}
	return "info"
}

type Meta struct {
	URI       string    `json:"uri"`
	RequestID string    `json:"request_id"`
	ID        string    `json:"id"`
	DT        time.Time `json:"dt"`
	Domain    string    `json:"domain"`
	Stream    string    `json:"stream"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
}

type OldNew struct {
	Old int64 `json:"old"`
	New int64 `json:"new"`
}

// The live feed's mix: mostly edits and categorizations, some log events and
// new pages.
var types = []struct {
	name   string
	weight int
}{{"edit", 55}, {"categorize", 30}, {"log", 10}, {"new", 5}}

var wikis = []string{"en.wikipedia.org", "commons.wikimedia.org", "www.wikidata.org", "de.wikipedia.org", "fr.wikipedia.org", "ja.wikipedia.org", "es.wikipedia.org", "ru.wikipedia.org"}

var words = strings.Fields(`the of and in to a is was for on as with by he at from his an were are which
this also be has or had first one their its new after who they two her she been other when time during
there into school more may years over only year most would world city some where between later three state
such then national used made known under many university united while part season team these american than
film second born south became states war through being including both before north high however people
family early history album area them series against until since district county name work life group music
following number company several four called played released career league game government house each based
day same won use station club international town located population general college river university article
reverted edits revision category added removed template infobox references external links citation needed
cleanup typo fixed spelling grammar wikidata item description label alias statement qualifier reference`)

// Generator builds events. It isn't safe for concurrent use.
type Generator struct {
	rng    *rand.Rand
	next   int64
	feeder string
	topic  string
	run    string // tags events and derives their IDs, if set
}

func NewGenerator(seed uint64, feeder, topic string) *Generator {
	return &Generator{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), next: 1, feeder: feeder, topic: topic}
}

// ForRun tags each event with run and its sequence number (from 1), and
// derives its event_id from them.
func (g *Generator) ForRun(run string) *Generator {
	g.run = run
	return g
}

// Next returns the next event, stamped as produced at now, as JSON.
func (g *Generator) Next(now time.Time) ([]byte, error) {
	r := g.rng
	id := g.next
	g.next++
	domain := wikis[r.IntN(len(wikis))]
	wiki := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(domain, "www."), ".org"), ".", "") + "wiki"
	title := g.phrase(1 + r.IntN(4))
	titleURL := strings.ReplaceAll(title, " ", "_")
	// Comments carry most of the size variation, as they do upstream.
	comment := g.phrase(4 + r.IntN(50))
	oldLen := 500 + r.Int64N(80_000)
	oldRev := 1_000_000_000 + r.Int64N(500_000_000)
	ev := Event{
		Schema: "/mediawiki/recentchange/1.0.0",
		Meta: Meta{
			URI:       fmt.Sprintf("https://%s/wiki/%s", domain, titleURL),
			RequestID: g.uuid(),
			ID:        g.uuid(),
			DT:        now.UTC(),
			Domain:    domain,
			Stream:    "mediawiki.recentchange",
			Topic:     "eqiad.mediawiki.recentchange",
			Partition: 0,
			Offset:    5_000_000_000 + id,
		},
		ID:               id,
		Type:             g.eventType(),
		Namespace:        []int{0, 0, 0, 1, 2, 4, 6, 14}[r.IntN(8)],
		Title:            title,
		TitleURL:         fmt.Sprintf("https://%s/wiki/%s", domain, titleURL),
		Comment:          comment,
		Timestamp:        now.Unix(),
		User:             fmt.Sprintf("LoadUser%d", r.IntN(5000)),
		Bot:              r.IntN(5) == 0,
		NotifyURL:        fmt.Sprintf("https://%s/w/index.php?diff=%d&oldid=%d", domain, oldRev+1, oldRev),
		Minor:            r.IntN(3) == 0,
		Patrolled:        r.IntN(2) == 0,
		Length:           OldNew{Old: oldLen, New: oldLen + r.Int64N(2000) - 1000},
		Revision:         OldNew{Old: oldRev, New: oldRev + 1},
		ServerURL:        "https://" + domain,
		ServerName:       domain,
		ServerScriptPath: "/w",
		Wiki:             wiki,
		ParsedComment:    fmt.Sprintf(`<a href="/wiki/%s" title="%s">%s</a>: %s`, titleURL, title, title, comment),
		Spillway:         Stamp{EventID: uuid.NewString(), ProducedAt: now.UTC(), Feeder: g.feeder},
	}
	if r.IntN(4) == 0 {
		ev.Tags = []string{"mobile edit", "mobile web edit"}
	}
	if g.run != "" {
		ev.Spillway.EventID = EventID(g.run, id)
		ev.Level = LevelOf(id)
		ev.Loadgen = &RunTag{Run: g.run, Seq: id}
	}
	return json.Marshal(ev)
}

func (g *Generator) eventType() string {
	n := g.rng.IntN(100)
	for _, t := range types {
		if n < t.weight {
			return t.name
		}
		n -= t.weight
	}
	return types[0].name
}

func (g *Generator) phrase(n int) string {
	var b strings.Builder
	for i := range n {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(words[g.rng.IntN(len(words))])
	}
	return b.String()
}

// uuid is a random-looking UUID from the generator's own source: upstream IDs
// only need to look real, and crypto/rand would cost more than the event.
func (g *Generator) uuid() string {
	var b [16]byte
	for i := 0; i < 16; i += 8 {
		v := g.rng.Uint64()
		for j := range 8 {
			b[i+j] = byte(v >> (8 * j))
		}
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return uuid.UUID(b).String()
}
