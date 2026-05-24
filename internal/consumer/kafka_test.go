package consumer

import (
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pushkn/go_search/internal/domain"
)

type mockDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newMockDeduper() *mockDeduper {
	return &mockDeduper{seen: make(map[string]bool)}
}

func (m *mockDeduper) Seen(u, s, q string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := u + "|" + s + "|" + q
	if m.seen[key] {
		return true
	}
	m.seen[key] = true
	return false
}

type mockAnomaly struct {
	mu    sync.Mutex
	calls []string
}

func (m *mockAnomaly) Observe(q string, _ time.Time) {
	m.mu.Lock()
	m.calls = append(m.calls, q)
	m.mu.Unlock()
}

type mockWindow struct {
	mu    sync.Mutex
	calls []string
}

func (m *mockWindow) Add(q string, _ time.Time, _ time.Time) bool {
	m.mu.Lock()
	m.calls = append(m.calls, q)
	m.mu.Unlock()
	return true
}

func newConsumer() (*Consumer, *mockDeduper, *mockAnomaly, *mockWindow) {
	d := newMockDeduper()
	a := &mockAnomaly{}
	w := &mockWindow{}
	c := &Consumer{
		logger:  slog.Default(),
		deduper: d,
		anomaly: a,
		window:  w,
	}
	return c, d, a, w
}

func makeEvent(t *testing.T, ev domain.SearchEvent) kafka.Message {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: b}
}

func TestProcess_ValidEvent(t *testing.T) {
	c, _, a, w := newConsumer()
	msg := makeEvent(t, domain.SearchEvent{
		EventID: "1", Query: "IPHONE", UserID: "u1",
		Timestamp: time.Now().UTC().Add(-time.Second),
	})
	c.process(msg)
	if len(a.calls) != 1 || a.calls[0] != "iphone" {
		t.Errorf("anomaly: %v", a.calls)
	}
	if len(w.calls) != 1 || w.calls[0] != "iphone" {
		t.Errorf("window: %v", w.calls)
	}
}

func TestProcess_InvalidJSON(t *testing.T) {
	c, _, a, w := newConsumer()
	c.process(kafka.Message{Value: []byte("not json")})
	if len(a.calls) != 0 || len(w.calls) != 0 {
		t.Error("invalid json: should drop")
	}
}

func TestProcess_ValidationFails(t *testing.T) {
	c, _, a, w := newConsumer()
	msg := makeEvent(t, domain.SearchEvent{EventID: "", Query: "x", UserID: "u1", Timestamp: time.Now()})
	c.process(msg)
	if len(a.calls) != 0 || len(w.calls) != 0 {
		t.Error("invalid event: should drop")
	}
}

func TestProcess_DedupDrops(t *testing.T) {
	c, _, a, w := newConsumer()
	ev := domain.SearchEvent{
		EventID: "1", Query: "iphone", UserID: "u1",
		Timestamp: time.Now().UTC().Add(-time.Second),
	}
	c.process(makeEvent(t, ev))
	ev.EventID = "2"
	c.process(makeEvent(t, ev))
	if len(a.calls) != 1 {
		t.Errorf("dedup: anomaly count %d, want 1", len(a.calls))
	}
	if len(w.calls) != 1 {
		t.Errorf("dedup: window count %d, want 1", len(w.calls))
	}
}

func TestProcess_QueryIsNormalized(t *testing.T) {
	c, _, _, w := newConsumer()
	msg := makeEvent(t, domain.SearchEvent{
		EventID: "1", Query: "  IPHONE   15  PRO  ", UserID: "u1",
		Timestamp: time.Now().UTC().Add(-time.Second),
	})
	c.process(msg)
	if len(w.calls) != 1 || w.calls[0] != "iphone 15 pro" {
		t.Errorf("normalize: %v", w.calls)
	}
}
