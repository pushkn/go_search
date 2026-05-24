package snapshot

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pushkn/go_search/internal/topk"
)

type fakeWindow struct {
	mu       sync.Mutex
	queries  map[string]int
	capacity int
}

func newFakeWindow() *fakeWindow {
	return &fakeWindow{queries: make(map[string]int), capacity: 1000}
}

func (f *fakeWindow) add(q string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries[q] += n
}

func (f *fakeWindow) Snapshot(now time.Time) *topk.SpaceSaving {
	f.mu.Lock()
	defer f.mu.Unlock()
	ss := topk.New(f.capacity)
	for q, c := range f.queries {
		for i := 0; i < c; i++ {
			ss.Add(q)
		}
	}
	return ss
}

type fakeAnomaly struct {
	mu       sync.Mutex
	suspects map[string]bool
}

func newFakeAnomaly() *fakeAnomaly {
	return &fakeAnomaly{suspects: make(map[string]bool)}
}

func (f *fakeAnomaly) mark(q string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.suspects[q] = true
}

func (f *fakeAnomaly) IsAnomaly(q string, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.suspects[q]
}

type fakeStopList struct {
	words map[string]bool
}

func (f *fakeStopList) Contains(w string) bool {
	return f.words[w]
}

func cfg() Config {
	return Config{
		Interval:       50 * time.Millisecond,
		MaxSize:        10,
		AnomalyPenalty: 0.1,
	}
}

func TestNewBuilder_Panics(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"zero interval", Config{Interval: 0, MaxSize: 10, AnomalyPenalty: 0.1}},
		{"zero size", Config{Interval: time.Second, MaxSize: 0, AnomalyPenalty: 0.1}},
		{"bad penalty", Config{Interval: time.Second, MaxSize: 10, AnomalyPenalty: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			NewBuilder(newFakeWindow(), nil, nil, tt.cfg)
		})
	}
}

func TestBuilder_GetReturnsEmptyBeforeBuild(t *testing.T) {
	b := NewBuilder(newFakeWindow(), nil, nil, cfg())
	if len(b.Get()) != 0 {
		t.Error("Get before Start should return empty")
	}
	if b.Ready() {
		t.Error("Ready before Start should be false")
	}
}

func TestBuilder_BuildPopulatesSnapshot(t *testing.T) {
	w := newFakeWindow()
	w.add("iphone", 100)
	w.add("samsung", 50)
	w.add("xiaomi", 30)

	b := NewBuilder(w, nil, nil, cfg())
	b.build(time.Now())

	got := b.Get()
	if len(got) != 3 {
		t.Fatalf("len: got %d, want 3", len(got))
	}
	if got[0].Query != "iphone" || got[0].Count != 100 {
		t.Errorf("top: got %+v, want iphone/100", got[0])
	}
	if !b.Ready() {
		t.Error("Ready should be true after build")
	}
}

func TestBuilder_AnomalyPenaltyApplied(t *testing.T) {
	w := newFakeWindow()
	w.add("victim", 1000)
	w.add("normal", 150)

	a := newFakeAnomaly()
	a.mark("victim")

	b := NewBuilder(w, a, nil, cfg())
	b.build(time.Now())

	got := b.Get()
	if got[0].Query != "normal" {
		t.Errorf("normal rank above victim: %+v", got)
	}
}

func TestBuilder_StopListFilters(t *testing.T) {
	w := newFakeWindow()
	w.add("good", 100)
	w.add("bad", 200)

	s := &fakeStopList{words: map[string]bool{"bad": true}}
	b := NewBuilder(w, nil, s, cfg())
	b.build(time.Now())

	got := b.Get()
	if len(got) != 1 || got[0].Query != "good" {
		t.Errorf("stop list should filter bad: got %+v", got)
	}
}

func TestBuilder_MaxSizeRespected(t *testing.T) {
	w := newFakeWindow()
	for i := 0; i < 100; i++ {
		w.add(string(rune('a'+i%26))+string(rune('0'+i/26)), 100-i)
	}

	c := cfg()
	c.MaxSize = 5
	b := NewBuilder(w, nil, nil, c)
	b.build(time.Now())

	if len(b.Get()) != 5 {
		t.Errorf("MaxSize: got %d, want 5", len(b.Get()))
	}
}

func TestBuilder_StartTriggersBuilds(t *testing.T) {
	w := newFakeWindow()
	w.add("x", 10)

	b := NewBuilder(w, nil, nil, cfg())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)

	time.Sleep(150 * time.Millisecond)

	if !b.Ready() {
		t.Error("should be ready after Start")
	}
	if len(b.Get()) == 0 {
		t.Error("Get after Start should return data")
	}
}

func TestBuilder_GetIsLockFree(t *testing.T) {
	w := newFakeWindow()
	for i := 0; i < 10; i++ {
		w.add(string(rune('a'+i)), 100-i)
	}
	b := NewBuilder(w, nil, nil, cfg())
	b.build(time.Now())

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = b.Get()
				}
			}
		}()
	}

	for i := 0; i < 100; i++ {
		b.build(time.Now())
	}
	close(stop)
	wg.Wait()
}

func BenchmarkBuilder_Get(b *testing.B) {
	w := newFakeWindow()
	for i := 0; i < 1000; i++ {
		w.add(string(rune('a'+i%26))+string(rune('0'+i/26)), 1000-i)
	}
	bld := NewBuilder(w, nil, nil, Config{Interval: time.Second, MaxSize: 100, AnomalyPenalty: 0.1})
	bld.build(time.Now())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bld.Get()
	}
}
