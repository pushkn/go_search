package topk

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func windowConfig() Config {
	return Config{
		WindowSize:     30 * time.Second,
		BucketDuration: 10 * time.Second,
		Capacity:       100,
	}
}

func TestNewWindow_PanicsOnInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"zero window", Config{WindowSize: 0, BucketDuration: time.Second, Capacity: 10}},
		{"zero bucket", Config{WindowSize: time.Minute, BucketDuration: 0, Capacity: 10}},
		{"non-divisible", Config{WindowSize: 10 * time.Second, BucketDuration: 3 * time.Second, Capacity: 10}},
		{"zero capacity", Config{WindowSize: time.Minute, BucketDuration: time.Second, Capacity: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			NewWindow(tt.cfg)
		})
	}
}

func TestWindow_BucketCount(t *testing.T) {
	w := NewWindow(windowConfig())
	if w.BucketCount() != 3 {
		t.Errorf("BucketCount: got %d, want 3", w.BucketCount())
	}
}

func TestWindow_AddAndSnapshot(t *testing.T) {
	w := NewWindow(windowConfig())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		w.Add("iphone", now, now)
	}
	for i := 0; i < 3; i++ {
		w.Add("samsung", now, now)
	}

	top := w.Snapshot(now).TopK(10)
	counts := topToMap(top)

	if counts["iphone"] != 5 {
		t.Errorf("iphone: got %d, want 5", counts["iphone"])
	}
	if counts["samsung"] != 3 {
		t.Errorf("samsung: got %d, want 3", counts["samsung"])
	}
}

func TestWindow_AddRejectsFuture(t *testing.T) {
	w := NewWindow(windowConfig())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	future := now.Add(1 * time.Second)

	if w.Add("x", future, now) {
		t.Error("Add should reject future timestamp")
	}
	if got := w.Snapshot(now).Len(); got != 0 {
		t.Errorf("snapshot should be empty: got %d", got)
	}
}

func TestWindow_AddRejectsStale(t *testing.T) {
	w := NewWindow(windowConfig())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-31 * time.Second)

	if w.Add("x", stale, now) {
		t.Error("Add should reject stale timestamp")
	}
	if got := w.Snapshot(now).Len(); got != 0 {
		t.Errorf("snapshot should be empty: got %d", got)
	}
}

func TestWindow_AddAcceptsBoundary(t *testing.T) {
	w := NewWindow(windowConfig())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	justInside := now.Add(-30*time.Second + time.Nanosecond)
	if !w.Add("inside", justInside, now) {
		t.Error("event just inside window should be accepted")
	}

	exactlyAtBoundary := now.Add(-30 * time.Second)
	if w.Add("boundary", exactlyAtBoundary, now) {
		t.Error("event exactly at -windowSize should be rejected")
	}
}

func TestWindow_EventsInDifferentBuckets(t *testing.T) {
	w := NewWindow(Config{
		WindowSize:     5 * time.Minute,
		BucketDuration: 10 * time.Second,
		Capacity:       100,
	})
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	w.Add("a", now, now)
	w.Add("b", now.Add(-15*time.Second), now)
	w.Add("c", now.Add(-25*time.Second), now)

	top := w.Snapshot(now).TopK(10)
	counts := topToMap(top)

	if counts["a"] != 1 || counts["b"] != 1 || counts["c"] != 1 {
		t.Errorf("expected 3 queries with cnt 1, got %+v", counts)
	}
}

func TestWindow_OldBucketReplacedOnNewEvent(t *testing.T) {
	w := NewWindow(windowConfig())

	t1 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		w.Add("old", t1, t1)
	}

	t2 := t1.Add(30 * time.Second)
	w.Add("new", t2, t2)

	top := w.Snapshot(t2).TopK(10)
	counts := topToMap(top)

	if counts["old"] != 0 {
		t.Errorf("'old' should be evicted, got count %d", counts["old"])
	}
	if counts["new"] != 1 {
		t.Errorf("'new' should be present, got count %d", counts["new"])
	}
}

func TestWindow_Rotate(t *testing.T) {
	w := NewWindow(windowConfig())
	t1 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 5; i++ {
		w.Add("x", t1, t1)
	}

	if got := w.Snapshot(t1).Len(); got != 1 {
		t.Fatalf("before rotate: got %d, want 1", got)
	}

	tFuture := t1.Add(60 * time.Second)
	w.Rotate(tFuture)

	if got := w.Snapshot(tFuture).Len(); got != 0 {
		t.Errorf("after rotate: got %d, want 0", got)
	}
}

func TestWindow_RotateKeepsValidBuckets(t *testing.T) {
	w := NewWindow(windowConfig())
	t1 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	w.Add("old", t1, t1)
	w.Add("fresh", t1.Add(20*time.Second), t1.Add(20*time.Second))

	now := t1.Add(25 * time.Second)
	w.Rotate(now)

	top := w.Snapshot(now).TopK(10)
	counts := topToMap(top)

	if counts["fresh"] != 1 {
		t.Errorf("fresh should survive: got %d", counts["fresh"])
	}
}

func TestWindow_SnapshotIgnoresStaleBuckets(t *testing.T) {
	w := NewWindow(windowConfig())
	t1 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	w.Add("x", t1, t1)

	future := t1.Add(60 * time.Second)
	top := w.Snapshot(future).TopK(10)
	if len(top) != 0 {
		t.Errorf("snapshot far in future should be empty: got %+v", top)
	}
}

func TestWindow_ConcurrentAdd(t *testing.T) {
	w := NewWindow(Config{
		WindowSize:     30 * time.Second,
		BucketDuration: 10 * time.Second,
		Capacity:       1000,
	})
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	const (
		workers       = 10
		eventsPerW    = 1000
		uniqueQueries = 50
	)

	var added int64
	var wg sync.WaitGroup
	for w_id := 0; w_id < workers; w_id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < eventsPerW; i++ {
				q := fmt.Sprintf("q-%d", (id*eventsPerW+i)%uniqueQueries)
				if w.Add(q, now, now) {
					atomic.AddInt64(&added, 1)
				}
			}
		}(w_id)
	}
	wg.Wait()

	expected := int64(workers * eventsPerW)
	if added != expected {
		t.Errorf("added count: got %d, want %d", added, expected)
	}

	top := w.Snapshot(now).TopK(uniqueQueries)
	var total int
	for _, e := range top {
		total += e.Count
	}
	if total != int(expected) {
		t.Errorf("total count in snapshot: got %d, want %d", total, expected)
	}
}

func TestWindow_ConcurrentAddAndSnapshot(t *testing.T) {
	w := NewWindow(Config{
		WindowSize:     30 * time.Second,
		BucketDuration: 10 * time.Second,
		Capacity:       1000,
	})
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := 0
			for {
				select {
				case <-stop:
					return
				default:
					w.Add(fmt.Sprintf("q-%d", j%100), now, now)
					j++
				}
			}
		}()
	}

	for i := 0; i < 100; i++ {
		_ = w.Snapshot(now)
	}
	close(stop)
	wg.Wait()
}

func TestWindow_RingBufferReuse(t *testing.T) {
	w := NewWindow(windowConfig())

	t1 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		w.Add("first", t1, t1)
	}

	t2 := t1.Add(30 * time.Second)
	for i := 0; i < 7; i++ {
		w.Add("second", t2, t2)
	}

	top := w.Snapshot(t2).TopK(10)
	counts := topToMap(top)

	if counts["first"] != 0 {
		t.Errorf("first should be replaced by second: got count %d", counts["first"])
	}
	if counts["second"] != 7 {
		t.Errorf("second: got %d, want 7", counts["second"])
	}
}

func TestWindow_TimestampTruncation(t *testing.T) {
	w := NewWindow(windowConfig())
	bucketStart := time.Date(2026, 5, 24, 12, 0, 10, 0, time.UTC)

	w.Add("a", bucketStart, bucketStart)
	w.Add("a", bucketStart.Add(3*time.Second), bucketStart.Add(3*time.Second))
	w.Add("a", bucketStart.Add(9*time.Second+999*time.Millisecond), bucketStart.Add(9*time.Second+999*time.Millisecond))

	now := bucketStart.Add(10 * time.Second)
	top := w.Snapshot(now).TopK(10)
	counts := topToMap(top)

	if counts["a"] != 3 {
		t.Errorf("all 3 events should be in same bucket: got count %d", counts["a"])
	}
}

func topToMap(top []Entry) map[string]int {
	m := make(map[string]int, len(top))
	for _, e := range top {
		m[e.Query] = e.Count
	}
	return m
}

func BenchmarkWindow_Add(b *testing.B) {
	w := NewWindow(Config{
		WindowSize:     5 * time.Minute,
		BucketDuration: 10 * time.Second,
		Capacity:       10_000,
	})
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Add("hot", now, now)
	}
}

func BenchmarkWindow_AddConcurrent(b *testing.B) {
	w := NewWindow(Config{
		WindowSize:     5 * time.Minute,
		BucketDuration: 10 * time.Second,
		Capacity:       10_000,
	})
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			w.Add(fmt.Sprintf("q-%d", i%1000), now, now)
			i++
		}
	})
}

func BenchmarkWindow_Snapshot(b *testing.B) {
	w := NewWindow(Config{
		WindowSize:     5 * time.Minute,
		BucketDuration: 10 * time.Second,
		Capacity:       10_000,
	})
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 100_000; i++ {
		w.Add(fmt.Sprintf("q-%d", i%5000), now, now)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.Snapshot(now)
	}
}
