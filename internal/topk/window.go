package topk

import (
	"sync"
	"time"
)

type bucketSlot struct {
	mu        sync.Mutex
	ss        *SpaceSaving
	startTime time.Time
}

type Window struct {
	mu             sync.RWMutex
	buckets        []*bucketSlot
	bucketCount    int
	bucketDuration time.Duration
	windowSize     time.Duration
	capacity       int
}

type Config struct {
	WindowSize     time.Duration
	BucketDuration time.Duration
	Capacity       int
}

func NewWindow(cfg Config) *Window {
	if cfg.WindowSize <= 0 {
		panic("topk: WindowSize must be positive")
	}
	if cfg.BucketDuration <= 0 {
		panic("topk: BucketDuration must be positive")
	}
	if cfg.WindowSize%cfg.BucketDuration != 0 {
		panic("topk: WindowSize must be a multiple of BucketDuration")
	}
	if cfg.Capacity <= 0 {
		panic("topk: Capacity must be positive")
	}

	bucketCount := int(cfg.WindowSize / cfg.BucketDuration)
	buckets := make([]*bucketSlot, bucketCount)
	for i := range buckets {
		buckets[i] = &bucketSlot{ss: New(cfg.Capacity)}
	}

	return &Window{
		buckets:        buckets,
		bucketCount:    bucketCount,
		bucketDuration: cfg.BucketDuration,
		windowSize:     cfg.WindowSize,
		capacity:       cfg.Capacity,
	}
}

func (w *Window) Add(query string, ts time.Time, now time.Time) bool {
	if ts.After(now) {
		return false
	}
	if now.Sub(ts) >= w.windowSize {
		return false
	}

	bucketStart := ts.Truncate(w.bucketDuration)
	idx := w.bucketIndex(bucketStart)

	w.mu.RLock()
	bs := w.buckets[idx]
	w.mu.RUnlock()

	bs.mu.Lock()
	defer bs.mu.Unlock()

	if !bs.startTime.Equal(bucketStart) {
		bs.ss = New(w.capacity)
		bs.startTime = bucketStart
	}
	bs.ss.Add(query)
	return true
}

func (w *Window) Snapshot(now time.Time) *SpaceSaving {
	w.mu.RLock()
	defer w.mu.RUnlock()

	merged := New(w.capacity)
	cutoff := now.Add(-w.windowSize)

	for _, bs := range w.buckets {
		bs.mu.Lock()
		valid := !bs.startTime.IsZero() &&
			!bs.startTime.Before(cutoff) &&
			!bs.startTime.After(now)
		if valid {
			merged.Merge(bs.ss)
		}
		bs.mu.Unlock()
	}
	return merged
}

func (w *Window) Rotate(now time.Time) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	cutoff := now.Add(-w.windowSize)
	for _, bs := range w.buckets {
		bs.mu.Lock()
		if !bs.startTime.IsZero() && !bs.startTime.After(cutoff) {
			bs.ss = New(w.capacity)
			bs.startTime = time.Time{}
		}
		bs.mu.Unlock()
	}
}

func (w *Window) BucketCount() int {
	return w.bucketCount
}

func (w *Window) bucketIndex(bucketStart time.Time) int {
	unitsSinceEpoch := bucketStart.UnixNano() / int64(w.bucketDuration)
	idx := int(unitsSinceEpoch % int64(w.bucketCount))
	if idx < 0 {
		idx += w.bucketCount
	}
	return idx
}
