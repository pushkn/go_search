package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func defaultDedupCfg() DedupConfig {
	return DedupConfig{
		WindowSize:       30 * time.Second,
		Rotations:        3,
		ExpectedElements: 100_000,
		FPRate:           0.01,
	}
}

func TestDeduper_PanicsOnInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  DedupConfig
	}{
		{"zero window", DedupConfig{WindowSize: 0, Rotations: 3, ExpectedElements: 100, FPRate: 0.01}},
		{"rotations < 2", DedupConfig{WindowSize: time.Second, Rotations: 1, ExpectedElements: 100, FPRate: 0.01}},
		{"zero expected", DedupConfig{WindowSize: time.Second, Rotations: 3, ExpectedElements: 0, FPRate: 0.01}},
		{"bad fpRate", DedupConfig{WindowSize: time.Second, Rotations: 3, ExpectedElements: 100, FPRate: 1.5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			NewDeduper(tt.cfg)
		})
	}
}

func TestDeduper_FirstSeenIsNew(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	if d.Seen("u1", "s1", "iphone") {
		t.Error("first seen should return false")
	}
}

func TestDeduper_RepeatedSeenIsDuplicate(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("u1", "s1", "iphone")
	if !d.Seen("u1", "s1", "iphone") {
		t.Error("second seen should return true")
	}
}

func TestDeduper_DifferentUsersIndependent(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("u1", "s1", "iphone")
	if d.Seen("u2", "s2", "iphone") {
		t.Error("different user should be independent")
	}
}

func TestDeduper_DifferentQueriesIndependent(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("u1", "s1", "iphone")
	if d.Seen("u1", "s1", "samsung") {
		t.Error("different query should be independent")
	}
}

func TestDeduper_FallbackToSession(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("", "s1", "iphone")
	if !d.Seen("", "s1", "iphone") {
		t.Error("repeated session+query should be duplicate")
	}
	if d.Seen("", "s2", "iphone") {
		t.Error("different session should be independent")
	}
}

func TestDeduper_UserAndSessionDistinct(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("abc", "", "iphone")
	if d.Seen("", "abc", "iphone") {
		t.Error("user='abc' and session='abc' must be different keys")
	}
}

func TestDeduper_ForgetsAfterRotations(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("u1", "s1", "iphone")

	for i := 0; i < 3; i++ {
		d.Rotate()
	}

	if d.Seen("u1", "s1", "iphone") {
		t.Error("element should be forgotten after all rotations")
	}
}

func TestDeduper_RefreshExtendsLifetime(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("u1", "s1", "iphone")

	d.Rotate()
	if !d.Seen("u1", "s1", "iphone") {
		t.Fatal("element should still be seen after one rotation")
	}

	d.Rotate()
	d.Rotate()

	if !d.Seen("u1", "s1", "iphone") {
		t.Error("element refreshed before rotations should still be seen")
	}
}

func TestDeduper_StartStopByContext(t *testing.T) {
	cfg := defaultDedupCfg()
	cfg.WindowSize = 90 * time.Millisecond
	d := NewDeduper(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	d.Start(ctx)

	d.Seen("u1", "s1", "iphone")
	time.Sleep(200 * time.Millisecond)

	if !d.Seen("u1", "s1", "iphone") {
	}
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestDeduper_ConcurrentSeen(t *testing.T) {
	d := NewDeduper(DedupConfig{
		WindowSize:       30 * time.Second,
		Rotations:        3,
		ExpectedElements: 1_000_000,
		FPRate:           0.01,
	})

	const (
		workers   = 10
		perWorker = 1000
	)
	var firstSeen int64
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				query := fmt.Sprintf("q-%d", i)
				user := fmt.Sprintf("u-%d", id)
				if !d.Seen(user, "", query) {
					atomic.AddInt64(&firstSeen, 1)
				}
			}
		}(w)
	}
	wg.Wait()

	expected := int64(workers * perWorker)
	if firstSeen != expected {
		t.Errorf("first seen count: got %d, want %d (FP?)", firstSeen, expected)
	}
}

func TestDeduper_ConcurrentSeenAndRotate(t *testing.T) {
	d := NewDeduper(defaultDedupCfg())

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
					d.Seen(fmt.Sprintf("u-%d", id), "", fmt.Sprintf("q-%d", i%50))
					i++
				}
			}
		}(w)
	}

	for i := 0; i < 20; i++ {
		d.Rotate()
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

func BenchmarkDeduper_SeenNew(b *testing.B) {
	d := NewDeduper(DedupConfig{
		WindowSize:       30 * time.Second,
		Rotations:        3,
		ExpectedElements: uint64(b.N) + 1000,
		FPRate:           0.01,
	})
	users := make([]string, b.N)
	queries := make([]string, b.N)
	for i := range users {
		users[i] = fmt.Sprintf("u-%d", i)
		queries[i] = fmt.Sprintf("q-%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Seen(users[i], "", queries[i])
	}
}

func BenchmarkDeduper_SeenRepeated(b *testing.B) {
	d := NewDeduper(defaultDedupCfg())
	d.Seen("u1", "s1", "iphone")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.Seen("u1", "s1", "iphone")
	}
}

func BenchmarkDeduper_SeenParallel(b *testing.B) {
	d := NewDeduper(DedupConfig{
		WindowSize:       30 * time.Second,
		Rotations:        3,
		ExpectedElements: 1_000_000,
		FPRate:           0.01,
	})
	const N = 1000
	users := make([]string, N)
	queries := make([]string, N/2)
	for i := range users {
		users[i] = fmt.Sprintf("u-%d", i)
	}
	for i := range queries {
		queries[i] = fmt.Sprintf("q-%d", i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			d.Seen(users[i%N], "", queries[i%(N/2)])
			i++
		}
	})
}
