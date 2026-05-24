package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func defaultAnomalyCfg() AnomalyConfig {
	return AnomalyConfig{
		BucketDuration: 10 * time.Second,
		Alpha:          0.3,
		Threshold:      5.0,
		MinAge:         60 * time.Second,
		GCInterval:     1 * time.Minute,
		GCTTL:          10 * time.Minute,
	}
}

func TestAnomaly_PanicsOnInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  AnomalyConfig
	}{
		{"zero bucket", AnomalyConfig{BucketDuration: 0, Alpha: 0.3, Threshold: 5, MinAge: time.Minute, GCInterval: time.Minute, GCTTL: time.Minute}},
		{"bad alpha low", AnomalyConfig{BucketDuration: time.Second, Alpha: 0, Threshold: 5, MinAge: time.Minute, GCInterval: time.Minute, GCTTL: time.Minute}},
		{"bad alpha high", AnomalyConfig{BucketDuration: time.Second, Alpha: 1.5, Threshold: 5, MinAge: time.Minute, GCInterval: time.Minute, GCTTL: time.Minute}},
		{"bad threshold", AnomalyConfig{BucketDuration: time.Second, Alpha: 0.3, Threshold: 1, MinAge: time.Minute, GCInterval: time.Minute, GCTTL: time.Minute}},
		{"bad GCInterval", AnomalyConfig{BucketDuration: time.Second, Alpha: 0.3, Threshold: 5, MinAge: time.Minute, GCInterval: 0, GCTTL: time.Minute}},
		{"bad GCTTL", AnomalyConfig{BucketDuration: time.Second, Alpha: 0.3, Threshold: 5, MinAge: time.Minute, GCInterval: time.Minute, GCTTL: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			NewAnomaly(tt.cfg)
		})
	}
}

func TestAnomaly_UnknownQueryIsNotAnomaly(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	if a.IsAnomaly("never-seen", now) {
		t.Error("unknown query must not be anomaly")
	}
}

func TestAnomaly_YoungQueryIsNotAnomaly(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 1000; i++ {
		a.Observe("burst", now)
	}
	if a.IsAnomaly("burst", now) {
		t.Error("young query mustnt be flagged")
	}
}

func TestAnomaly_StableTrafficIsNotAnomaly(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	start := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	for bucket := 0; bucket < 20; bucket++ {
		bucketTime := start.Add(time.Duration(bucket) * 10 * time.Second)
		for i := 0; i < 10; i++ {
			a.Observe("stable", bucketTime.Add(time.Duration(i)*time.Millisecond))
		}
	}
	now := start.Add(200 * time.Second).Add(5 * time.Second)
	for i := 0; i < 10; i++ {
		a.Observe("stable", now.Add(time.Duration(i)*time.Millisecond))
	}
	if a.IsAnomaly("stable", now) {
		t.Error("stable traffic should not be anomaly")
	}
}

func TestAnomaly_BurstOnOldQueryIsAnomaly(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	start := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	for bucket := 0; bucket < 20; bucket++ {
		bucketTime := start.Add(time.Duration(bucket) * 10 * time.Second)
		for i := 0; i < 5; i++ {
			a.Observe("victim", bucketTime.Add(time.Duration(i)*time.Millisecond))
		}
	}

	burstTime := start.Add(200 * time.Second).Add(5 * time.Second)
	for i := 0; i < 100; i++ {
		a.Observe("victim", burstTime.Add(time.Duration(i)*time.Millisecond))
	}
	if !a.IsAnomaly("victim", burstTime.Add(200*time.Millisecond)) {
		t.Error("burst should be flagged")
	}
}

func TestAnomaly_BurstOnYoungQueryIsNotAnomaly(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 1000; i++ {
		a.Observe("trending", now.Add(time.Duration(i)*time.Millisecond))
	}
	if a.IsAnomaly("trending", now.Add(time.Second)) {
		t.Error("young trending query shouldnt be flagged")
	}
}

func TestAnomaly_DifferentQueriesIndependent(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	start := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	for bucket := 0; bucket < 20; bucket++ {
		bucketTime := start.Add(time.Duration(bucket) * 10 * time.Second)
		for i := 0; i < 5; i++ {
			a.Observe("victim", bucketTime.Add(time.Duration(i)*time.Millisecond))
			a.Observe("normal", bucketTime.Add(time.Duration(i)*time.Millisecond))
		}
	}

	now := start.Add(200 * time.Second).Add(5 * time.Second)
	for i := 0; i < 100; i++ {
		a.Observe("victim", now.Add(time.Duration(i)*time.Millisecond))
	}
	for i := 0; i < 5; i++ {
		a.Observe("normal", now.Add(time.Duration(i)*time.Millisecond))
	}

	checkTime := now.Add(200 * time.Millisecond)
	if !a.IsAnomaly("victim", checkTime) {
		t.Error("victim should be anomaly")
	}
	if a.IsAnomaly("normal", checkTime) {
		t.Error("normal should not be anomaly")
	}
}

func TestAnomaly_GCRemovesStaleEntries(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	t0 := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	a.Observe("old", t0)
	a.Observe("fresh", t0.Add(5*time.Minute))

	if a.Size() != 2 {
		t.Fatalf("expected 2 queries before GC, got %d", a.Size())
	}

	a.gc(t0.Add(15 * time.Minute))

	if a.Size() != 1 {
		t.Errorf("expected 1 query after GC, got %d", a.Size())
	}
	if a.IsAnomaly("old", t0.Add(15*time.Minute)) {
		t.Error("old query should be gone")
	}
}

func TestAnomaly_StartStopByContext(t *testing.T) {
	cfg := defaultAnomalyCfg()
	cfg.GCInterval = 50 * time.Millisecond
	a := NewAnomaly(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	a.Start(ctx)
	time.Sleep(150 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestAnomaly_ConcurrentObserve(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	const workers = 10
	const perWorker = 1000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				q := fmt.Sprintf("q-%d", i%50)
				a.Observe(q, now)
			}
		}(w)
	}
	wg.Wait()

	if a.Size() != 50 {
		t.Errorf("expected 50 unique queries, got %d", a.Size())
	}
}

func TestAnomaly_ConcurrentObserveAndIsAnomaly(t *testing.T) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

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
					a.Observe(fmt.Sprintf("q-%d", i%100), now)
					i++
				}
			}
		}(w)
	}
	for i := 0; i < 1000; i++ {
		_ = a.IsAnomaly(fmt.Sprintf("q-%d", i%100), now)
	}
	close(stop)
	wg.Wait()
}

func BenchmarkAnomaly_Observe_Existing(b *testing.B) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	a.Observe("hot", now)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Observe("hot", now)
	}
}

func BenchmarkAnomaly_Observe_New(b *testing.B) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	queries := make([]string, b.N)
	for i := range queries {
		queries[i] = fmt.Sprintf("q-%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Observe(queries[i], now)
	}
}

func BenchmarkAnomaly_IsAnomaly(b *testing.B) {
	a := NewAnomaly(defaultAnomalyCfg())
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)
	a.Observe("hot", now)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = a.IsAnomaly("hot", now)
	}
}
