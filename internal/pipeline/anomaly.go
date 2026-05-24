package pipeline

import (
	"context"
	"sync"
	"time"
)

type AnomalyConfig struct {
	BucketDuration time.Duration
	Alpha          float64
	Threshold      float64
	MinAge         time.Duration
	GCInterval     time.Duration
	GCTTL          time.Duration
}

type queryStats struct {
	mu                 sync.Mutex
	ewma               float64
	currentBucketCount int
	currentBucketStart time.Time
	firstSeen          time.Time
	lastUpdate         time.Time
}

type Anomaly struct {
	mu      sync.RWMutex
	queries map[string]*queryStats
	cfg     AnomalyConfig
}

func NewAnomaly(cfg AnomalyConfig) *Anomaly {
	if cfg.BucketDuration <= 0 {
		panic("anomaly: BucketDuration must be positive")
	}
	if cfg.Alpha <= 0 || cfg.Alpha > 1 {
		panic("anomaly: Alpha must be in (0, 1]")
	}
	if cfg.Threshold <= 1 {
		panic("anomaly: Threshold must be > 1")
	}
	if cfg.MinAge < 0 {
		panic("anomaly: MinAge must be >= 0")
	}
	if cfg.GCInterval <= 0 {
		panic("anomaly: GCInterval must be positive")
	}
	if cfg.GCTTL <= 0 {
		panic("anomaly: GCTTL must be positive")
	}
	return &Anomaly{
		queries: make(map[string]*queryStats),
		cfg:     cfg,
	}
}

func (a *Anomaly) Observe(query string, now time.Time) {
	a.mu.RLock()
	stats, ok := a.queries[query]
	a.mu.RUnlock()

	if !ok {
		a.mu.Lock()
		stats, ok = a.queries[query]
		if !ok {
			stats = &queryStats{
				firstSeen:          now,
				currentBucketStart: now.Truncate(a.cfg.BucketDuration),
				lastUpdate:         now,
				currentBucketCount: 0,
			}
			a.queries[query] = stats
		}
		a.mu.Unlock()
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()

	bucketStart := now.Truncate(a.cfg.BucketDuration)
	if bucketStart.After(stats.currentBucketStart) {
		closedRate := float64(stats.currentBucketCount)
		stats.ewma = a.cfg.Alpha*closedRate + (1-a.cfg.Alpha)*stats.ewma

		missedBuckets := int(bucketStart.Sub(stats.currentBucketStart)/a.cfg.BucketDuration) - 1
		for i := 0; i < missedBuckets; i++ {
			stats.ewma = (1 - a.cfg.Alpha) * stats.ewma
		}

		stats.currentBucketStart = bucketStart
		stats.currentBucketCount = 0
	}

	stats.currentBucketCount++
	stats.lastUpdate = now
}

func (a *Anomaly) IsAnomaly(query string, now time.Time) bool {
	a.mu.RLock()
	stats, ok := a.queries[query]
	a.mu.RUnlock()

	if !ok {
		return false
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()

	if now.Sub(stats.firstSeen) < a.cfg.MinAge {
		return false
	}
	if stats.ewma < 1 {
		return false
	}
	return float64(stats.currentBucketCount) > stats.ewma*a.cfg.Threshold
}

func (a *Anomaly) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(a.cfg.GCInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				a.gc(t)
			}
		}
	}()
}

func (a *Anomaly) gc(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cutoff := now.Add(-a.cfg.GCTTL)
	for q, stats := range a.queries {
		stats.mu.Lock()
		expired := stats.lastUpdate.Before(cutoff)
		stats.mu.Unlock()
		if expired {
			delete(a.queries, q)
		}
	}
}

func (a *Anomaly) Size() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.queries)
}
