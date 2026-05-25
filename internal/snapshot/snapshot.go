package snapshot

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pushkn/go_search/internal/metrics"
	"github.com/pushkn/go_search/internal/topk"
)

type Entry struct {
	Query string `json:"query"`
	Count int    `json:"count"`
}

type Config struct {
	Interval       time.Duration
	MaxSize        int
	AnomalyPenalty float64
}

type WindowSnapshot interface {
	Snapshot(now time.Time) *topk.SpaceSaving
}

type AnomalyChecker interface {
	IsAnomaly(query string, now time.Time) bool
}

type StopListChecker interface {
	Contains(word string) bool
}

type Builder struct {
	window   WindowSnapshot
	anomaly  AnomalyChecker
	stoplist StopListChecker
	cfg      Config
	value    atomic.Value
	ready    atomic.Bool
}

func NewBuilder(window WindowSnapshot, anomaly AnomalyChecker, stoplist StopListChecker, cfg Config) *Builder {
	if window == nil {
		panic("snapshot: window must not be nil")
	}
	if cfg.Interval <= 0 {
		panic("snapshot: Interval must be positive")
	}
	if cfg.MaxSize <= 0 {
		panic("snapshot: MaxSize must be positive")
	}
	if cfg.AnomalyPenalty <= 0 || cfg.AnomalyPenalty > 1 {
		panic("snapshot: AnomalyPenalty must be in (0, 1]")
	}
	b := &Builder{
		window:   window,
		anomaly:  anomaly,
		stoplist: stoplist,
		cfg:      cfg,
	}
	b.value.Store([]Entry{})
	return b
}

func (b *Builder) Get() []Entry {
	return b.value.Load().([]Entry)
}

func (b *Builder) Ready() bool {
	return b.ready.Load()
}

func (b *Builder) Start(ctx context.Context) {
	go func() {
		b.build(time.Now())

		ticker := time.NewTicker(b.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				b.build(t)
			}
		}
	}()
}

func (b *Builder) build(now time.Time) {
	start := time.Now()
	defer func() {
		metrics.SnapshotBuildDuration.Observe(time.Since(start).Seconds())
		metrics.SnapshotLastBuilt.Set(float64(time.Now().Unix()))
	}()

	ss := b.window.Snapshot(now)
	raw := ss.TopK(b.cfg.MaxSize * 2)

	type scored struct {
		query string
		score float64
	}
	entries := make([]scored, 0, len(raw))
	anomalyCount := 0
	for _, e := range raw {
		if b.stoplist != nil && b.stoplist.Contains(e.Query) {
			continue
		}
		score := float64(e.Count)
		if b.anomaly != nil && b.anomaly.IsAnomaly(e.Query, now) {
			score *= b.cfg.AnomalyPenalty
			anomalyCount++
		}
		entries = append(entries, scored{e.Query, score})
	}
	metrics.EventsMarkedAnomaly.Add(float64(anomalyCount))

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].score > entries[j].score
	})

	limit := b.cfg.MaxSize
	if len(entries) < limit {
		limit = len(entries)
	}
	result := make([]Entry, limit)
	for i := 0; i < limit; i++ {
		result[i] = Entry{
			Query: entries[i].query,
			Count: int(entries[i].score),
		}
	}

	b.value.Store(result)
	b.ready.Store(true)
	metrics.SnapshotSize.Set(float64(len(result)))
}

func (b *Builder) BuildOnce() {
	b.build(time.Now())
}
