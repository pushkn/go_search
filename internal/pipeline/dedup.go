package pipeline

import (
	"context"
	"sync"
	"time"
)

type DedupConfig struct {
	WindowSize       time.Duration
	Rotations        int
	ExpectedElements uint64
	FPRate           float64
}

type Deduper struct {
	mu      sync.RWMutex
	filters []*Bloom
	cfg     DedupConfig
}

func NewDeduper(cfg DedupConfig) *Deduper {
	if cfg.WindowSize <= 0 {
		panic("dedup: WindowSize must be positive")
	}
	if cfg.Rotations < 2 {
		panic("dedup: Rotations must be >= 2")
	}
	if cfg.ExpectedElements == 0 {
		panic("dedup: ExpectedElements must be > 0")
	}
	if cfg.FPRate <= 0 || cfg.FPRate >= 1 {
		panic("dedup: FPRate must be in (0, 1)")
	}

	perFilterCapacity := cfg.ExpectedElements / uint64(cfg.Rotations)
	if perFilterCapacity == 0 {
		perFilterCapacity = 1
	}

	filters := make([]*Bloom, cfg.Rotations)
	for i := range filters {
		filters[i] = NewBloom(perFilterCapacity, cfg.FPRate)
	}

	return &Deduper{
		filters: filters,
		cfg:     cfg,
	}
}

var keyBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 128)
		return &buf
	},
}

func (d *Deduper) Seen(userID, sessionID, query string) bool {
	bufPtr := keyBufPool.Get().(*[]byte)
	defer func() {
		*bufPtr = (*bufPtr)[:0]
		keyBufPool.Put(bufPtr)
	}()
	key := appendKey((*bufPtr)[:0], userID, sessionID, query)
	*bufPtr = key

	d.mu.RLock()
	defer d.mu.RUnlock()

	current := d.filters[0]
	for _, f := range d.filters {
		if f.Contains(key) {
			current.Add(key)
			return true
		}
	}
	current.Add(key)
	return false
}

func (d *Deduper) Rotate() {
	d.mu.Lock()
	defer d.mu.Unlock()

	last := len(d.filters) - 1
	oldest := d.filters[last]
	oldest.Reset()

	for i := last; i > 0; i-- {
		d.filters[i] = d.filters[i-1]
	}
	d.filters[0] = oldest
}

func (d *Deduper) Start(ctx context.Context) {
	interval := d.cfg.WindowSize / time.Duration(d.cfg.Rotations)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.Rotate()
			}
		}
	}()
}

func appendKey(buf []byte, userID, sessionID, query string) []byte {
	if userID != "" {
		buf = append(buf, 'u', ':')
		buf = append(buf, userID...)
	} else {
		buf = append(buf, 's', ':')
		buf = append(buf, sessionID...)
	}
	buf = append(buf, '|')
	buf = append(buf, query...)
	return buf
}
