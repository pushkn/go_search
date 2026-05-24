package pipeline

import (
	"math"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

type Bloom struct {
	bits []uint64
	m    uint64
	k    uint64
}

func NewBloom(n uint64, fpRate float64) *Bloom {
	if n == 0 {
		panic("bloom: n must be > 0")
	}
	if fpRate <= 0 || fpRate >= 1 {
		panic("bloom: fpRate must be in (0, 1)")
	}

	mFloat := -float64(n) * math.Log(fpRate) / (math.Ln2 * math.Ln2)
	m := uint64(math.Ceil(mFloat))
	if m < 64 {
		m = 64
	}

	kFloat := (float64(m) / float64(n)) * math.Ln2
	k := uint64(math.Ceil(kFloat))
	if k < 1 {
		k = 1
	}

	words := (m + 63) / 64
	return &Bloom{
		bits: make([]uint64, words),
		m:    words * 64,
		k:    k,
	}
}

func (b *Bloom) Add(key []byte) {
	h1, h2 := hash128(key)
	for i := uint64(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		word := idx >> 6
		mask := uint64(1) << (idx & 63)
		atomic.OrUint64(&b.bits[word], mask)
	}
}

func (b *Bloom) Contains(key []byte) bool {
	h1, h2 := hash128(key)
	for i := uint64(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		word := idx >> 6
		mask := uint64(1) << (idx & 63)
		if atomic.LoadUint64(&b.bits[word])&mask == 0 {
			return false
		}
	}
	return true
}

func (b *Bloom) Reset() {
	for i := range b.bits {
		atomic.StoreUint64(&b.bits[i], 0)
	}
}

func (b *Bloom) M() uint64 { return b.m }
func (b *Bloom) K() uint64 { return b.k }

func hash128(key []byte) (uint64, uint64) {
	h := xxhash.Sum64(key)
	return h, mix64(h)
}

func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
