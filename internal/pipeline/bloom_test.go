package pipeline

import (
	"fmt"
	"sync"
	"testing"
)

func TestBloom_PanicsOnInvalidParams(t *testing.T) {
	tests := []struct {
		name   string
		n      uint64
		fpRate float64
	}{
		{"zero n", 0, 0.01},
		{"zero fpRate", 100, 0},
		{"negative fpRate", 100, -0.1},
		{"fpRate >= 1", 100, 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("expected panic")
				}
			}()
			NewBloom(tt.n, tt.fpRate)
		})
	}
}

func TestBloom_NoFalseNegatives(t *testing.T) {
	b := NewBloom(10_000, 0.01)
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		b.Add(keys[i])
	}
	for i, k := range keys {
		if !b.Contains(k) {
			t.Errorf("key %d not found after Add (false negative)", i)
		}
	}
}

func TestBloom_FalsePositiveRate(t *testing.T) {
	const (
		n      = 10_000
		fp     = 0.01
		probes = 100_000
	)
	b := NewBloom(n, fp)
	for i := 0; i < n; i++ {
		b.Add([]byte(fmt.Sprintf("added-%d", i)))
	}

	falsePositives := 0
	for i := 0; i < probes; i++ {
		key := []byte(fmt.Sprintf("probe-%d", i))
		if b.Contains(key) {
			falsePositives++
		}
	}
	actualFP := float64(falsePositives) / float64(probes)
	if actualFP > fp*2.5 {
		t.Errorf("FP rate too high: got %.4f, expected ~%.4f", actualFP, fp)
	}
	t.Logf("FP rate: got %.4f, target %.4f", actualFP, fp)
}

func TestBloom_Reset(t *testing.T) {
	b := NewBloom(1000, 0.01)
	key := []byte("hello")
	b.Add(key)
	if !b.Contains(key) {
		t.Fatal("key should be present after Add")
	}
	b.Reset()
	if b.Contains(key) {
		t.Error("key should be absent after Reset")
	}
}

func TestBloom_EmptyHasNoMembers(t *testing.T) {
	b := NewBloom(1000, 0.01)
	for i := 0; i < 100; i++ {
		if b.Contains([]byte(fmt.Sprintf("k-%d", i))) {
			t.Errorf("empty filter returned true for %d", i)
		}
	}
}

func TestBloom_ConcurrentAdd(t *testing.T) {
	b := NewBloom(100_000, 0.01)
	const workers = 8
	const perWorker = 1000

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				b.Add([]byte(fmt.Sprintf("w%d-i%d", id, i)))
			}
		}(w)
	}
	wg.Wait()

	missing := 0
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			if !b.Contains([]byte(fmt.Sprintf("w%d-i%d", w, i))) {
				missing++
			}
		}
	}
	if missing > 0 {
		t.Errorf("missing keys after concurrent Add: %d", missing)
	}
}

func TestBloom_SizingFormula(t *testing.T) {
	tests := []struct {
		n      uint64
		fpRate float64
	}{
		{1_000, 0.01},
		{1_000_000, 0.01},
		{1_000_000, 0.001},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("n=%d_fp=%f", tt.n, tt.fpRate), func(t *testing.T) {
			b := NewBloom(tt.n, tt.fpRate)
			if b.M() < tt.n {
				t.Errorf("M=%d too small for n=%d", b.M(), tt.n)
			}
			if b.K() < 1 || b.K() > 32 {
				t.Errorf("K=%d looks unreasonable", b.K())
			}
		})
	}
}

func BenchmarkBloom_Add(b *testing.B) {
	bl := NewBloom(1_000_000, 0.01)
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bl.Add(keys[i%len(keys)])
	}
}

func BenchmarkBloom_Contains(b *testing.B) {
	bl := NewBloom(1_000_000, 0.01)
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
		bl.Add(keys[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bl.Contains(keys[i%len(keys)])
	}
}

func BenchmarkBloom_AddParallel(b *testing.B) {
	bl := NewBloom(1_000_000, 0.01)
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bl.Add(keys[i%len(keys)])
			i++
		}
	})
}
