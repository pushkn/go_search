package topk

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestNew_PanicsOnInvalidCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on capacity <= 0")
		}
	}()
	New(0)
}

func TestEmpty(t *testing.T) {
	s := New(10)
	if s.Len() != 0 {
		t.Errorf("Len: got %d, want 0", s.Len())
	}
	if s.Cap() != 10 {
		t.Errorf("Cap: got %d, want 10", s.Cap())
	}
	if got := s.TopK(5); len(got) != 0 {
		t.Errorf("TopK on empty: got %v, want empty", got)
	}
}

func TestSingleAdd(t *testing.T) {
	s := New(10)
	s.Add("iphone")

	if s.Len() != 1 {
		t.Errorf("Len: got %d, want 1", s.Len())
	}
	got := s.TopK(5)
	if len(got) != 1 || got[0].Query != "iphone" || got[0].Count != 1 || got[0].Error != 0 {
		t.Errorf("TopK: got %+v, want [{iphone 1 0}]", got)
	}
}

func TestRepeatedAdd(t *testing.T) {
	s := New(10)
	for i := 0; i < 5; i++ {
		s.Add("iphone")
	}
	got := s.TopK(1)
	if len(got) != 1 || got[0].Query != "iphone" || got[0].Count != 5 {
		t.Errorf("TopK: got %+v, want count=5", got)
	}
}

func TestTopOrder(t *testing.T) {
	s := New(10)
	stream := []string{
		"a", "a", "a", "a", "a",
		"b", "b", "b",
		"c", "c", "c", "c",
		"d",
	}
	for _, q := range stream {
		s.Add(q)
	}

	got := s.TopK(3)
	if len(got) != 3 {
		t.Fatalf("len: got %d, want 3", len(got))
	}
	expected := []struct {
		query string
		count int
	}{
		{"a", 5},
		{"c", 4},
		{"b", 3},
	}
	for i, e := range expected {
		if got[i].Query != e.query || got[i].Count != e.count {
			t.Errorf("position %d: got %+v, want {%s %d}", i, got[i], e.query, e.count)
		}
	}
}

func TestTopKExceedsLen(t *testing.T) {
	s := New(10)
	s.Add("a")
	s.Add("b")

	got := s.TopK(100)
	if len(got) != 2 {
		t.Errorf("len: got %d, want 2", len(got))
	}
}

func TestTopKZeroOrNegative(t *testing.T) {
	s := New(10)
	s.Add("a")

	if got := s.TopK(0); len(got) != 0 {
		t.Errorf("TopK(0): got %v, want empty", got)
	}
	if got := s.TopK(-1); len(got) != 0 {
		t.Errorf("TopK(-1): got %v, want empty", got)
	}
}

func TestCapacityRespected(t *testing.T) {
	s := New(3)
	queries := []string{"a", "b", "c", "d", "e", "f"}
	for _, q := range queries {
		s.Add(q)
	}

	if s.Len() != 3 {
		t.Errorf("Len: got %d, want 3 (capacity)", s.Len())
	}
}

func TestEvictionPreservesFrequent(t *testing.T) {
	s := New(3)

	for i := 0; i < 100; i++ {
		s.Add("hot")
	}
	for _, q := range []string{"x1", "x2", "x3", "x4", "x5", "x6", "x7"} {
		s.Add(q)
	}

	top := s.TopK(1)
	if len(top) != 1 || top[0].Query != "hot" {
		t.Errorf("hot should survive: got %+v", top)
	}
	if top[0].Count != 100 {
		t.Errorf("hot count: got %d, want 100", top[0].Count)
	}
}

func TestEvictionInheritsCount(t *testing.T) {
	s := New(2)

	s.Add("a")
	s.Add("a")
	s.Add("a")
	s.Add("b")
	s.Add("b")
	s.Add("c")

	top := s.TopK(2)

	var entryC Entry
	found := false
	for _, e := range top {
		if e.Query == "c" {
			entryC = e
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("c should be in top: got %+v", top)
	}

	if entryC.Count != 3 {
		t.Errorf("c count: got %d, want 3 (min+1 = 2+1)", entryC.Count)
	}
	if entryC.Error != 2 {
		t.Errorf("c errVal: got %d, want 2 (inherited from victim)", entryC.Error)
	}
}

func TestGuarantee_FrequentAlwaysPresent(t *testing.T) {
	const (
		k    = 100
		n    = 100_000
		hotN = 10
	)
	s := New(k)

	hots := make([]string, hotN)
	for i := range hots {
		hots[i] = fmt.Sprintf("hot-%d", i)
	}

	rng := rand.New(rand.NewSource(42))
	hotShare := 0.5
	hotEventsPerQuery := int(float64(n) * hotShare / float64(hotN))
	hotEvents := hotN * hotEventsPerQuery
	coldEvents := n - hotEvents

	stream := make([]string, 0, n)
	for _, h := range hots {
		for i := 0; i < hotEventsPerQuery; i++ {
			stream = append(stream, h)
		}
	}
	for i := 0; i < coldEvents; i++ {
		stream = append(stream, fmt.Sprintf("cold-%d", rng.Intn(coldEvents)))
	}
	rng.Shuffle(len(stream), func(i, j int) { stream[i], stream[j] = stream[j], stream[i] })

	for _, q := range stream {
		s.Add(q)
	}

	top := s.TopK(k)
	present := make(map[string]bool, k)
	for _, e := range top {
		present[e.Query] = true
	}
	for _, h := range hots {
		if !present[h] {
			t.Errorf("hot query %q (freq=%d > N/k=%d) must be in topK",
				h, hotEventsPerQuery, n/k)
		}
	}
}

func TestErrorBound(t *testing.T) {
	s := New(2)

	s.Add("a")
	s.Add("a")
	s.Add("a")
	s.Add("b")
	s.Add("b")
	s.Add("c")
	s.Add("c")
	s.Add("c")
	s.Add("c")
	s.Add("c")

	for _, e := range s.TopK(10) {
		if e.Count < e.Error {
			t.Errorf("%q: count=%d < errVal=%d (invariant broken)",
				e.Query, e.Count, e.Error)
		}
	}
}

func TestMerge(t *testing.T) {
	a := New(10)
	for i := 0; i < 3; i++ {
		a.Add("x")
	}
	a.Add("y")

	b := New(10)
	for i := 0; i < 2; i++ {
		b.Add("x")
	}
	for i := 0; i < 5; i++ {
		b.Add("z")
	}

	a.Merge(b)

	top := a.TopK(10)
	counts := make(map[string]int, len(top))
	for _, e := range top {
		counts[e.Query] = e.Count
	}
	if counts["x"] != 5 {
		t.Errorf("x: got %d, want 5", counts["x"])
	}
	if counts["y"] != 1 {
		t.Errorf("y: got %d, want 1", counts["y"])
	}
	if counts["z"] != 5 {
		t.Errorf("z: got %d, want 5", counts["z"])
	}
}

func TestMergeNil(t *testing.T) {
	a := New(10)
	a.Add("x")
	a.Merge(nil)
	if a.Len() != 1 {
		t.Errorf("Len after merge nil: got %d, want 1", a.Len())
	}
}

func TestStressRandomStream(t *testing.T) {
	const (
		k         = 50
		uniqueN   = 1000
		streamLen = 50_000
	)
	s := New(k)
	exactCounts := make(map[string]int, uniqueN)
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < streamLen; i++ {
		q := fmt.Sprintf("q-%d", rng.Intn(uniqueN))
		s.Add(q)
		exactCounts[q]++
	}

	type qc struct {
		query string
		count int
	}
	all := make([]qc, 0, len(exactCounts))
	for q, c := range exactCounts {
		all = append(all, qc{q, c})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].count > all[j].count })

	threshold := streamLen / k
	got := s.TopK(k)
	gotSet := make(map[string]bool, k)
	for _, e := range got {
		gotSet[e.Query] = true
	}
	for _, qc := range all {
		if qc.count > threshold && !gotSet[qc.query] {
			t.Errorf("guarantee violated: %q count=%d > N/k=%d not in top",
				qc.query, qc.count, threshold)
		}
	}
}

func BenchmarkAdd_Existing(b *testing.B) {
	s := New(10_000)
	s.Add("hot")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add("hot")
	}
}

func BenchmarkAdd_NewWithCapacity(b *testing.B) {
	s := New(b.N + 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add(fmt.Sprintf("q-%d", i))
	}
}

func BenchmarkAdd_Eviction(b *testing.B) {
	const k = 1000
	s := New(k)
	for i := 0; i < k; i++ {
		s.Add(fmt.Sprintf("seed-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add(fmt.Sprintf("new-%d", i))
	}
}

func BenchmarkAdd_Mixed_Zipf(b *testing.B) {
	s := New(10_000)
	rng := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rng, 1.2, 1.0, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := fmt.Sprintf("q-%d", zipf.Uint64())
		s.Add(q)
	}
}
