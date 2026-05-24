package stoplist

import (
	"fmt"
	"sync"
	"testing"
)

func TestEmpty(t *testing.T) {
	s := New()
	if s.Contains("anything") {
		t.Error("empty should not contain")
	}
	if s.Size() != 0 {
		t.Error("size should be 0")
	}
	if len(s.List()) != 0 {
		t.Error("list should be empty")
	}
}

func TestAddContains(t *testing.T) {
	s := New()
	s.Add("реклама")
	if !s.Contains("реклама") {
		t.Error("added word should be present")
	}
	if s.Contains("other") {
		t.Error("non-added word should be absent")
	}
}

func TestAddNormalizes(t *testing.T) {
	s := New()
	s.Add("  ReKlAmA  ")
	if !s.Contains("reklama") {
		t.Error("not normalized")
	}
	s.Add("")
	s.Add("   ")
	if s.Size() != 1 {
		t.Error("empty ignored")
	}
}

func TestRemove(t *testing.T) {
	s := New()
	s.Add("x")
	if !s.Remove("x") {
		t.Error("removing existing returns true")
	}
	if s.Contains("x") {
		t.Error("removed should be gone")
	}
	if s.Remove("x") {
		t.Error("removing absent returns false")
	}
}

func TestListSorted(t *testing.T) {
	s := New()
	s.Add("c")
	s.Add("a")
	s.Add("b")
	got := s.List()
	want := []string{"a", "b", "c"}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("position %d: got %s, want %s", i, got[i], w)
		}
	}
}

func TestConcurrent(t *testing.T) {
	s := New()
	const workers = 10
	const perWorker = 100
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				word := fmt.Sprintf("w%d-%d", id, i)
				s.Add(word)
				s.Contains(word)
				s.List()
			}
		}(w)
	}
	wg.Wait()

	if s.Size() != workers*perWorker {
		t.Errorf("size: got %d, want %d", s.Size(), workers*perWorker)
	}
}

func BenchmarkContains(b *testing.B) {
	s := New()
	for i := 0; i < 100; i++ {
		s.Add(fmt.Sprintf("word-%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Contains("word-50")
	}
}
