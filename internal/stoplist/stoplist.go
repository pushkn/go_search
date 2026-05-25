package stoplist

import (
	"sort"
	"strings"
	"sync"

	"github.com/pushkn/go_search/internal/metrics"
)

type StopList struct {
	mu    sync.RWMutex
	words map[string]struct{}
}

func New() *StopList {
	return &StopList{words: make(map[string]struct{})}
}

func (s *StopList) Add(word string) {
	word = normalize(word)
	if word == "" {
		return
	}
	s.mu.Lock()
	s.words[word] = struct{}{}
	size := len(s.words)
	s.mu.Unlock()
	metrics.StopListSize.Set(float64(size))
}

func (s *StopList) Remove(word string) bool {
	word = normalize(word)
	if word == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.words[word]; !ok {
		return false
	}
	delete(s.words, word)
	metrics.StopListSize.Set(float64(len(s.words)))
	return true
}
func (s *StopList) Contains(word string) bool {
	s.mu.RLock()
	_, ok := s.words[word]
	s.mu.RUnlock()
	return ok
}

func (s *StopList) List() []string {
	s.mu.RLock()
	out := make([]string, 0, len(s.words))
	for w := range s.words {
		out = append(out, w)
	}
	s.mu.RUnlock()
	sort.Strings(out)
	return out
}

func (s *StopList) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.words)
}

func normalize(w string) string {
	return strings.ToLower(strings.TrimSpace(w))
}
