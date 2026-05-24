package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pushkn/go_search/internal/snapshot"
	"github.com/pushkn/go_search/internal/stoplist"
	"github.com/pushkn/go_search/internal/topk"
)

type stubWindow struct{}

func (stubWindow) Snapshot(_ time.Time) *topk.SpaceSaving {
	return topk.New(10)
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	sl := stoplist.New()
	builder := snapshot.NewBuilder(&stubWindow{}, nil, sl, snapshot.Config{
		Interval:       1,
		MaxSize:        100,
		AnomalyPenalty: 0.5,
	})
	builder.BuildOnce()
	return NewServer(Config{Port: "0"}, slog.Default(), builder, sl)
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status: %d", w.Code)
	}
}

func TestStopListFlow(t *testing.T) {
	s := newTestServer(t)
	handler := s.srv.Handler

	body := bytes.NewBufferString(`{"word":"spam"}`)
	req := httptest.NewRequest("POST", "/api/v1/stoplist", body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("add: %d", w.Code)
	}

	req = httptest.NewRequest("GET", "/api/v1/stoplist", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	var resp struct {
		Data []string `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if len(resp.Data) != 1 || resp.Data[0] != "spam" {
		t.Errorf("list: %v", resp.Data)
	}

	req = httptest.NewRequest("DELETE", "/api/v1/stoplist/spam", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("delete: %d", w.Code)
	}
}

func TestStopListAddInvalid(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest("POST", "/api/v1/stoplist", bytes.NewBufferString(`{"word":""}`))
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("empty word: %d", w.Code)
	}
}

func TestStopListDeleteMissing(t *testing.T) {
	s := newTestServer(t)
	req := httptest.NewRequest("DELETE", "/api/v1/stoplist/none", nil)
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("missing: %d", w.Code)
	}
}

func TestTrendingLimitValidation(t *testing.T) {
	s := newTestServer(t)
	cases := []struct {
		url    string
		status int
	}{
		{"/api/v1/trending", http.StatusOK},
		{"/api/v1/trending?limit=10", http.StatusOK},
		{"/api/v1/trending?limit=abc", http.StatusBadRequest},
		{"/api/v1/trending?limit=0", http.StatusBadRequest},
		{"/api/v1/trending?limit=99999", http.StatusBadRequest},
	}
	for _, tc := range cases {
		req := httptest.NewRequest("GET", tc.url, nil)
		w := httptest.NewRecorder()
		s.srv.Handler.ServeHTTP(w, req)
		if w.Code != tc.status {
			t.Errorf("%s: got %d, want %d", tc.url, w.Code, tc.status)
		}
	}
}
