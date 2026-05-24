package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.builder.Ready() {
		writeError(w, http.StatusServiceUnavailable, "snapshot not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleTrending(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	all := s.builder.Get()
	if limit > len(all) {
		limit = len(all)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": all[:limit]})
}

func (s *Server) handleStopListGet(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"data": s.stoplist.List()})
}

type stopListReq struct {
	Word string `json:"word"`
}

func (s *Server) handleStopListAdd(w http.ResponseWriter, r *http.Request) {
	var req stopListReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if strings.TrimSpace(req.Word) == "" {
		writeError(w, http.StatusBadRequest, "word required")
		return
	}
	s.stoplist.Add(req.Word)
	writeJSON(w, http.StatusCreated, map[string]string{"status": "added"})
}

func (s *Server) handleStopListDelete(w http.ResponseWriter, r *http.Request) {
	word := chi.URLParam(r, "word")
	if word == "" {
		writeError(w, http.StatusBadRequest, "word required")
		return
	}
	if !s.stoplist.Remove(word) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func parseLimit(s string) (int, error) {
	if s == "" {
		return defaultLimit, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, errors.New("limit must be a number")
	}
	if n < 1 {
		return 0, errors.New("limit must be >= 1")
	}
	if n > maxLimit {
		return 0, errors.New("limit too large")
	}
	return n, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
