package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxQueryLength = 256
	MaxStaleAge    = 5 * time.Minute
)

var (
	ErrEmptyQuery   = errors.New("query is empty")
	ErrQueryTooLong = errors.New("query exceeds max length")
	ErrInvalidUTF8  = errors.New("query is not valid utf-8")
	ErrEmptyEventID = errors.New("event_id is empty")
	ErrFutureTime   = errors.New("timestamp is in the future")
	ErrStaleEvent   = errors.New("event is too old")
	ErrNoIdentifier = errors.New("both user_id and session_id are empty")
)

type SearchEvent struct {
	EventID   string    `json:"event_id"`
	Query     string    `json:"query"`
	UserID    string    `json:"user_id"`
	SessionID string    `json:"session_id"`
	Timestamp time.Time `json:"timestamp"`
}

func (e *SearchEvent) Normalize() {
	e.Query = normalizeQuery(e.Query)
	e.UserID = strings.TrimSpace(e.UserID)
	e.SessionID = strings.TrimSpace(e.SessionID)
}

func (e *SearchEvent) Validate(now time.Time) error {
	if e.EventID == "" {
		return ErrEmptyEventID
	}
	if e.Query == "" {
		return ErrEmptyQuery
	}
	if !utf8.ValidString(e.Query) {
		return ErrInvalidUTF8
	}
	if utf8.RuneCountInString(e.Query) > MaxQueryLength {
		return ErrQueryTooLong
	}
	if e.UserID == "" && e.SessionID == "" {
		return ErrNoIdentifier
	}
	if e.Timestamp.After(now) {
		return ErrFutureTime
	}
	if now.Sub(e.Timestamp) > MaxStaleAge {
		return ErrStaleEvent
	}
	return nil
}

func (e *SearchEvent) Identifier() string {
	if e.UserID != "" {
		return "u:" + e.UserID
	}
	return "s:" + e.SessionID
}

func normalizeQuery(q string) string {
	q = strings.ToLower(q)
	q = strings.TrimSpace(q)
	if q == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(q))
	prevSpace := false
	for _, r := range q {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteRune(r)
		prevSpace = false
	}
	return b.String()
}
