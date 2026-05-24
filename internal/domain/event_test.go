package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name  string
		input SearchEvent
		want  SearchEvent
	}{
		{
			name:  "lowercase",
			input: SearchEvent{Query: "IPHONE 15 PRO"},
			want:  SearchEvent{Query: "iphone 15 pro"},
		},
		{
			name:  "trim spaces",
			input: SearchEvent{Query: "   iphone   "},
			want:  SearchEvent{Query: "iphone"},
		},
		{
			name:  "collapse multiple spaces",
			input: SearchEvent{Query: "iphone    15    pro"},
			want:  SearchEvent{Query: "iphone 15 pro"},
		},
		{
			name:  "tabs and newlines as spaces",
			input: SearchEvent{Query: "iphone\t15\npro"},
			want:  SearchEvent{Query: "iphone 15 pro"},
		},
		{
			name:  "russian preserved",
			input: SearchEvent{Query: "  АЙФОН 15  "},
			want:  SearchEvent{Query: "айфон 15"},
		},
		{
			name:  "trim user_id and session_id",
			input: SearchEvent{Query: "x", UserID: " u1 ", SessionID: " s1 "},
			want:  SearchEvent{Query: "x", UserID: "u1", SessionID: "s1"},
		},
		{
			name:  "empty query stays empty",
			input: SearchEvent{Query: "   "},
			want:  SearchEvent{Query: ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input
			got.Normalize()
			if got.Query != tt.want.Query {
				t.Errorf("Query: got %q, want %q", got.Query, tt.want.Query)
			}
			if got.UserID != tt.want.UserID {
				t.Errorf("UserID: got %q, want %q", got.UserID, tt.want.UserID)
			}
			if got.SessionID != tt.want.SessionID {
				t.Errorf("SessionID: got %q, want %q", got.SessionID, tt.want.SessionID)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	now := time.Date(2026, 5, 24, 12, 0, 0, 0, time.UTC)

	validEvent := func() SearchEvent {
		return SearchEvent{
			EventID:   "evt-1",
			Query:     "iphone",
			UserID:    "u1",
			SessionID: "s1",
			Timestamp: now.Add(-1 * time.Second),
		}
	}

	tests := []struct {
		name    string
		mutate  func(*SearchEvent)
		wantErr error
	}{
		{
			name:    "valid event passes",
			mutate:  func(e *SearchEvent) {},
			wantErr: nil,
		},
		{
			name:    "empty event_id",
			mutate:  func(e *SearchEvent) { e.EventID = "" },
			wantErr: ErrEmptyEventID,
		},
		{
			name:    "empty query",
			mutate:  func(e *SearchEvent) { e.Query = "" },
			wantErr: ErrEmptyQuery,
		},
		{
			name:    "query too long",
			mutate:  func(e *SearchEvent) { e.Query = strings.Repeat("a", MaxQueryLength+1) },
			wantErr: ErrQueryTooLong,
		},
		{
			name:    "query at max length is ok",
			mutate:  func(e *SearchEvent) { e.Query = strings.Repeat("a", MaxQueryLength) },
			wantErr: nil,
		},
		{
			name:    "invalid utf8",
			mutate:  func(e *SearchEvent) { e.Query = string([]byte{0xff, 0xfe, 0xfd}) },
			wantErr: ErrInvalidUTF8,
		},
		{
			name:    "russian query is valid utf8",
			mutate:  func(e *SearchEvent) { e.Query = "айфон" },
			wantErr: nil,
		},
		{
			name:    "no user_id and no session_id",
			mutate:  func(e *SearchEvent) { e.UserID = ""; e.SessionID = "" },
			wantErr: ErrNoIdentifier,
		},
		{
			name:    "only session_id is ok",
			mutate:  func(e *SearchEvent) { e.UserID = "" },
			wantErr: nil,
		},
		{
			name:    "only user_id is ok",
			mutate:  func(e *SearchEvent) { e.SessionID = "" },
			wantErr: nil,
		},
		{
			name:    "future timestamp",
			mutate:  func(e *SearchEvent) { e.Timestamp = now.Add(1 * time.Second) },
			wantErr: ErrFutureTime,
		},
		{
			name:    "stale event",
			mutate:  func(e *SearchEvent) { e.Timestamp = now.Add(-6 * time.Minute) },
			wantErr: ErrStaleEvent,
		},
		{
			name:    "exactly at stale boundary is ok",
			mutate:  func(e *SearchEvent) { e.Timestamp = now.Add(-MaxStaleAge) },
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEvent()
			tt.mutate(&e)
			err := e.Validate(now)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got error %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestIdentifier(t *testing.T) {
	tests := []struct {
		name string
		e    SearchEvent
		want string
	}{
		{"user_id has priority", SearchEvent{UserID: "u1", SessionID: "s1"}, "u:u1"},
		{"only session_id", SearchEvent{SessionID: "s1"}, "s:s1"},
		{"only user_id", SearchEvent{UserID: "u1"}, "u:u1"},
		{"different user and session with same value", SearchEvent{UserID: "abc"}, "u:abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Identifier(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
