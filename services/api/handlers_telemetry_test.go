package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuildRangeClausePresets(t *testing.T) {
	cases := map[string]string{
		"":    "start: -5m",
		"5m":  "start: -5m",
		"1h":  "start: -1h",
		"24h": "start: -24h",
	}
	for rangeParam, want := range cases {
		r := httptest.NewRequest("GET", "/?range="+rangeParam, nil)
		got, err := buildRangeClause(r)
		if err != nil {
			t.Fatalf("range=%q: unexpected error: %v", rangeParam, err)
		}
		if got != want {
			t.Errorf("range=%q: got %q, want %q", rangeParam, got, want)
		}
	}
}

func TestBuildRangeClauseRejectsUnknownPreset(t *testing.T) {
	r := httptest.NewRequest("GET", "/?range=3days", nil)
	if _, err := buildRangeClause(r); err == nil {
		t.Fatal("expected an unrecognized range preset to be rejected")
	}
}

func TestBuildRangeClauseCustomStartEnd(t *testing.T) {
	r := httptest.NewRequest("GET", "/?start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z", nil)
	got, err := buildRangeClause(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(got, "start: 2026-01-01T00:00:00Z") || !strings.Contains(got, "stop: 2026-01-02T00:00:00Z") {
		t.Errorf("got %q, want both start and stop present", got)
	}
}

func TestBuildRangeClauseCustomStartOnly(t *testing.T) {
	r := httptest.NewRequest("GET", "/?start=2026-01-01T00:00:00Z", nil)
	got, err := buildRangeClause(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "start: 2026-01-01T00:00:00Z" {
		t.Errorf("got %q", got)
	}
}

func TestBuildRangeClauseRejectsMalformedTimestamps(t *testing.T) {
	cases := []string{
		"/?start=not-a-timestamp",
		"/?start=2026-01-01T00:00:00Z&end=not-a-timestamp",
	}
	for _, url := range cases {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := buildRangeClause(r); err == nil {
			t.Errorf("url=%q: expected malformed timestamp to be rejected", url)
		}
	}
}
