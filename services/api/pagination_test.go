package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseLimitOffsetDefaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	limit, offset := parseLimitOffset(r)
	if limit != defaultLimit || offset != 0 {
		t.Errorf("got limit=%d offset=%d, want %d/0", limit, offset, defaultLimit)
	}
}

func TestParseLimitOffsetClampsToMax(t *testing.T) {
	r := httptest.NewRequest("GET", "/?limit=99999", nil)
	limit, _ := parseLimitOffset(r)
	if limit != maxLimit {
		t.Errorf("got limit=%d, want it clamped to %d", limit, maxLimit)
	}
}

func TestParseLimitOffsetIgnoresInvalidValues(t *testing.T) {
	r := httptest.NewRequest("GET", "/?limit=-5&offset=-1", nil)
	limit, offset := parseLimitOffset(r)
	if limit != defaultLimit {
		t.Errorf("negative limit should fall back to default, got %d", limit)
	}
	if offset != 0 {
		t.Errorf("negative offset should fall back to 0, got %d", offset)
	}
}

func TestParseLimitOffsetRespectsValidValues(t *testing.T) {
	r := httptest.NewRequest("GET", "/?limit=5&offset=10", nil)
	limit, offset := parseLimitOffset(r)
	if limit != 5 || offset != 10 {
		t.Errorf("got limit=%d offset=%d, want 5/10", limit, offset)
	}
}
