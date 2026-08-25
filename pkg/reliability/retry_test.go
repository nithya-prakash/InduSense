package reliability

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithBackoffSucceedsWithoutRetrying(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), 5, time.Millisecond, func(time.Duration) {}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 call, got %d", calls)
	}
}

func TestRetryWithBackoffRetriesTransientFailures(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), 3, time.Millisecond, func(time.Duration) {}, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryWithBackoffGivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), 3, time.Millisecond, func(time.Duration) {}, func() error {
		calls++
		return errors.New("always fails")
	})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if calls != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", calls)
	}
}

func TestRetryWithBackoffStopsImmediatelyOnPermanentError(t *testing.T) {
	calls := 0
	err := RetryWithBackoff(context.Background(), 5, time.Millisecond, func(time.Duration) {}, func() error {
		calls++
		return &ErrPermanent{Err: errors.New("schema invalid")}
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected permanent error to stop after 1 attempt, got %d calls", calls)
	}
}

func TestRetryWithBackoffUsesDoublingDelay(t *testing.T) {
	var delays []time.Duration
	_ = RetryWithBackoff(context.Background(), 4, time.Second, func(d time.Duration) {
		delays = append(delays, d)
	}, func() error {
		return errors.New("fail")
	})
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	if len(delays) != len(want) {
		t.Fatalf("expected %d sleeps, got %d: %v", len(want), len(delays), delays)
	}
	for i, d := range want {
		if delays[i] != d {
			t.Errorf("delay[%d] = %v, want %v", i, delays[i], d)
		}
	}
}
