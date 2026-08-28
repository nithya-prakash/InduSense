package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nithya-prakash/indusense/pkg/reliability"
)

// newTestKafkaIO builds a kafkaIO with just the retry/breaker fields
// protectedWrite needs, decoupled from any real *kafka.Writer/Reader — the
// point of factoring protectedWrite out (see kafka.go) is to make this
// wiring testable without a broker.
func newTestKafkaIO(threshold int, cooldown time.Duration, maxRetries int) *kafkaIO {
	return &kafkaIO{
		breaker:    reliability.NewCircuitBreaker(threshold, cooldown),
		maxRetries: maxRetries,
		retryDelay: time.Millisecond,
	}
}

func TestProtectedWrite_TransientFailureRecoversWithinRetryBudget(t *testing.T) {
	k := newTestKafkaIO(3, time.Second, 5)
	calls := 0
	err := k.protectedWrite(context.Background(), "test-topic", func() error {
		calls++
		if calls < 3 {
			return errors.New("transient broker hiccup")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected eventual success within the retry budget, got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 attempts, got %d", calls)
	}
	if k.breakerState() != "CLOSED" {
		t.Errorf("expected breaker to stay CLOSED after an eventual success, got %s", k.breakerState())
	}
}

func TestProtectedWrite_RetryExhaustionReturnsErrorAndRecordsFailure(t *testing.T) {
	k := newTestKafkaIO(5, time.Second, 3) // threshold 5 > maxRetries 3: one call alone can't open it
	calls := 0
	err := k.protectedWrite(context.Background(), "test-topic", func() error {
		calls++
		return errors.New("broker unreachable")
	})
	if err == nil {
		t.Fatal("expected an error after exhausting the retry budget")
	}
	if calls != 3 {
		t.Errorf("expected exactly maxRetries=3 attempts, got %d", calls)
	}
	if k.breakerState() != "CLOSED" {
		t.Errorf("expected breaker still CLOSED after 1 of 5 allowed failures, got %s", k.breakerState())
	}
}

func TestProtectedWrite_BreakerOpensAndShortCircuitsWithoutCallingWrite(t *testing.T) {
	k := newTestKafkaIO(2, time.Hour, 1) // threshold 2, maxRetries 1: each protectedWrite call is exactly one breaker failure
	failingWrite := func() error { return errors.New("broker unreachable") }

	if err := k.protectedWrite(context.Background(), "test-topic", failingWrite); err == nil {
		t.Fatal("expected the 1st failing call to return an error")
	}
	if k.breakerState() != "CLOSED" {
		t.Fatalf("expected CLOSED after 1/2 failures, got %s", k.breakerState())
	}

	if err := k.protectedWrite(context.Background(), "test-topic", failingWrite); err == nil {
		t.Fatal("expected the 2nd failing call to return an error")
	}
	if k.breakerState() != "OPEN" {
		t.Fatalf("expected OPEN after 2/2 failures (threshold reached), got %s", k.breakerState())
	}

	calls := 0
	err := k.protectedWrite(context.Background(), "test-topic", func() error {
		calls++
		return nil // would succeed if ever actually called
	})
	if err == nil {
		t.Fatal("expected an OPEN breaker to reject the call outright")
	}
	if calls != 0 {
		t.Errorf("expected the write function to never be called while the breaker is OPEN, got %d calls", calls)
	}
}

func TestProtectedWrite_HalfOpenRecoveryClosesBreakerOnSuccess(t *testing.T) {
	fakeNow := time.Now()
	k := newTestKafkaIO(1, time.Second, 1) // threshold 1: a single failure opens it
	k.breaker.SetNowFunc(func() time.Time { return fakeNow })

	if err := k.protectedWrite(context.Background(), "test-topic", func() error {
		return errors.New("broker unreachable")
	}); err == nil {
		t.Fatal("expected the failing call to return an error")
	}
	if k.breakerState() != "OPEN" {
		t.Fatalf("expected OPEN after the single allowed failure, got %s", k.breakerState())
	}

	// Still within cooldown: must stay rejected.
	if err := k.protectedWrite(context.Background(), "test-topic", func() error { return nil }); err == nil {
		t.Fatal("expected the breaker to still reject calls before cooldown elapses")
	}

	// Advance past cooldown and let the trial call succeed.
	fakeNow = fakeNow.Add(2 * time.Second)
	calls := 0
	if err := k.protectedWrite(context.Background(), "test-topic", func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("expected the half-open trial call to succeed, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected exactly 1 trial call once half-open, got %d", calls)
	}
	if k.breakerState() != "CLOSED" {
		t.Errorf("expected breaker to close again after the trial call succeeded, got %s", k.breakerState())
	}
}
