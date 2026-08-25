package reliability

import (
	"testing"
	"time"
)

func TestCircuitBreakerStartsClosed(t *testing.T) {
	b := NewCircuitBreaker(3, time.Second)
	if b.State() != "CLOSED" {
		t.Fatalf("expected CLOSED, got %s", b.State())
	}
	if !b.Allow() {
		t.Fatal("expected CLOSED breaker to allow calls")
	}
}

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	b := NewCircuitBreaker(3, time.Second)
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != "CLOSED" {
		t.Fatalf("expected still CLOSED after 2/3 failures, got %s", b.State())
	}
	b.RecordFailure()
	if b.State() != "OPEN" {
		t.Fatalf("expected OPEN after 3 consecutive failures, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("expected OPEN breaker to reject calls before cooldown elapses")
	}
}

func TestCircuitBreakerTransitionsToHalfOpenAfterCooldown(t *testing.T) {
	fakeNow := time.Now()
	b := NewCircuitBreaker(1, time.Second)
	b.now = func() time.Time { return fakeNow }

	b.RecordFailure() // -> OPEN
	if b.State() != "OPEN" {
		t.Fatalf("expected OPEN, got %s", b.State())
	}

	fakeNow = fakeNow.Add(2 * time.Second) // advance past cooldown
	if !b.Allow() {
		t.Fatal("expected breaker to allow one trial call after cooldown")
	}
	if b.State() != "HALF_OPEN" {
		t.Fatalf("expected HALF_OPEN after cooldown trial granted, got %s", b.State())
	}
	if b.Allow() {
		t.Fatal("expected a second concurrent call to be rejected while a trial is in flight")
	}
}

func TestCircuitBreakerHalfOpenSuccessCloses(t *testing.T) {
	fakeNow := time.Now()
	b := NewCircuitBreaker(1, time.Second)
	b.now = func() time.Time { return fakeNow }

	b.RecordFailure()
	fakeNow = fakeNow.Add(2 * time.Second)
	b.Allow() // -> HALF_OPEN
	b.RecordSuccess()

	if b.State() != "CLOSED" {
		t.Fatalf("expected CLOSED after successful trial, got %s", b.State())
	}
}

func TestCircuitBreakerHalfOpenFailureReopens(t *testing.T) {
	fakeNow := time.Now()
	b := NewCircuitBreaker(1, time.Second)
	b.now = func() time.Time { return fakeNow }

	b.RecordFailure()
	fakeNow = fakeNow.Add(2 * time.Second)
	b.Allow() // -> HALF_OPEN
	b.RecordFailure()

	if b.State() != "OPEN" {
		t.Fatalf("expected OPEN after failed trial, got %s", b.State())
	}
}
