package main

import (
	"sync"
	"time"
)

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

// CircuitBreaker protects a downstream dependency (Kafka, in this service)
// from retry storms during an outage: once failures cross the threshold, it
// opens and short-circuits calls immediately for a cooldown period, then
// allows a single trial call through (half-open) to test recovery before
// fully closing again.
//
// It is useful here because Kafka publish failures are correlated (a broker
// outage fails every in-flight publish at once) — retrying each one
// individually would just amplify load on an already-struggling broker. It
// would NOT be useful for, say, a single validation failure, which is
// independent per-message and never "recovers" on its own.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            breakerState
	consecutiveFails int
	threshold        int
	cooldown         time.Duration
	openedAt         time.Time
	trialInFlight    bool
	now              func() time.Time
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		cooldown:  cooldown,
		now:       time.Now,
	}
}

// Allow reports whether a call may proceed. When the breaker is open but the
// cooldown has elapsed, it transitions to half-open and allows exactly one
// trial call through.
func (b *CircuitBreaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case breakerClosed:
		return true
	case breakerHalfOpen:
		return false // a trial call is already in flight
	case breakerOpen:
		if b.now().Sub(b.openedAt) >= b.cooldown {
			b.state = breakerHalfOpen
			b.trialInFlight = true
			return true
		}
		return false
	}
	return false
}

func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFails = 0
	b.trialInFlight = false
	b.state = breakerClosed
}

func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == breakerHalfOpen {
		// The trial call failed: back to open for another full cooldown.
		b.state = breakerOpen
		b.openedAt = b.now()
		b.trialInFlight = false
		return
	}

	b.consecutiveFails++
	if b.consecutiveFails >= b.threshold {
		b.state = breakerOpen
		b.openedAt = b.now()
	}
}

func (b *CircuitBreaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		return "CLOSED"
	case breakerOpen:
		return "OPEN"
	case breakerHalfOpen:
		return "HALF_OPEN"
	}
	return "UNKNOWN"
}
