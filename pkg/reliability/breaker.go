// Package reliability holds retry-with-backoff and circuit-breaker
// primitives shared by every service that calls an external dependency
// (Kafka, InfluxDB, Postgres, a notification provider) where failures are
// often correlated (a broker outage fails every in-flight call at once).
package reliability

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

// CircuitBreaker short-circuits calls to a struggling dependency instead of
// retrying each one individually, which would just add load to an already
// failing broker/database. It is NOT useful for independent, uncorrelated
// failures (e.g. a single message failing validation) since there's nothing
// to "trip" — the breaker earns its keep on dependency-wide outages only.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            breakerState
	consecutiveFails int
	threshold        int
	cooldown         time.Duration
	openedAt         time.Time
	now              func() time.Time
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		cooldown:  cooldown,
		now:       time.Now,
	}
}

// Allow reports whether a call may proceed. When open but the cooldown has
// elapsed, it transitions to half-open and allows exactly one trial call.
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
	b.state = breakerClosed
}

func (b *CircuitBreaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == breakerHalfOpen {
		b.state = breakerOpen
		b.openedAt = b.now()
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

// SetNowFunc overrides the breaker's clock, for deterministic tests.
func (b *CircuitBreaker) SetNowFunc(now func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.now = now
}
