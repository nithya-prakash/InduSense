"""Retry-with-backoff and circuit-breaker primitives shared by every service
that calls an external dependency (Kafka, InfluxDB, Postgres, a
notification provider) where failures are often correlated (a broker outage
fails every in-flight call at once).

This is a direct port of pkg/reliability — the same CLOSED/OPEN/HALF_OPEN
state machine and retry logic, not swapped for a third-party library like
`tenacity`, so the design stays exactly as it was and is easy to explain.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from enum import Enum


class ErrPermanent(Exception):
    """Wraps an error that retrying cannot fix. retry_with_backoff stops
    immediately on this error instead of burning through its attempt
    budget on something that will never succeed (e.g. a JSON marshal
    failure — no amount of retrying fixes malformed data)."""


def retry_with_backoff(
    max_attempts: int,
    base_delay_seconds: float,
    fn: Callable[[], None],
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Calls fn up to max_attempts times, doubling the delay between
    attempts starting at base_delay_seconds (1s, 2s, 4s, 8s, 16s, ...).
    Stops early on an ErrPermanent. Raises the last error if every attempt
    fails.
    """
    delay = base_delay_seconds
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            fn()
            return
        except ErrPermanent:
            raise
        except Exception as exc:  # noqa: BLE001 - deliberately broad, mirrors Go's plain `error`
            last_error = exc
            if attempt == max_attempts:
                break
            sleep(delay)
            delay *= 2

    assert last_error is not None
    raise last_error


class _BreakerState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Short-circuits calls to a struggling dependency instead of retrying
    each one individually, which would just add load to an already-failing
    broker/database. Not useful for independent, uncorrelated failures
    (e.g. a single message failing validation) since there's nothing to
    "trip" — the breaker earns its keep on dependency-wide outages only.
    """

    def __init__(self, threshold: int, cooldown_seconds: float):
        self._lock = threading.Lock()
        self._state = _BreakerState.CLOSED
        self._consecutive_fails = 0
        self._threshold = threshold
        self._cooldown_seconds = cooldown_seconds
        self._opened_at: float = 0.0
        self._now: Callable[[], float] = time.monotonic

    def allow(self) -> bool:
        """Reports whether a call may proceed. When open but the cooldown
        has elapsed, transitions to half-open and allows exactly one trial
        call."""
        with self._lock:
            if self._state == _BreakerState.CLOSED:
                return True
            if self._state == _BreakerState.HALF_OPEN:
                return False  # a trial call is already in flight
            # OPEN
            if self._now() - self._opened_at >= self._cooldown_seconds:
                self._state = _BreakerState.HALF_OPEN
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            self._consecutive_fails = 0
            self._state = _BreakerState.CLOSED

    def record_failure(self) -> None:
        with self._lock:
            if self._state == _BreakerState.HALF_OPEN:
                self._state = _BreakerState.OPEN
                self._opened_at = self._now()
                return

            self._consecutive_fails += 1
            if self._consecutive_fails >= self._threshold:
                self._state = _BreakerState.OPEN
                self._opened_at = self._now()

    def state(self) -> str:
        with self._lock:
            return self._state.value

    def set_now_func(self, now: Callable[[], float]) -> None:
        """Overrides the breaker's clock, for deterministic tests."""
        with self._lock:
            self._now = now
