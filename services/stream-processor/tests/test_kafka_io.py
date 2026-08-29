import time

import pytest

from kafka_io import KafkaIO, KafkaPublishError
from shared.reliability import CircuitBreaker


def new_test_kafka_io(threshold: int, cooldown_seconds: float, max_retries: int) -> KafkaIO:
    """Builds a KafkaIO with just the retry/breaker fields protected_write
    needs, decoupled from any real Consumer/Producer — the point of
    factoring protected_write out (see kafka_io.py) is to make this wiring
    testable without a broker. Mirrors kafka_test.go's newTestKafkaIO."""
    kio = KafkaIO.__new__(KafkaIO)
    kio._breaker = CircuitBreaker(threshold, cooldown_seconds)
    kio._max_retries = max_retries
    kio._retry_delay = 0.001
    return kio


def test_protected_write_transient_failure_recovers_within_retry_budget():
    kio = new_test_kafka_io(3, 1.0, 5)
    calls = 0

    def write():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient broker hiccup")

    kio.protected_write("test-topic", write)
    assert calls == 3
    assert kio.breaker_state() == "CLOSED"


def test_protected_write_retry_exhaustion_returns_error_and_records_failure():
    kio = new_test_kafka_io(5, 1.0, 3)  # threshold 5 > maxRetries 3: one call alone can't open it
    calls = 0

    def write():
        nonlocal calls
        calls += 1
        raise RuntimeError("broker unreachable")

    with pytest.raises(RuntimeError):
        kio.protected_write("test-topic", write)
    assert calls == 3
    assert kio.breaker_state() == "CLOSED"


def test_protected_write_breaker_opens_and_short_circuits_without_calling_write():
    kio = new_test_kafka_io(2, 3600.0, 1)  # threshold 2, maxRetries 1: each call is exactly one breaker failure

    def failing_write():
        raise RuntimeError("broker unreachable")

    with pytest.raises(RuntimeError):
        kio.protected_write("test-topic", failing_write)
    assert kio.breaker_state() == "CLOSED"

    with pytest.raises(RuntimeError):
        kio.protected_write("test-topic", failing_write)
    assert kio.breaker_state() == "OPEN"

    calls = 0

    def would_succeed():
        nonlocal calls
        calls += 1

    with pytest.raises(KafkaPublishError):
        kio.protected_write("test-topic", would_succeed)
    assert calls == 0


def test_protected_write_half_open_recovery_closes_breaker_on_success():
    fake_now = [time.monotonic()]
    kio = new_test_kafka_io(1, 1.0, 1)  # threshold 1: a single failure opens it
    kio._breaker.set_now_func(lambda: fake_now[0])

    def failing_write():
        raise RuntimeError("broker unreachable")

    with pytest.raises(RuntimeError):
        kio.protected_write("test-topic", failing_write)
    assert kio.breaker_state() == "OPEN"

    # Still within cooldown: must stay rejected.
    with pytest.raises(KafkaPublishError):
        kio.protected_write("test-topic", lambda: None)

    # Advance past cooldown and let the trial call succeed.
    fake_now[0] += 2.0
    calls = 0

    def trial():
        nonlocal calls
        calls += 1

    kio.protected_write("test-topic", trial)
    assert calls == 1
    assert kio.breaker_state() == "CLOSED"
