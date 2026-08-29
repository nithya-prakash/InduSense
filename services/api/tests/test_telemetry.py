import pytest
from starlette.requests import Request

from handlers_telemetry import _build_range_clause


def _make_request(query_string: str) -> Request:
    scope = {
        "type": "http", "method": "GET", "path": "/",
        "query_string": query_string.encode(), "headers": [], "client": ("127.0.0.1", 1),
    }
    return Request(scope)


@pytest.mark.parametrize("range_param,want", [
    ("", "start: -5m"),
    ("5m", "start: -5m"),
    ("1h", "start: -1h"),
    ("24h", "start: -24h"),
])
def test_build_range_clause_presets(range_param, want):
    got = _build_range_clause(_make_request(f"range={range_param}"))
    assert got == want


def test_build_range_clause_rejects_unknown_preset():
    with pytest.raises(ValueError):
        _build_range_clause(_make_request("range=3days"))


def test_build_range_clause_custom_start_end():
    got = _build_range_clause(_make_request("start=2026-01-01T00:00:00Z&end=2026-01-02T00:00:00Z"))
    assert "start: 2026-01-01T00:00:00Z" in got
    assert "stop: 2026-01-02T00:00:00Z" in got


def test_build_range_clause_custom_start_only():
    got = _build_range_clause(_make_request("start=2026-01-01T00:00:00Z"))
    assert got == "start: 2026-01-01T00:00:00Z"


@pytest.mark.parametrize("query_string", [
    "start=not-a-timestamp",
    "start=2026-01-01T00:00:00Z&end=not-a-timestamp",
])
def test_build_range_clause_rejects_malformed_timestamps(query_string):
    with pytest.raises(ValueError):
        _build_range_clause(_make_request(query_string))
