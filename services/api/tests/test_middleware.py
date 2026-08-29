import pytest
from starlette.requests import Request

from middleware import ClientIPResolver


def _make_request(client_host: str, headers: dict[str, str] | None = None) -> Request:
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "query_string": b"",
        "headers": raw_headers,
        "client": (client_host, 54321),
    }
    return Request(scope)


def test_client_ip_resolver_default_config_ignores_forwarded_for():
    """Proves fix #1 from the pre-GitHub audit: with the default
    (API_TRUST_PROXY_HEADERS=false) configuration, X-Forwarded-For is
    never consulted, so a request that sets it to any value is always
    resolved to the real TCP peer address -- an attacker changing the
    header on every request cannot reset their own rate-limit bucket."""
    resolver = ClientIPResolver(False, [])

    for spoofed in ["1.1.1.1", "2.2.2.2", "3.3.3.3"]:
        r = _make_request("203.0.113.9", {"X-Forwarded-For": spoofed})
        got = resolver.resolve(r)
        assert got == "203.0.113.9", f"with X-Forwarded-For={spoofed!r} and trust disabled, got {got!r}"


def test_client_ip_resolver_untrusted_peer_forwarded_for_ignored():
    """Proves that even with trust enabled, a direct client that isn't
    itself a configured trusted proxy cannot spoof its way past the
    limiter just by setting the header."""
    resolver = ClientIPResolver(True, ["10.0.0.1"])

    r = _make_request("198.51.100.5", {"X-Forwarded-For": "9.9.9.9"})  # not in the trusted list
    got = resolver.resolve(r)
    assert got == "198.51.100.5"


def test_client_ip_resolver_trusted_proxy_uses_forwarded_for():
    """Proves the positive case: a reverse proxy explicitly listed in
    API_TRUSTED_PROXY_CIDRS has its X-Forwarded-For value honored, taking
    the left-most (original client) entry from a multi-hop chain."""
    resolver = ClientIPResolver(True, ["10.0.0.1", "172.16.0.0/12"])

    r = _make_request("10.0.0.1", {"X-Forwarded-For": "198.51.100.42, 10.0.0.1"})  # exact trusted proxy address
    assert resolver.resolve(r) == "198.51.100.42"

    # A peer inside the trusted CIDR range also counts, not just an exact match.
    r2 = _make_request("172.16.5.5", {"X-Forwarded-For": "203.0.113.77"})
    assert resolver.resolve(r2) == "203.0.113.77"


def test_client_ip_resolver_trusted_proxy_no_forwarded_for_header():
    """Falls back to the peer address cleanly when a trusted proxy
    forgets to set the header."""
    resolver = ClientIPResolver(True, ["10.0.0.1"])

    r = _make_request("10.0.0.1")
    assert resolver.resolve(r) == "10.0.0.1"


def test_new_client_ip_resolver_rejects_invalid_cidr():
    """Ensures a misconfigured trusted-proxy list fails fast at startup
    rather than silently never matching at request time."""
    with pytest.raises(ValueError):
        ClientIPResolver(True, ["not-an-ip"])


def test_client_ip_resolver_same_real_ip_produces_same_key():
    """The "same real IP twice" half of the audit's rate-limit test -- the
    resolver must return a stable value for the same peer across requests
    so the rate limiter's window key actually accumulates instead of
    scattering across buckets."""
    resolver = ClientIPResolver(False, [])

    r1 = _make_request("203.0.113.9")
    r2 = _make_request("203.0.113.9")  # different ephemeral port, same client (port isn't part of the resolved value anyway)

    assert resolver.resolve(r1) == resolver.resolve(r2)
