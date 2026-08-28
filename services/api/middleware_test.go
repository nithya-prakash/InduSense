package main

import (
	"net/http/httptest"
	"testing"
)

// TestClientIPResolver_DefaultConfig_IgnoresForwardedFor proves fix #1 from
// the pre-GitHub audit: with the default (API_TRUST_PROXY_HEADERS=false)
// configuration, X-Forwarded-For is never consulted, so a request that
// sets it to any value is always resolved to the real TCP peer address —
// an attacker changing the header on every request cannot reset their own
// rate-limit bucket.
func TestClientIPResolver_DefaultConfig_IgnoresForwardedFor(t *testing.T) {
	resolver, err := newClientIPResolver(false, nil)
	if err != nil {
		t.Fatalf("newClientIPResolver: %v", err)
	}

	for _, spoofed := range []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "203.0.113.9:54321"
		r.Header.Set("X-Forwarded-For", spoofed)

		got := resolver.resolve(r)
		if got != "203.0.113.9" {
			t.Errorf("with X-Forwarded-For=%q and trust disabled, got %q, want the real peer %q", spoofed, got, "203.0.113.9")
		}
	}
}

// TestClientIPResolver_UntrustedPeer_ForwardedForIgnored proves that even
// with trust enabled, a direct client that isn't itself a configured
// trusted proxy cannot spoof its way past the limiter just by setting the
// header — the peer address is what's checked against the trust list, not
// the header's own claim about itself.
func TestClientIPResolver_UntrustedPeer_ForwardedForIgnored(t *testing.T) {
	resolver, err := newClientIPResolver(true, []string{"10.0.0.1"})
	if err != nil {
		t.Fatalf("newClientIPResolver: %v", err)
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "198.51.100.5:1111" // not in the trusted list
	r.Header.Set("X-Forwarded-For", "9.9.9.9")

	got := resolver.resolve(r)
	if got != "198.51.100.5" {
		t.Errorf("untrusted peer's X-Forwarded-For was honored: got %q, want the real peer %q", got, "198.51.100.5")
	}
}

// TestClientIPResolver_TrustedProxy_UsesForwardedFor proves the positive
// case: a reverse proxy explicitly listed in API_TRUSTED_PROXY_CIDRS has
// its X-Forwarded-For value honored, taking the left-most (original
// client) entry from a multi-hop chain.
func TestClientIPResolver_TrustedProxy_UsesForwardedFor(t *testing.T) {
	resolver, err := newClientIPResolver(true, []string{"10.0.0.1", "172.16.0.0/12"})
	if err != nil {
		t.Fatalf("newClientIPResolver: %v", err)
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:443" // exact trusted proxy address
	r.Header.Set("X-Forwarded-For", "198.51.100.42, 10.0.0.1")

	got := resolver.resolve(r)
	if got != "198.51.100.42" {
		t.Errorf("trusted proxy's X-Forwarded-For was not honored: got %q, want %q", got, "198.51.100.42")
	}

	// A peer inside the trusted CIDR range also counts, not just an exact match.
	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "172.16.5.5:443"
	r2.Header.Set("X-Forwarded-For", "203.0.113.77")
	if got := resolver.resolve(r2); got != "203.0.113.77" {
		t.Errorf("trusted proxy CIDR range was not honored: got %q, want %q", got, "203.0.113.77")
	}
}

// TestClientIPResolver_TrustedProxy_NoForwardedForHeader falls back to the
// peer address cleanly when a trusted proxy forgets to set the header.
func TestClientIPResolver_TrustedProxy_NoForwardedForHeader(t *testing.T) {
	resolver, err := newClientIPResolver(true, []string{"10.0.0.1"})
	if err != nil {
		t.Fatalf("newClientIPResolver: %v", err)
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.1:443"

	if got := resolver.resolve(r); got != "10.0.0.1" {
		t.Errorf("got %q, want fallback to peer %q", got, "10.0.0.1")
	}
}

// TestNewClientIPResolver_RejectsInvalidCIDR ensures a misconfigured
// trusted-proxy list fails fast at startup rather than silently never
// matching at request time.
func TestNewClientIPResolver_RejectsInvalidCIDR(t *testing.T) {
	if _, err := newClientIPResolver(true, []string{"not-an-ip"}); err == nil {
		t.Error("expected an error for an invalid trusted-proxy entry, got nil")
	}
}

// TestClientIPResolver_SameRealIP_ProducesSameKey is the "same real IP
// twice" half of the audit's rate-limit test — the resolver must return a
// stable value for the same peer across requests so the rate limiter's
// window key actually accumulates instead of scattering across buckets.
func TestClientIPResolver_SameRealIP_ProducesSameKey(t *testing.T) {
	resolver, err := newClientIPResolver(false, nil)
	if err != nil {
		t.Fatalf("newClientIPResolver: %v", err)
	}

	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "203.0.113.9:11111"
	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "203.0.113.9:22222" // different ephemeral port, same client

	if resolver.resolve(r1) != resolver.resolve(r2) {
		t.Errorf("same client IP with different source ports resolved differently: %q vs %q", resolver.resolve(r1), resolver.resolve(r2))
	}
}
