// Package integration exercises the running api service over real HTTP
// against real Postgres/Redis — not a mocked handler test. It targets the
// docker-compose stack's api container (default http://localhost:8080) and
// the demo data scripts/seed puts there: two organizations
// ("musterfabrik-gmbh" and "zweite-firma-gmbh", the latter seeded
// specifically so tenant isolation has something real to fail against) and
// one demo user per role, password "ChangeMe123!".
//
// Every test skips (not fails) if the API isn't reachable, so this package
// degrades gracefully in an environment where `make up` hasn't been run,
// while still catching real regressions whenever it has.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const demoPassword = "ChangeMe123!"

func baseURL() string {
	if v := os.Getenv("API_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func redisAddr() string {
	host := "localhost"
	if v := os.Getenv("REDIS_HOST"); v != "" {
		host = v
	}
	port := "6379"
	if v := os.Getenv("REDIS_PORT"); v != "" {
		port = v
	}
	return host + ":" + port
}

// testRedisClient is a package-level client (set up in TestMain, closed
// after m.Run()) so flushRateLimitBuckets can be called cheaply from every
// login() call without reconnecting each time. Left nil if Redis isn't
// reachable at startup, in which case flushRateLimitBuckets is a no-op and
// individual tests' own live-stack checks (e.g. requireLiveAPI) still
// handle an unreachable environment gracefully.
var testRedisClient *redis.Client

// TestMain flushes this API's rate-limit buckets before any test in this
// package runs, and wires up flushRateLimitBuckets (called from login, see
// below) for the rest of the run. The rate limiter itself is correct and
// untouched here — the problem this works around is test isolation:
// tests/integration and tests/e2e both log in against the real api
// service, which keys its atomic Redis rate limiter by real client IP (see
// clientIPResolver in services/api/middleware.go — trusting a spoofed
// X-Forwarded-For was the actual vulnerability an earlier fix closed, so
// tests can no longer use a fake per-request IP to dodge this the way they
// once did). Without isolation, two consecutive `go test ./...`
// invocations — or, just as easily, two repetitions within one `-count=N`
// invocation, since TestMain and its `m.Run()` only wrap the *whole* run
// once, not each repetition — inherit earlier counters on the same
// 60-second window and see spurious 429s. Not a rate-limiter bug, a
// shared-state one. The flush only ever touches "ratelimit:*" keys
// (self-expiring counters, not business data) on whatever Redis this test
// environment already points at.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	client := redis.NewClient(&redis.Options{Addr: redisAddr()})
	if client.Ping(ctx).Err() == nil {
		testRedisClient = client
	} else {
		client.Close()
	}
	cancel()

	code := m.Run()
	if testRedisClient != nil {
		testRedisClient.Close()
	}
	os.Exit(code)
}

// flushRateLimitBuckets clears every rate-limit counter so the call that
// follows always starts from a clean budget — see TestMain for why a
// single flush at process start isn't enough on its own (it doesn't cover
// repetitions within one `-count=N` run, or a test elsewhere in the
// package, like TestRateLimit_LoginEndpoint_EventuallyReturns429, that
// deliberately exhausts the same bucket). Called from login() rather than
// once per test so every login attempt — regardless of which test, which
// repetition, or what ran immediately before it — gets its own clean shot,
// without changing what the rate limiter itself does or how many requests
// it takes to trip it.
func flushRateLimitBuckets(t *testing.T) {
	t.Helper()
	if testRedisClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	iter := testRedisClient.Scan(ctx, 0, "ratelimit:*", 0).Iterator()
	for iter.Next(ctx) {
		testRedisClient.Del(ctx, iter.Val())
	}
}

func requireLiveAPI(t *testing.T) string {
	t.Helper()
	url := baseURL()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url + "/live")
	if err != nil {
		t.Skipf("api not reachable at %s, skipping: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("api at %s not healthy (status %d), skipping", url, resp.StatusCode)
	}
	return url
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// login returns a fresh access token for the given demo user, failing the
// test (not skipping) if the seeded credentials don't work — that's a real
// regression, not an environment problem.
//
// This package used to give each test its own synthetic X-Forwarded-For
// identity so login()'s use of the "auth" rate-limit bucket wouldn't
// interfere between tests. That trick relied on the API trusting a
// client-supplied header for its own rate-limit key — exactly the spoofing
// vulnerability the pre-GitHub audit found and pkg fix #1 closed. With the
// header now correctly ignored by default (clientIPResolver in
// services/api/middleware.go), every request in this package legitimately
// shares one real-IP bucket, same as it would for any real client. See
// TestRateLimit_LoginEndpoint_EventuallyReturns429's comment for how this
// package now avoids self-interference without relying on spoofing.
func login(t *testing.T, url, email, password string) string {
	t.Helper()
	flushRateLimitBuckets(t)
	body, _ := json.Marshal(map[string]string{"email": email, "password": password})
	resp, err := http.Post(url+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("login request for %s: %v", email, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login for %s: expected 200, got %d", email, resp.StatusCode)
	}
	var tok tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatalf("decode login response for %s: %v", email, err)
	}
	if tok.AccessToken == "" {
		t.Fatalf("login for %s returned an empty access token", email)
	}
	return tok.AccessToken
}

func authedGet(t *testing.T, url, path, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func TestLogin_ValidCredentials_ReturnsTokens(t *testing.T) {
	url := requireLiveAPI(t)
	token := login(t, url, "admin@musterfabrik-gmbh.de", demoPassword)
	if len(token) < 20 {
		t.Errorf("access token looks too short to be a real JWT: %q", token)
	}
}

func TestLogin_WrongPassword_Returns401(t *testing.T) {
	url := requireLiveAPI(t)
	flushRateLimitBuckets(t) // makes its own raw request rather than calling login(), so it needs its own clean budget
	body, _ := json.Marshal(map[string]string{"email": "admin@musterfabrik-gmbh.de", "password": "definitely-wrong"})
	resp, err := http.Post(url+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("login request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for wrong password, got %d", resp.StatusCode)
	}
	var envelope errorEnvelope
	json.NewDecoder(resp.Body).Decode(&envelope)
	if envelope.Error.Code != "INVALID_CREDENTIALS" {
		t.Errorf("expected error code INVALID_CREDENTIALS, got %q", envelope.Error.Code)
	}
}

func TestProtectedEndpoint_NoToken_Returns401(t *testing.T) {
	url := requireLiveAPI(t)
	resp, err := http.Get(url + "/api/v1/factories")
	if err != nil {
		t.Fatalf("GET /api/v1/factories: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 with no Authorization header, got %d", resp.StatusCode)
	}
}

func TestProtectedEndpoint_MalformedToken_Returns401(t *testing.T) {
	url := requireLiveAPI(t)
	req, _ := http.NewRequest(http.MethodGet, url+"/api/v1/factories", nil)
	req.Header.Set("Authorization", "Bearer not-a-real-jwt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/factories: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 with a malformed token, got %d", resp.StatusCode)
	}
}

// TestTenantIsolation_FactoriesScopedToOrganization is the single most
// important test in this package: it proves — against the real API and
// real Postgres, not by reading the query in handlers_factories.go — that
// an organization's admin can never see another organization's factories.
// "Zweite Firma GmbH" was seeded specifically to make this claim testable:
// its only factory is "Stuttgart Plant" in Stuttgart, a city/name that
// never appears among musterfabrik-gmbh's seeded factories (Berlin,
// Dresden, Munich, Hamburg, ...).
func TestTenantIsolation_FactoriesScopedToOrganization(t *testing.T) {
	url := requireLiveAPI(t)

	type factory struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		City string `json:"city"`
	}
	type page struct {
		Items []factory `json:"items"`
	}

	fetchFactories := func(token string) []factory {
		resp := authedGet(t, url, "/api/v1/factories?limit=100", token)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /api/v1/factories: expected 200, got %d", resp.StatusCode)
		}
		var p page
		if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
			t.Fatalf("decode factories response: %v", err)
		}
		return p.Items
	}

	org1Token := login(t, url, "admin@musterfabrik-gmbh.de", demoPassword)
	org2Token := login(t, url, "admin@zweite-firma-gmbh.de", demoPassword)

	org1Factories := fetchFactories(org1Token)
	org2Factories := fetchFactories(org2Token)

	if len(org1Factories) == 0 {
		t.Fatal("expected musterfabrik-gmbh to have seeded factories, got none")
	}
	for _, f := range org1Factories {
		if f.Name == "Stuttgart Plant" {
			t.Errorf("tenant isolation broken: musterfabrik-gmbh admin can see zweite-firma-gmbh's factory %q", f.Name)
		}
	}

	if len(org2Factories) != 1 || org2Factories[0].Name != "Stuttgart Plant" {
		t.Fatalf("expected zweite-firma-gmbh to see exactly its own factory (Stuttgart Plant), got %+v", org2Factories)
	}
}

// TestRBAC_ViewerCannotProvisionDevice_AdminCan proves permission
// enforcement runs before the handler body: a VIEWER (no devices:write)
// is rejected with 403 before any request-body validation happens, while
// an ADMIN with the same malformed body reaches the handler and fails on
// business validation instead (400) — proving the 403 for VIEWER really is
// about the permission, not a coincidentally-invalid request.
func TestRBAC_ViewerCannotProvisionDevice_AdminCan(t *testing.T) {
	url := requireLiveAPI(t)

	viewerToken := login(t, url, "viewer@musterfabrik-gmbh.de", demoPassword)
	adminToken := login(t, url, "admin@musterfabrik-gmbh.de", demoPassword)

	body := []byte(`{"machine_id":"00000000-0000-0000-0000-000000000000","serial_number":"SN-RBAC-TEST-DOES-NOT-EXIST"}`)

	post := func(token string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, url+"/api/v1/devices", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /api/v1/devices: %v", err)
		}
		return resp
	}

	viewerResp := post(viewerToken)
	defer viewerResp.Body.Close()
	if viewerResp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for VIEWER provisioning a device, got %d", viewerResp.StatusCode)
	}

	adminResp := post(adminToken)
	defer adminResp.Body.Close()
	if adminResp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for ADMIN with a non-existent machine_id (permission granted, business validation failed), got %d", adminResp.StatusCode)
	}
}

// TestRateLimit_LoginEndpoint_EventuallyReturns429 fires far more login
// attempts than the configured per-minute limit (default 30, from
// API_RATE_LIMIT_AUTH_PER_MIN — see docker-compose.yml's comment on that
// var for why it isn't the code default of 10) and asserts at least one is
// rejected. Deliberately runs last in this file: every other test's real
// login() calls in this package (5 total) share the same real-IP "auth"
// bucket as this test now that spoofing a different identity via
// X-Forwarded-For no longer works (correctly — see login()'s doc comment)
// — running this one last means it only ever burns budget that's no
// longer needed by anything else in this package. The loop bound of 60 is
// a safety margin, not the actual cost: the loop breaks the instant it
// sees a 429, so it only ever consumes ~(limit+1) real attempts in
// practice.
//
// Known, accepted consequence, not a bug: this test's whole job is to
// legitimately exhaust the real-IP "auth" bucket, so running this package's
// tests twice within the same 60-second window will make the second run's
// normal logins also see 429s until the window rolls over. That's the rate
// limiter correctly doing its job — a control that could be reset on
// demand by test tooling wouldn't be much of a control. This is a real
// property of testing a real rate limiter, not something to engineer
// around; CI runs the suite once per invocation, so it isn't affected.
func TestRateLimit_LoginEndpoint_EventuallyReturns429(t *testing.T) {
	url := requireLiveAPI(t)
	// Starting from a known-clean budget makes this test's own outcome
	// deterministic too: it always needs the real, configured
	// API_RATE_LIMIT_AUTH_PER_MIN attempts to trip 429, not "however many
	// were already used up by whatever ran before it" — without touching
	// the limit itself or how the limiter decides to trip.
	flushRateLimitBuckets(t)

	body, _ := json.Marshal(map[string]string{"email": "rate-limit-probe@musterfabrik-gmbh.de", "password": "wrong"})
	saw429 := false
	for i := 0; i < 60; i++ {
		resp, err := http.Post(url+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("login attempt %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			saw429 = true
			break
		}
	}
	if !saw429 {
		t.Error("expected at least one 429 Too Many Requests among 60 rapid login attempts, got none")
	}
}

func TestHealthEndpointsReportRealDependencyStatus(t *testing.T) {
	url := requireLiveAPI(t)
	resp, err := http.Get(url + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := jsonBody(resp)
		t.Fatalf("expected /health to report 200 with all dependencies up, got %d: %s", resp.StatusCode, body)
	}
}

func jsonBody(resp *http.Response) (string, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return "", err
	}
	return buf.String(), nil
}
