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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

const demoPassword = "ChangeMe123!"

func baseURL() string {
	if v := os.Getenv("API_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
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

// testClientIP hands each test its own synthetic client IP (RFC 5737
// TEST-NET-3, guaranteed never a real address) for the login rate
// limiter's X-Forwarded-For-keyed bucket. Every test in this package logs
// in from the same test-runner IP; without this, one test's login calls
// count against the same 10/min bucket every other test's login() depends
// on, so two test runs within the same minute (or one test that
// deliberately exhausts the limit) make unrelated tests fail with 429
// instead of the status code they're actually checking for.
var ipCounter atomic.Int64

func testClientIP() string {
	n := ipCounter.Add(1)
	return fmt.Sprintf("203.0.113.%d", n%250+1)
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
func login(t *testing.T, url, clientIP, email, password string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"email": email, "password": password})
	req, err := http.NewRequest(http.MethodPost, url+"/api/v1/auth/login", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build login request for %s: %v", email, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-For", clientIP)
	resp, err := http.DefaultClient.Do(req)
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

func authedGet(t *testing.T, url, path, clientIP, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+path, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Forwarded-For", clientIP)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func TestLogin_ValidCredentials_ReturnsTokens(t *testing.T) {
	url := requireLiveAPI(t)
	token := login(t, url, testClientIP(), "admin@musterfabrik-gmbh.de", demoPassword)
	if len(token) < 20 {
		t.Errorf("access token looks too short to be a real JWT: %q", token)
	}
}

func TestLogin_WrongPassword_Returns401(t *testing.T) {
	url := requireLiveAPI(t)
	body, _ := json.Marshal(map[string]string{"email": "admin@musterfabrik-gmbh.de", "password": "definitely-wrong"})
	req, _ := http.NewRequest(http.MethodPost, url+"/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-For", testClientIP())
	resp, err := http.DefaultClient.Do(req)
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
	ip := testClientIP()

	type factory struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		City string `json:"city"`
	}
	type page struct {
		Items []factory `json:"items"`
	}

	fetchFactories := func(token string) []factory {
		resp := authedGet(t, url, "/api/v1/factories?limit=100", ip, token)
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

	org1Token := login(t, url, ip, "admin@musterfabrik-gmbh.de", demoPassword)
	org2Token := login(t, url, ip, "admin@zweite-firma-gmbh.de", demoPassword)

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
	ip := testClientIP()

	viewerToken := login(t, url, ip, "viewer@musterfabrik-gmbh.de", demoPassword)
	adminToken := login(t, url, ip, "admin@musterfabrik-gmbh.de", demoPassword)

	body := []byte(`{"machine_id":"00000000-0000-0000-0000-000000000000","serial_number":"SN-RBAC-TEST-DOES-NOT-EXIST"}`)

	post := func(token string) *http.Response {
		req, _ := http.NewRequest(http.MethodPost, url+"/api/v1/devices", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-For", ip)
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

// TestRateLimit_LoginEndpoint_EventuallyReturns429 fires more login
// attempts than the configured per-minute limit (default 10, from
// API_RATE_LIMIT_AUTH_PER_MIN) from its own synthetic client IP and
// asserts at least one is rejected. It checks "at least one 429 among N
// attempts" rather than "exactly the Nth request" to stay robust against
// the fixed-window limiter's minute-boundary edge case.
func TestRateLimit_LoginEndpoint_EventuallyReturns429(t *testing.T) {
	url := requireLiveAPI(t)
	ip := testClientIP()

	body, _ := json.Marshal(map[string]string{"email": "rate-limit-probe@musterfabrik-gmbh.de", "password": "wrong"})
	saw429 := false
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest(http.MethodPost, url+"/api/v1/auth/login", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-For", ip)
		resp, err := http.DefaultClient.Do(req)
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
		t.Error("expected at least one 429 Too Many Requests among 20 rapid login attempts, got none")
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
