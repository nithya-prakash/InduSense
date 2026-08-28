package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// syncBuffer wraps bytes.Buffer with a mutex so it's safe to write from a
// background goroutine (via log.Printf) while a test polls it from the main
// goroutine — a plain bytes.Buffer has no such synchronization and trips
// -race under exactly this pattern.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *syncBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// TestStartHealthServer_BindFailureIsLogged verifies the fix for a
// pre-GitHub audit finding: startHealthServer used to discard
// http.ListenAndServe's error entirely (`_ = http.ListenAndServe(...)`), so
// a bind failure (e.g. the port already in use) left /live and /ready
// silently unreachable for the process's whole lifetime with no log line
// explaining why. Occupies a real port first, then asserts the resulting
// bind failure is actually logged. Passes a nil pool deliberately — the
// /ready handler that would use it never runs, since the server never
// successfully starts serving.
func TestStartHealthServer_BindFailureIsLogged(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	defer ln.Close()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split occupied address: %v", err)
	}

	var buf syncBuffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	startHealthServer(port, nil)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && buf.Len() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(buf.String(), "health/metrics server") {
		t.Fatalf("expected the bind failure to be logged, got: %q", buf.String())
	}
}

// waitForHealthServer polls until startHealthServer's background listener
// is actually accepting connections, since it starts serving in its own
// goroutine with no signal back to the caller.
func waitForHealthServer(t *testing.T, port string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get("http://localhost:" + port + "/live"); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("health server on port %s never became reachable", port)
}

// freePort asks the OS for a currently-unused port by briefly binding to
// ":0" and closing again. startHealthServer's own listener never gets
// stopped (it's fire-and-forget, matching real production usage where the
// health server runs for the process's whole lifetime) — a hardcoded port
// would leak a bound listener from one test run straight into the next,
// which -count>1 (running this test repeatedly in one process) turns into
// a guaranteed collision: the second run's ListenAndServe fails to bind,
// and requests silently fall through to the first run's server instead —
// one built with a Postgres pool the first run has since closed via its
// own defer, so /ready starts wrongly reporting 503. A fresh port per call
// sidesteps this instead of trying to tear the server down.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split free-port address: %v", err)
	}
	ln.Close()
	return port
}

// TestReadyEndpoint_ReflectsRealPostgresState verifies the fix for a
// pre-GitHub audit finding: /ready used to unconditionally return
// {"ready": true} regardless of whether alert-service could actually reach
// Postgres — its one genuinely-required dependency, since every alert this
// service creates is a Postgres write. It checks both directions: ready
// against a real, reachable database, and not-ready (503) against a pool
// that can never connect.
func TestReadyEndpoint_ReflectsRealPostgresState(t *testing.T) {
	dsn := os.Getenv("ALERT_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	realPool, err := pgxpool.New(ctx, dsn)
	if err != nil || realPool.Ping(ctx) != nil {
		t.Skipf("no live Postgres reachable, skipping: %v", err)
	}
	defer realPool.Close()

	port := freePort(t)
	startHealthServer(port, realPool)
	waitForHealthServer(t, port)

	resp, err := http.Get("http://localhost:" + port + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 with a reachable Postgres, got %d", resp.StatusCode)
	}
	var body struct {
		Ready             bool `json:"ready"`
		PostgresConnected bool `json:"postgres_connected"`
	}
	json.NewDecoder(resp.Body).Decode(&body)
	if !body.Ready || !body.PostgresConnected {
		t.Errorf("expected ready=true, postgres_connected=true with a reachable Postgres, got %+v", body)
	}

	// Not-ready case: a pool pointed at a port nothing is listening on can
	// never connect, so Ping must fail and /ready must report it.
	deadCtx, deadCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deadCancel()
	deadPool, err := pgxpool.New(deadCtx, "postgres://indusense:wrong@localhost:1/indusense?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("construct unreachable pool: %v", err)
	}
	defer deadPool.Close()

	deadPort := freePort(t)
	startHealthServer(deadPort, deadPool)
	waitForHealthServer(t, deadPort)

	resp2, err := http.Get("http://localhost:" + deadPort + "/ready")
	if err != nil {
		t.Fatalf("GET /ready (unreachable postgres): %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 with an unreachable Postgres, got %d", resp2.StatusCode)
	}
	var body2 struct {
		Ready             bool `json:"ready"`
		PostgresConnected bool `json:"postgres_connected"`
	}
	json.NewDecoder(resp2.Body).Decode(&body2)
	if body2.Ready || body2.PostgresConnected {
		t.Errorf("expected ready=false, postgres_connected=false with an unreachable Postgres, got %+v", body2)
	}
}
