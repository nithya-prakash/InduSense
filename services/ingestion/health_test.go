package main

import (
	"bytes"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
// bind failure is actually logged.
func TestStartHealthServer_BindFailureIsLogged(t *testing.T) {
	// Occupy the same address form startHealthServer binds to (":port",
	// i.e. the wildcard address) — occupying a different address form
	// (e.g. "127.0.0.1:port") isn't guaranteed to actually conflict with a
	// later wildcard bind on every OS/network stack.
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

	var mqttConnected atomic.Bool
	startHealthServer(port, &mqttConnected, &kafkaSink{})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && buf.Len() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(buf.String(), "health/metrics server") {
		t.Fatalf("expected the bind failure to be logged, got: %q", buf.String())
	}
}
