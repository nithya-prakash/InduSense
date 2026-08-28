package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nithya-prakash/indusense/pkg/auth"
	"github.com/nithya-prakash/indusense/pkg/logging"
	"github.com/redis/go-redis/v9"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyClaims
)

func requestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKeyRequestID).(string); ok {
		return id
	}
	return ""
}

func claimsFromContext(ctx context.Context) *auth.Claims {
	claims, _ := ctx.Value(ctxKeyClaims).(*auth.Claims)
	return claims
}

// withRequestID assigns a request ID (from an incoming X-Request-ID header
// if present, so a caller's own tracing ID is preserved, or a fresh UUID
// otherwise) and echoes it back on the response.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), ctxKeyRequestID, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withLogging logs one structured line per request — method, path, status,
// duration, request ID — after the handler completes.
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		logging.WithContext(r.Context(), logger).Info("request handled",
			"method", r.Method, "path", r.URL.Path, "status", sw.status,
			"duration_ms", time.Since(start).Milliseconds(), "request_id", requestIDFromContext(r.Context()))
		metricAPIRequestsTotal.WithLabelValues(r.Method, r.URL.Path, fmt.Sprintf("%d", sw.status)).Inc()
		metricAPIRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Hijack delegates to the underlying ResponseWriter's Hijacker so this
// wrapper doesn't break WebSocket upgrades — gorilla/websocket's Upgrade()
// requires the http.Hijacker interface to take over the raw TCP connection,
// which a plain embedding wrapper hides unless explicitly forwarded.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not support hijacking")
	}
	return hijacker.Hijack()
}

// withRecover converts a panic in any handler into a clean 500 response
// instead of crashing the process or leaking a stack trace to the client.
func withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("api: recovered panic [request_id=%s]: %v", requestIDFromContext(r.Context()), rec)
				writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func withCORS(allowedOrigin string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// requireAuth parses the Bearer access token, validates it, and attaches
// its Claims to the request context. Every resource handler downstream
// scopes its Postgres/InfluxDB queries by claims.OrganizationID — never by
// anything the client supplies — which is what makes tenant isolation
// enforced at the backend rather than trusted from the frontend.
func requireAuth(accessSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tokenString := bearerToken(r)
			if tokenString == "" {
				writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "missing or malformed Authorization header")
				return
			}
			claims, err := auth.ParseAndValidate(tokenString, accessSecret, auth.TokenTypeAccess)
			if err != nil {
				writeError(w, r, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid or expired access token")
				return
			}
			ctx := context.WithValue(r.Context(), ctxKeyClaims, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) > len(prefix) && h[:len(prefix)] == prefix {
		return h[len(prefix):]
	}
	// WebSocket handshakes can't set custom headers from a browser, so a
	// query-param fallback is accepted there specifically — see ws_alerts.go.
	return ""
}

// requirePermission must run after requireAuth. It never grants access on
// missing claims (a nil claims value — which should be structurally
// impossible if requireAuth ran first — fails closed, not open).
func requirePermission(permission string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims := claimsFromContext(r.Context())
			if claims == nil || !auth.HasPermission(claims.Permissions, permission) {
				writeError(w, r, http.StatusForbidden, "FORBIDDEN", fmt.Sprintf("requires permission %q", permission))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// rateLimitScript makes the increment-and-set-expiry atomic. A plain
// `INCR` followed by a separate `EXPIRE` call (the previous implementation)
// has a window between the two Redis round-trips where a crash or
// cancelled context leaves the key permanently without a TTL — harmless in
// practice since the key already encodes its own minute and would just
// become inert clutter, but a Lua script closes the gap for free since
// Redis already guarantees atomic script execution, no extra
// infrastructure required.
var rateLimitScript = redis.NewScript(`
	local count = redis.call("INCR", KEYS[1])
	if count == 1 then
		redis.call("EXPIRE", KEYS[1], ARGV[1])
	end
	return count
`)

// rateLimiter is a Redis-backed fixed-window limiter: atomically increment
// a per-window counter keyed by (client IP, route bucket), set its expiry
// only on the first increment of the window, and reject once the limit is
// exceeded. Fixed-window is simpler than a sliding-window/token-bucket and
// sufficient here — the spec asks for rate limiting to exist and return
// 429s with correct headers, not for a particular algorithm's
// burst-smoothing properties.
type rateLimiter struct {
	redis      *redis.Client
	ipResolver *clientIPResolver
}

func newRateLimiter(redisClient *redis.Client, ipResolver *clientIPResolver) *rateLimiter {
	return &rateLimiter{redis: redisClient, ipResolver: ipResolver}
}

func (rl *rateLimiter) middleware(bucket string, limit int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			// atomic INCR+EXPIRE via a Lua script — see rateLimitScript for
			// why a separate INCR-then-EXPIRE isn't safe.
			windowKey := fmt.Sprintf("ratelimit:%s:%s:%d", bucket, rl.ipResolver.resolve(r), time.Now().Unix()/60)

			count, err := rateLimitScript.Run(ctx, rl.redis, []string{windowKey}, int(time.Minute.Seconds())).Int64()
			if err != nil {
				// Fail open: Redis being briefly unavailable shouldn't take
				// the whole API down. Logged for visibility.
				log.Printf("api: rate limiter redis error (failing open): %v", err)
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", limit))
			w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", max0(limit-int(count))))

			if int(count) > limit {
				w.Header().Set("Retry-After", "60")
				writeError(w, r, http.StatusTooManyRequests, "RATE_LIMITED", "too many requests, try again later")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func max0(n int) int {
	if n < 0 {
		return 0
	}
	return n
}

// clientIPResolver decides what "the client's IP" means for rate limiting
// and auth audit logging. By default it's just the TCP peer address
// (RemoteAddr) — X-Forwarded-For is never consulted, because it's just a
// header any direct client can set to whatever it wants. When
// trustProxyHeaders is enabled, X-Forwarded-For is trusted ONLY when the
// actual TCP peer connecting to this process is itself in trustedProxies
// (e.g. a known reverse proxy/load balancer address) — a client that
// connects directly, bypassing that proxy, still can't spoof its way past
// rate limiting by setting the header itself.
type clientIPResolver struct {
	trustProxyHeaders bool
	trustedProxies    []*net.IPNet
}

// newClientIPResolver parses trustedCIDRs (bare IPs are treated as /32 or
// /128) once at startup, so a malformed value fails fast at boot rather
// than silently never matching at request time.
func newClientIPResolver(trustProxyHeaders bool, trustedCIDRs []string) (*clientIPResolver, error) {
	nets := make([]*net.IPNet, 0, len(trustedCIDRs))
	for _, entry := range trustedCIDRs {
		cidr := entry
		if !strings.Contains(cidr, "/") {
			if ip := net.ParseIP(entry); ip != nil && ip.To4() != nil {
				cidr = entry + "/32"
			} else {
				cidr = entry + "/128"
			}
		}
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid entry %q in API_TRUSTED_PROXY_CIDRS: %w", entry, err)
		}
		nets = append(nets, ipNet)
	}
	return &clientIPResolver{trustProxyHeaders: trustProxyHeaders, trustedProxies: nets}, nil
}

// resolve returns just the IP, never the port — r.RemoteAddr carries a
// fresh ephemeral port on every new TCP connection, so keying rate limits
// on the raw RemoteAddr string would put every single request in its own
// bucket and never actually limit anything.
func (c *clientIPResolver) resolve(r *http.Request) string {
	peer := r.RemoteAddr
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		peer = host
	}

	if !c.trustProxyHeaders || !c.isTrustedProxy(peer) {
		return peer
	}

	fwd := r.Header.Get("X-Forwarded-For")
	if fwd == "" {
		return peer
	}
	// X-Forwarded-For is a comma-separated hop chain appended to by each
	// proxy in the path; the left-most entry is the original client as
	// seen by the first (trusted) proxy.
	first := strings.TrimSpace(strings.Split(fwd, ",")[0])
	if first == "" {
		return peer
	}
	return first
}

func (c *clientIPResolver) isTrustedProxy(peer string) bool {
	ip := net.ParseIP(peer)
	if ip == nil {
		return false
	}
	for _, ipNet := range c.trustedProxies {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// chain applies middleware in the given order, outermost first.
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
