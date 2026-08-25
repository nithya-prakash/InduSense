// Command api is InduSense's REST + WebSocket API: authentication, RBAC,
// tenant-scoped factory/device/telemetry/alert/incident endpoints, and a
// real-time alert feed — all wired against the domain logic built in
// earlier phases (pkg/auth, pkg/incidents) rather than reimplementing it.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nithya-prakash/indusense/pkg/auth"
	"github.com/nithya-prakash/indusense/pkg/incidents"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("api: connect to postgres: %v", err)
	}
	defer pool.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer redisClient.Close()

	authSvc := auth.NewService(pool, redisClient, cfg.JWTAccessSecret, cfg.JWTRefreshSecret, cfg.JWTAccessTTL, cfg.JWTRefreshTTL)
	incidentStore := incidents.NewStore(pool, nil)
	limiter := newRateLimiter(redisClient)
	queryAPI := newInfluxQueryAPI(cfg.InfluxURL, cfg.InfluxToken, cfg.InfluxOrg)

	hub := newWSHub()
	go runAlertsFanOut(ctx, cfg.KafkaBrokers, cfg.TopicAlerts, hub)

	mux := http.NewServeMux()
	registerRoutes(mux, cfg, pool, redisClient, authSvc, incidentStore, limiter, queryAPI, hub)

	handler := chain(mux,
		withRecover,
		withRequestID,
		withLogging,
		withCORS(cfg.CORSAllowedOrigin),
	)

	server := &http.Server{Addr: ":" + cfg.Port, Handler: handler}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("api: listening on :%s", cfg.Port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("api: server error: %v", err)
	}
	log.Println("api: shutdown complete")
}

func registerRoutes(
	mux *http.ServeMux,
	cfg Config,
	pool *pgxpool.Pool,
	redisClient *redis.Client,
	authSvc *auth.Service,
	incidentStore *incidents.Store,
	limiter *rateLimiter,
	queryAPI api.QueryAPI,
	hub *wsHub,
) {
	authed := func(h http.HandlerFunc, perm string) http.Handler {
		if perm == "" {
			return chain(h, requireAuth(cfg.JWTAccessSecret))
		}
		return chain(h, requireAuth(cfg.JWTAccessSecret), requirePermission(perm))
	}
	authLimit := limiter.middleware("auth", cfg.RateLimitAuthPerMinute)
	defaultLimit := limiter.middleware("default", cfg.RateLimitDefaultPerMin)

	// --- Unauthenticated ---
	mux.Handle("GET /live", http.HandlerFunc(handleLive()))
	mux.Handle("GET /ready", http.HandlerFunc(handleReady(cfg, pool, redisClient)))
	mux.Handle("GET /health", http.HandlerFunc(handleHealth(cfg, pool, redisClient)))
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("GET /docs", http.HandlerFunc(handleSwaggerUI()))
	mux.Handle("GET /api/v1/openapi.json", http.HandlerFunc(handleOpenAPISpec()))
	mux.Handle("GET /ws/alerts", http.HandlerFunc(handleWSAlerts(hub, cfg.JWTAccessSecret)))

	mux.Handle("POST /api/v1/auth/login", chain(handleLogin(authSvc), authLimit))
	mux.Handle("POST /api/v1/auth/refresh", chain(handleRefresh(authSvc), authLimit))
	mux.Handle("POST /api/v1/auth/logout", chain(handleLogout(authSvc), authLimit))

	// --- Authenticated ---
	mux.Handle("GET /api/v1/auth/me", authed(handleMe(), ""))

	mux.Handle("GET /api/v1/factories", chain(authed(handleListFactories(pool), auth.PermFactoriesRead), defaultLimit))
	mux.Handle("GET /api/v1/factories/{id}", authed(handleGetFactory(pool), auth.PermFactoriesRead))
	mux.Handle("GET /api/v1/factories/{id}/production-lines", authed(handleListProductionLines(pool), auth.PermFactoriesRead))
	mux.Handle("GET /api/v1/production-lines/{id}/machines", authed(handleListLineMachines(pool), auth.PermFactoriesRead))
	mux.Handle("GET /api/v1/machines/{id}", authed(handleGetMachine(pool), auth.PermFactoriesRead))
	mux.Handle("GET /api/v1/machines/{id}/devices", authed(handleListMachineDevices(pool), auth.PermDevicesRead))

	mux.Handle("GET /api/v1/devices", chain(authed(handleListDevices(pool), auth.PermDevicesRead), defaultLimit))
	mux.Handle("POST /api/v1/devices", chain(authed(handleProvisionDevice(pool), auth.PermDevicesWrite), defaultLimit))
	mux.Handle("GET /api/v1/devices/{id}", authed(handleGetDevice(pool), auth.PermDevicesRead))
	mux.Handle("GET /api/v1/devices/{id}/sensors", authed(handleListDeviceSensors(pool), auth.PermDevicesRead))
	mux.Handle("POST /api/v1/devices/{id}/rotate-credentials", chain(authed(handleRotateCredentials(pool), auth.PermDevicesWrite), defaultLimit))
	mux.Handle("POST /api/v1/devices/{id}/decommission", chain(authed(handleDecommissionDevice(pool), auth.PermDevicesWrite), defaultLimit))

	mux.Handle("GET /api/v1/telemetry/latest", chain(authed(handleTelemetryLatest(pool, queryAPI, cfg.InfluxBucket), auth.PermTelemetryRead), defaultLimit))
	mux.Handle("GET /api/v1/telemetry/range", chain(authed(handleTelemetryRange(pool, queryAPI, cfg.InfluxBucket), auth.PermTelemetryRead), defaultLimit))

	mux.Handle("GET /api/v1/alerts", chain(authed(handleListAlerts(pool), auth.PermAlertsRead), defaultLimit))
	mux.Handle("GET /api/v1/alerts/{id}", authed(handleGetAlert(pool), auth.PermAlertsRead))
	mux.Handle("POST /api/v1/alerts/{id}/acknowledge", chain(authed(handleAcknowledgeAlert(pool), auth.PermAlertsManage), defaultLimit))

	mux.Handle("GET /api/v1/incidents", authed(handleListIncidents(incidentStore), auth.PermIncidentsRead))
	mux.Handle("GET /api/v1/incidents/{id}", authed(handleGetIncident(incidentStore), auth.PermIncidentsRead))
	mux.Handle("POST /api/v1/incidents/{id}/transition", authed(handleTransitionIncident(incidentStore), auth.PermIncidentsManage))
	mux.Handle("POST /api/v1/incidents/{id}/assign", authed(handleAssignIncident(incidentStore), auth.PermIncidentsManage))
	mux.Handle("POST /api/v1/incidents/{id}/resolve", authed(handleResolveIncident(incidentStore), auth.PermIncidentsManage))

	// Catch-all: any path not matched by a more specific pattern above
	// still gets the consistent JSON error envelope, not Go's default
	// plain-text "404 page not found".
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "no such route")
	})
}
