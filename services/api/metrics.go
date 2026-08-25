package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricAPIRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "api_requests_total",
		Help: "HTTP requests handled, by method, path, and status.",
	}, []string{"method", "path", "status"})

	metricAPIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "api_request_duration_seconds",
		Help:    "HTTP request handling duration, by method and path.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	metricWebsocketConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "websocket_connections",
		Help: "Currently connected WebSocket clients.",
	})
)
