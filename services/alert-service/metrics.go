package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricAnomaliesConsumed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "anomalies_consumed_total",
		Help: "Anomaly events consumed from anomalies.detected.",
	})

	metricAlertsGenerated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alerts_generated_total",
		Help: "Alerts created, by severity.",
	}, []string{"severity"})

	metricAlertsSuppressed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "alerts_suppressed_total",
		Help: "Alert-worthy events suppressed by dedup or cooldown, by reason.",
	}, []string{"reason"})

	metricAlertsEscalated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerts_escalated_total",
		Help: "Alerts escalated to a higher severity after remaining unacknowledged.",
	})

	metricIncidentsOpen = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "incidents_open_total",
		Help: "Currently open alerts (proxy for open incidents until Phase 8 adds incident records).",
	})

	metricNotificationSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_sent_total",
		Help: "Notifications successfully sent, by provider.",
	}, []string{"provider"})

	metricNotificationFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_failed_total",
		Help: "Notification attempts that failed, by provider.",
	}, []string{"provider"})

	metricMessagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_failed_total",
		Help: "Messages that failed processing, by reason.",
	}, []string{"reason"})

	metricDLQMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dlq_messages_total",
		Help: "Messages routed to the dead-letter topic.",
	})
)
