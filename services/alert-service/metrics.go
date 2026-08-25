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
		Help: "Currently open (non-terminal) incidents.",
	})

	metricIncidentsOpenBySeverity = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "incidents_open_by_severity",
		Help: "Currently open (non-terminal) incidents, by severity — what the Grafana 'critical incidents' panel actually needs.",
	}, []string{"severity"})

	metricIncidentsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "incidents_created_total",
		Help: "New incidents opened from an alert.",
	})

	metricAlertsAttachedToIncident = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alerts_attached_to_incident_total",
		Help: "Alerts attached to an already-open incident instead of creating a new one.",
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
