package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/nithya-prakash/indusense/pkg/reliability"
)

// Notification is the provider-agnostic shape every NotificationProvider
// sends — deliberately independent of the Postgres Alert row or the Kafka
// AlertEvent so providers don't need to know about either.
type Notification struct {
	AlertID      string
	Title        string
	Description  string
	Severity     string
	MachineID    string
	DeviceID     string
	TriggeredAt  time.Time
	IsEscalation bool
}

// NotificationProvider is intentionally minimal so new channels (email,
// Slack, PagerDuty, ...) can be added without touching the alert engine
// itself — see docs/RELIABILITY.md for why console/webhook are the only
// ones actually implemented here.
type NotificationProvider interface {
	Send(ctx context.Context, n Notification) error
	Name() string
}

// ConsoleProvider logs the notification. It's the default in local
// development and CI, requiring no external service.
type ConsoleProvider struct{}

func (ConsoleProvider) Name() string { return "console" }

func (ConsoleProvider) Send(_ context.Context, n Notification) error {
	tag := "ALERT"
	if n.IsEscalation {
		tag = "ALERT ESCALATED"
	}
	log.Printf("[%s] severity=%s title=%q machine=%s device=%s alert_id=%s: %s",
		tag, n.Severity, n.Title, n.MachineID, n.DeviceID, n.AlertID, n.Description)
	return nil
}

// WebhookProvider POSTs a JSON payload to a configured URL — the local,
// no-paid-service stand-in for Slack/PagerDuty/etc. A failure here is
// logged and counted, never dead-lettered: the alert itself is already
// durably recorded in Postgres by the time notification is attempted, so a
// webhook outage means a missed notification, not lost alert data.
type WebhookProvider struct {
	URL        string
	httpClient *http.Client
	breaker    *reliability.CircuitBreaker
}

func NewWebhookProvider(url string) *WebhookProvider {
	return &WebhookProvider{
		URL:        url,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		breaker:    reliability.NewCircuitBreaker(5, 30*time.Second),
	}
}

func (WebhookProvider) Name() string { return "webhook" }

func (w *WebhookProvider) Send(ctx context.Context, n Notification) error {
	if !w.breaker.Allow() {
		return fmt.Errorf("webhook circuit breaker open")
	}

	payload, err := json.Marshal(map[string]any{
		"alert_id":      n.AlertID,
		"title":         n.Title,
		"description":   n.Description,
		"severity":      n.Severity,
		"machine_id":    n.MachineID,
		"device_id":     n.DeviceID,
		"triggered_at":  n.TriggeredAt,
		"is_escalation": n.IsEscalation,
	})
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}

	err = reliability.RetryWithBackoff(ctx, 3, 500*time.Millisecond, func(d time.Duration) { time.Sleep(d) }, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(payload))
		if err != nil {
			return &reliability.ErrPermanent{Err: err}
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := w.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("webhook returned %d", resp.StatusCode)
		}
		if resp.StatusCode >= 400 {
			return &reliability.ErrPermanent{Err: fmt.Errorf("webhook returned %d", resp.StatusCode)}
		}
		return nil
	})

	if err != nil {
		w.breaker.RecordFailure()
		return err
	}
	w.breaker.RecordSuccess()
	return nil
}

// notifyAll fans a notification out to every configured provider,
// independently — one provider's failure never blocks another's.
func notifyAll(ctx context.Context, providers []NotificationProvider, n Notification) {
	for _, p := range providers {
		if err := p.Send(ctx, n); err != nil {
			metricNotificationFailed.WithLabelValues(p.Name()).Inc()
			log.Printf("alert-service: notification via %s failed: %v", p.Name(), err)
		} else {
			metricNotificationSent.WithLabelValues(p.Name()).Inc()
		}
	}
}
