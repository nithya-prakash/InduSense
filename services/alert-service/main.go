// Command alert-service consumes anomalies.detected (and device.events for
// unexpected-shutdown alerts), matches incoming events against
// organization-configured alert rules, and creates/escalates alerts in
// Postgres with deduplication and cooldown to prevent alert storms —
// notifying via whichever providers are configured (console always,
// webhook optionally).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nithya-prakash/indusense/pkg/events"
	kafka "github.com/segmentio/kafka-go"
)

const machineShutdownMetric = "machine_status"

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("alert-service: connect to postgres: %v", err)
	}
	defer pool.Close()

	rules, err := newRuleCache(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("alert-service: load initial rule cache: %v", err)
	}
	defer rules.close()
	go runRuleRefresher(ctx, cfg, rules)

	store := newAlertStore(pool)
	counter := newAnomalyCountTracker()
	kio := newKafkaIO(cfg)
	defer kio.close()

	providers := buildProviders(cfg)
	log.Printf("alert-service: notification providers: %v", providerNames(providers))

	go runEscalationSweeper(ctx, cfg, store, kio, providers)

	startHealthServer(cfg.HTTPPort)
	log.Printf("alert-service: health/metrics server listening on :%s", cfg.HTTPPort)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumeAnomalies(ctx, cfg, kio, rules, store, counter, providers)
	}()
	go func() {
		defer wg.Done()
		consumeDeviceEvents(ctx, cfg, kio, rules, store, providers)
	}()

	log.Printf("alert-service: consuming %s and %s as group %s", cfg.TopicAnomalies, cfg.TopicDeviceEvents, cfg.ConsumerGroupID)
	wg.Wait()
	log.Println("alert-service: shutdown complete")
}

func buildProviders(cfg Config) []NotificationProvider {
	var providers []NotificationProvider
	if cfg.NotificationConsoleEnabled {
		providers = append(providers, ConsoleProvider{})
	}
	if cfg.NotificationWebhookURL != "" {
		providers = append(providers, NewWebhookProvider(cfg.NotificationWebhookURL))
	}
	return providers
}

func providerNames(providers []NotificationProvider) []string {
	names := make([]string, len(providers))
	for i, p := range providers {
		names[i] = p.Name()
	}
	return names
}

func runRuleRefresher(ctx context.Context, cfg Config, rules *ruleCache) {
	ticker := time.NewTicker(cfg.RuleRefreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := rules.refresh(ctx); err != nil {
				log.Printf("alert-service: rule cache refresh failed (keeping stale rules): %v", err)
			}
		}
	}
}

func consumeAnomalies(ctx context.Context, cfg Config, kio *kafkaIO, rules *ruleCache, store *alertStore, counter *anomalyCountTracker, providers []NotificationProvider) {
	for {
		msg, err := kio.anomalyReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("alert-service: anomaly fetch error: %v", err)
			continue
		}

		shouldCommit := processAnomaly(ctx, cfg, kio, rules, store, counter, providers, msg)
		if shouldCommit {
			if err := kio.anomalyReader.CommitMessages(ctx, msg); err != nil {
				log.Printf("alert-service: anomaly commit failed for offset %d: %v", msg.Offset, err)
			}
		}
	}
}

func processAnomaly(ctx context.Context, cfg Config, kio *kafkaIO, rules *ruleCache, store *alertStore, counter *anomalyCountTracker, providers []NotificationProvider, msg kafka.Message) bool {
	metricAnomaliesConsumed.Inc()

	var anomaly events.AnomalyDetected
	if err := json.Unmarshal(msg.Value, &anomaly); err != nil {
		metricMessagesFailed.WithLabelValues("unmarshal").Inc()
		return dlqOrHold(ctx, kio, msg.Value, err, "unmarshal", "", cfg.TopicAnomalies)
	}

	for _, rule := range rules.rulesFor(anomaly.OrganizationID, anomaly.Metric) {
		if !rule.scopeMatches(anomaly.MachineID, anomaly.DeviceID, anomaly.SensorID) {
			continue
		}

		count := 0
		if rule.Condition == "ANOMALY_COUNT" {
			key := rule.ID + "|" + anomaly.DeviceID
			count = counter.record(key, anomaly.DetectedAt, time.Duration(rule.WindowSeconds)*time.Second)
		}

		if !conditionMatches(rule, anomaly.Value, count) {
			continue
		}

		dedupeKey := fmt.Sprintf("%s:%s", anomaly.DeviceID, anomaly.Metric)
		alert := Alert{
			OrganizationID: anomaly.OrganizationID,
			Severity:       rule.Severity,
			FactoryID:      anomaly.FactoryID,
			MachineID:      anomaly.MachineID,
			DeviceID:       anomaly.DeviceID,
			SensorID:       anomaly.SensorID,
			Title:          rule.Name,
			Description:    anomaly.Reason,
		}

		result, created, err := store.createIfDue(ctx, rule.ID, rule.CooldownSeconds, alert, dedupeKey)
		if err != nil {
			metricMessagesFailed.WithLabelValues("store_alert").Inc()
			return dlqOrHold(ctx, kio, msg.Value, err, "create_alert", anomaly.EventID, cfg.TopicAnomalies)
		}

		switch result {
		case resultCreated:
			metricAlertsGenerated.WithLabelValues(created.Severity).Inc()
			metricIncidentsOpen.Inc()
			publishAndNotify(ctx, kio, providers, created, rule.ID, events.AlertEventCreated, false)
		case resultSuppressedOpen:
			metricAlertsSuppressed.WithLabelValues("open").Inc()
		case resultSuppressedCooldown:
			metricAlertsSuppressed.WithLabelValues("cooldown").Inc()
		}
	}

	return true
}

func consumeDeviceEvents(ctx context.Context, cfg Config, kio *kafkaIO, rules *ruleCache, store *alertStore, providers []NotificationProvider) {
	for {
		msg, err := kio.eventsReader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("alert-service: device event fetch error: %v", err)
			continue
		}

		shouldCommit := processDeviceEvent(ctx, cfg, kio, rules, store, providers, msg)
		if shouldCommit {
			if err := kio.eventsReader.CommitMessages(ctx, msg); err != nil {
				log.Printf("alert-service: device event commit failed for offset %d: %v", msg.Offset, err)
			}
		}
	}
}

func processDeviceEvent(ctx context.Context, cfg Config, kio *kafkaIO, rules *ruleCache, store *alertStore, providers []NotificationProvider, msg kafka.Message) bool {
	var evt events.NormalizedMachineEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		metricMessagesFailed.WithLabelValues("unmarshal").Inc()
		return dlqOrHold(ctx, kio, msg.Value, err, "unmarshal", "", cfg.TopicDeviceEvents)
	}

	if evt.EventType != "MACHINE_STOPPED" {
		return true // only unexpected shutdown is an alert-worthy device event today
	}

	shutdownRules := rules.rulesFor(evt.OrganizationID, machineShutdownMetric)
	if len(shutdownRules) == 0 {
		return true // no sentinel rule seeded for this org — nothing to dedupe/cooldown against
	}
	rule := shutdownRules[0]

	dedupeKey := "shutdown:" + evt.MachineID
	alert := Alert{
		OrganizationID: evt.OrganizationID,
		Severity:       rule.Severity,
		FactoryID:      evt.FactoryID,
		MachineID:      evt.MachineID,
		DeviceID:       evt.DeviceID,
		Title:          "Unexpected machine shutdown",
		Description:    fmt.Sprintf("machine %s stopped unexpectedly", evt.MachineID),
	}

	result, created, err := store.createIfDue(ctx, rule.ID, rule.CooldownSeconds, alert, dedupeKey)
	if err != nil {
		metricMessagesFailed.WithLabelValues("store_alert").Inc()
		return dlqOrHold(ctx, kio, msg.Value, err, "create_alert", evt.CorrelationID, cfg.TopicDeviceEvents)
	}

	switch result {
	case resultCreated:
		metricAlertsGenerated.WithLabelValues(created.Severity).Inc()
		metricIncidentsOpen.Inc()
		publishAndNotify(ctx, kio, providers, created, rule.ID, events.AlertEventCreated, false)
	case resultSuppressedOpen:
		metricAlertsSuppressed.WithLabelValues("open").Inc()
	case resultSuppressedCooldown:
		metricAlertsSuppressed.WithLabelValues("cooldown").Inc()
	}
	return true
}

func runEscalationSweeper(ctx context.Context, cfg Config, store *alertStore, kio *kafkaIO, providers []NotificationProvider) {
	ticker := time.NewTicker(cfg.EscalationCheckEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			candidates, err := store.dueForEscalation(ctx, cfg.EscalationAfterSeconds)
			if err != nil {
				log.Printf("alert-service: escalation sweep query failed: %v", err)
				continue
			}
			for _, a := range candidates {
				newSeverity := nextSeverity(a.Severity)
				if newSeverity == a.Severity {
					continue // already at the top of the ladder
				}
				if err := store.escalate(ctx, a.ID, newSeverity); err != nil {
					log.Printf("alert-service: escalate alert %s failed: %v", a.ID, err)
					continue
				}
				a.Severity = newSeverity
				a.EscalationLevel++
				metricAlertsEscalated.Inc()
				publishAndNotify(ctx, kio, providers, a, "", events.AlertEventEscalated, true)
				log.Printf("alert-service: escalated alert %s to %s (level %d)", a.ID, newSeverity, a.EscalationLevel)
			}
		}
	}
}

func publishAndNotify(ctx context.Context, kio *kafkaIO, providers []NotificationProvider, a Alert, ruleID, eventType string, isEscalation bool) {
	evt := events.AlertEvent{
		AlertID:         a.ID,
		EventType:       eventType,
		OrganizationID:  a.OrganizationID,
		AlertRuleID:     ruleID,
		FactoryID:       a.FactoryID,
		MachineID:       a.MachineID,
		DeviceID:        a.DeviceID,
		SensorID:        a.SensorID,
		Severity:        a.Severity,
		Status:          "OPEN",
		Title:           a.Title,
		Description:     a.Description,
		EscalationLevel: a.EscalationLevel,
		TriggeredAt:     a.TriggeredAt,
		Timestamp:       time.Now().UTC(),
	}
	if err := kio.publishAlert(ctx, a.DeviceID, evt); err != nil {
		log.Printf("alert-service: failed to publish alert event for alert %s: %v", a.ID, err)
	}

	notifyAll(ctx, providers, Notification{
		AlertID:      a.ID,
		Title:        a.Title,
		Description:  a.Description,
		Severity:     a.Severity,
		MachineID:    a.MachineID,
		DeviceID:     a.DeviceID,
		TriggeredAt:  a.TriggeredAt,
		IsEscalation: isEscalation,
	})
}

// dlqOrHold routes a message to dead-letter and reports whether the caller
// should commit the offset (true) or leave it for redelivery (false, only
// when the dead-letter write itself also failed).
func dlqOrHold(ctx context.Context, kio *kafkaIO, payload []byte, cause error, stage, correlationID, sourceTopic string) bool {
	if err := kio.deadLetter(ctx, payload, cause, stage, correlationID, sourceTopic); err != nil {
		log.Printf("alert-service: dead-letter write failed, leaving message unacked: %v", err)
		return false
	}
	metricDLQMessages.Inc()
	return true
}
