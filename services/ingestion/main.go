// Command ingestion bridges MQTT and Kafka: it subscribes to telemetry and
// machine status/event topics, validates and normalizes each message, and
// publishes it to the appropriate Kafka topic, partitioned by device_id so
// per-device ordering is preserved while different devices process in
// parallel. It never performs database writes itself — that's the stream
// processor's job downstream.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nithya-prakash/indusense/pkg/events"
	"github.com/nithya-prakash/indusense/pkg/logging"
	"github.com/nithya-prakash/indusense/pkg/tracing"
	"go.opentelemetry.io/otel/attribute"
)

var logger = logging.Init("ingestion")

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Init(ctx, "ingestion")
	if err != nil {
		log.Printf("ingestion: tracing disabled: %v", err)
	}
	defer shutdownTracing(context.Background())

	sink := newKafkaSink(cfg)
	defer sink.close()

	var mqttConnected atomic.Bool
	jobs := make(chan inboundMessage, cfg.QueueCapacity)

	client, err := connectMQTT(cfg, &mqttConnected, jobs)
	if err != nil {
		log.Fatalf("ingestion: failed to connect to MQTT broker: %v", err)
	}
	defer client.Disconnect(1000)

	startHealthServer(cfg.HTTPPort, &mqttConnected, sink)
	log.Printf("ingestion: health/metrics server listening on :%s", cfg.HTTPPort)

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerPoolSize; i++ {
		wg.Add(1)
		go worker(ctx, &wg, sink, jobs)
	}

	<-ctx.Done()
	log.Println("ingestion: shutdown signal received, draining queue...")
	close(jobs)
	wg.Wait()
	log.Println("ingestion: shutdown complete")
}

func worker(ctx context.Context, wg *sync.WaitGroup, sink *kafkaSink, jobs <-chan inboundMessage) {
	defer wg.Done()
	for job := range jobs {
		processMessage(ctx, sink, job)
	}
}

func processMessage(ctx context.Context, sink *kafkaSink, job inboundMessage) {
	start := time.Now()
	defer func() { metricProcessingLatency.Observe(time.Since(start).Seconds()) }()

	ctx, span := tracing.Tracer("ingestion").Start(ctx, "ingestion.process_message")
	span.SetAttributes(attribute.String("mqtt.topic", job.topic))
	defer span.End()

	switch {
	case strings.HasSuffix(job.topic, "/telemetry"):
		metricMessagesReceived.WithLabelValues("telemetry").Inc()
		handleTelemetry(ctx, sink, job)
	case strings.HasSuffix(job.topic, "/status"):
		metricMessagesReceived.WithLabelValues("status").Inc()
		handleMachineEvent(ctx, sink, job)
	case strings.HasSuffix(job.topic, "/events"):
		metricMessagesReceived.WithLabelValues("events").Inc()
		handleMachineEvent(ctx, sink, job)
	default:
		logging.WithContext(ctx, logger).Warn("unrecognized MQTT topic, dropping", "mqtt_topic", job.topic)
		job.ack()
	}
}

func handleTelemetry(ctx context.Context, sink *kafkaSink, job inboundMessage) {
	var raw events.TelemetryEvent
	if err := json.Unmarshal(job.payload, &raw); err != nil {
		metricMessagesFailed.WithLabelValues("validation").Inc()
		if dlqErr := sink.deadLetterValidationFailure(ctx, job.payload, err, uuid.NewString(), job.topic); dlqErr != nil {
			logging.WithContext(ctx, logger).Error("dead-letter write failed for malformed telemetry, leaving unacked", "error", dlqErr, "mqtt_topic", job.topic)
			return // do not ack: let MQTT redeliver
		}
		job.ack()
		return
	}

	if err := validateTelemetry(raw); err != nil {
		metricMessagesFailed.WithLabelValues("validation").Inc()
		if dlqErr := sink.deadLetterValidationFailure(ctx, job.payload, err, raw.EventID, job.topic); dlqErr != nil {
			logging.WithContext(ctx, logger).Error("dead-letter write failed for invalid telemetry, leaving unacked", "error", dlqErr, "event_id", raw.EventID, "device_id", raw.DeviceID, "organization_id", raw.OrganizationID)
			return
		}
		job.ack()
		return
	}

	normalized := events.NormalizedTelemetryEvent{
		TelemetryEvent: raw,
		CorrelationID:  raw.EventID,
		IngestedAt:     time.Now().UTC(),
		SchemaVersion:  events.SchemaVersion,
	}

	if err := sink.publishTelemetry(ctx, raw.DeviceID, normalized, job.payload, job.topic); err != nil {
		logging.WithContext(ctx, logger).Error("could not durably record telemetry, leaving unacked for redelivery", "error", err, "event_id", raw.EventID, "device_id", raw.DeviceID, "organization_id", raw.OrganizationID)
		return
	}
	logging.WithContext(ctx, logger).Info("telemetry event ingested", "event_id", raw.EventID, "device_id", raw.DeviceID, "organization_id", raw.OrganizationID)
	job.ack()
}

func handleMachineEvent(ctx context.Context, sink *kafkaSink, job inboundMessage) {
	var raw events.MachineEvent
	if err := json.Unmarshal(job.payload, &raw); err != nil {
		metricMessagesFailed.WithLabelValues("validation").Inc()
		if dlqErr := sink.deadLetterValidationFailure(ctx, job.payload, err, uuid.NewString(), job.topic); dlqErr != nil {
			logging.WithContext(ctx, logger).Error("dead-letter write failed for malformed machine event, leaving unacked", "error", dlqErr, "mqtt_topic", job.topic)
			return
		}
		job.ack()
		return
	}

	correlationID := uuid.NewString()
	if err := validateMachineEvent(raw); err != nil {
		metricMessagesFailed.WithLabelValues("validation").Inc()
		if dlqErr := sink.deadLetterValidationFailure(ctx, job.payload, err, correlationID, job.topic); dlqErr != nil {
			logging.WithContext(ctx, logger).Error("dead-letter write failed for invalid machine event, leaving unacked", "error", dlqErr, "device_id", raw.DeviceID, "organization_id", raw.OrganizationID)
			return
		}
		job.ack()
		return
	}

	normalized := events.NormalizedMachineEvent{
		MachineEvent:  raw,
		CorrelationID: correlationID,
		IngestedAt:    time.Now().UTC(),
		SchemaVersion: events.SchemaVersion,
	}

	if err := sink.publishMachineEvent(ctx, raw.DeviceID, normalized, job.payload, job.topic); err != nil {
		logging.WithContext(ctx, logger).Error("could not durably record machine event, leaving unacked for redelivery", "error", err, "event_id", correlationID, "device_id", raw.DeviceID, "organization_id", raw.OrganizationID)
		return
	}
	logging.WithContext(ctx, logger).Info("machine event ingested", "event_id", correlationID, "device_id", raw.DeviceID, "organization_id", raw.OrganizationID, "event_type", raw.EventType)
	job.ack()
}
