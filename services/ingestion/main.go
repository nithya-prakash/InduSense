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

	client, err := connectMQTT(ctx, cfg, &mqttConnected, jobs)
	if err != nil {
		log.Fatalf("ingestion: failed to connect to MQTT broker: %v", err)
	}

	startHealthServer(cfg.HTTPPort, &mqttConnected, sink)
	log.Printf("ingestion: health/metrics server listening on :%s", cfg.HTTPPort)

	var wg sync.WaitGroup
	for i := 0; i < cfg.WorkerPoolSize; i++ {
		wg.Add(1)
		go worker(ctx, &wg, sink, jobs)
	}

	<-ctx.Done()
	log.Println("ingestion: shutdown signal received, draining queue...")

	// Ordered shutdown, not deferred: a pre-GitHub audit found that
	// deferring client.Disconnect meant it ran *after* close(jobs) and
	// wg.Wait() (defers unwind in LIFO order only once main() returns,
	// while those two ran inline, first) — so the MQTT client was still
	// connected, and still able to dispatch a message handler that tried
	// to send into an already-closed jobs channel, panicking.
	//
	// Fixing the ordering alone isn't enough: paho's own docs warn that
	// Disconnect "may return before all activities (goroutines) have
	// completed" — it spawns a new, untracked goroutine per inbound
	// message (see mqtt.go) and never waits for them. An earlier version
	// of this fix tried to track those goroutines with a sync.WaitGroup
	// (Add at the top of the handler, Wait here); go test -race caught
	// that this doesn't work — a WaitGroup requires all Add calls to be
	// coordinated with Wait, and a handler goroutine paho schedules late
	// can call Add concurrently with (or after) a Wait that already
	// returned, which is a documented WaitGroup misuse and panics.
	//
	// The actual fix: never give the race anything to land in. jobs is
	// deliberately never closed, so a straggling handler goroutine — no
	// matter when paho gets around to running it — can only ever send
	// into an open, eventually-abandoned channel, never panic on a closed
	// one. Workers (see worker) stop via ctx cancellation, not by ranging
	// jobs to exhaustion, so closing it was never actually necessary.
	client.Disconnect(1000) // 1-2. stop accepting new messages; paho quiesces in-flight work for up to 1s
	wg.Wait()               // 3-5. workers drain whatever's buffered then stop on ctx.Done(); jobs is left open, not closed
	log.Println("ingestion: shutdown complete")
}

func worker(ctx context.Context, wg *sync.WaitGroup, sink *kafkaSink, jobs <-chan inboundMessage) {
	defer wg.Done()
	for {
		select {
		case job := <-jobs:
			processMessage(ctx, sink, job)
		case <-ctx.Done():
			// Drain whatever's already buffered rather than abandoning it.
			// jobs is intentionally never closed (see main's shutdown
			// sequence), so this is the only place it gets emptied.
			for {
				select {
				case job := <-jobs:
					processMessage(ctx, sink, job)
				default:
					return
				}
			}
		}
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
