package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nithya-prakash/indusense/pkg/events"
	"github.com/nithya-prakash/indusense/pkg/reliability"
	"github.com/nithya-prakash/indusense/pkg/tracing"
	kafka "github.com/segmentio/kafka-go"
)

// kafkaSink owns one Writer per topic and applies retry-with-backoff plus a
// circuit breaker around every publish, so a Kafka outage degrades
// gracefully (fail fast, stop hammering the broker) instead of blocking the
// whole worker pool indefinitely.
type kafkaSink struct {
	telemetryWriter *kafka.Writer
	eventsWriter    *kafka.Writer
	dlqWriter       *kafka.Writer

	breaker    *reliability.CircuitBreaker
	maxRetries int
	retryDelay time.Duration
}

func newKafkaSink(cfg Config) *kafkaSink {
	newWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // same key (device_id) always -> same partition -> per-device ordering
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 50 * time.Millisecond,
			Async:        false,
		}
	}

	return &kafkaSink{
		telemetryWriter: newWriter(cfg.TopicTelemetryRaw),
		eventsWriter:    newWriter(cfg.TopicDeviceEvents),
		dlqWriter:       newWriter(cfg.TopicDeadLetter),
		breaker:         reliability.NewCircuitBreaker(cfg.BreakerFailureThreshold, cfg.BreakerCooldown),
		maxRetries:      cfg.KafkaMaxRetries,
		retryDelay:      cfg.KafkaRetryBaseDelay,
	}
}

func (k *kafkaSink) close() {
	_ = k.telemetryWriter.Close()
	_ = k.eventsWriter.Close()
	_ = k.dlqWriter.Close()
}

// publishTelemetry attempts to write to telemetry.raw with retry + circuit
// breaker. On exhaustion it routes the event to the dead-letter topic itself
// so a Kafka blip never silently drops data — the only case that's truly
// unrecoverable is the dead-letter write also failing, in which case the
// caller must not ack the source MQTT message so it gets redelivered later.
func (k *kafkaSink) publishTelemetry(ctx context.Context, key string, evt events.NormalizedTelemetryEvent, rawPayload []byte, sourceTopic string) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return &reliability.ErrPermanent{Err: fmt.Errorf("marshal normalized telemetry: %w", err)}
	}

	err = k.writeWithProtection(ctx, k.telemetryWriter, key, payload)
	if err == nil {
		metricMessagesProcessed.WithLabelValues("success").Inc()
		return nil
	}

	log.Printf("ingestion: kafka publish to telemetry.raw failed after retries, routing to dead-letter: %v", err)
	return k.deadLetter(ctx, rawPayload, err, events.ErrorTypeTransient, "kafka_publish", evt.CorrelationID, sourceTopic)
}

func (k *kafkaSink) publishMachineEvent(ctx context.Context, key string, evt events.NormalizedMachineEvent, rawPayload []byte, sourceTopic string) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return &reliability.ErrPermanent{Err: fmt.Errorf("marshal normalized machine event: %w", err)}
	}

	err = k.writeWithProtection(ctx, k.eventsWriter, key, payload)
	if err == nil {
		metricMessagesProcessed.WithLabelValues("success").Inc()
		return nil
	}

	log.Printf("ingestion: kafka publish to device.events failed after retries, routing to dead-letter: %v", err)
	return k.deadLetter(ctx, rawPayload, err, events.ErrorTypeTransient, "kafka_publish", evt.CorrelationID, sourceTopic)
}

func (k *kafkaSink) deadLetterValidationFailure(ctx context.Context, rawPayload []byte, validationErr error, correlationID, sourceTopic string) error {
	return k.deadLetter(ctx, rawPayload, validationErr, events.ErrorTypeValidation, "validation", correlationID, sourceTopic)
}

func (k *kafkaSink) deadLetter(ctx context.Context, rawPayload []byte, cause error, errorType, stage, correlationID, sourceTopic string) error {
	record := events.DeadLetterRecord{
		OriginalPayload: string(rawPayload),
		Error:           cause.Error(),
		ErrorType:       errorType,
		Service:         "ingestion",
		ProcessingStage: stage,
		RetryCount:      k.maxRetries,
		Timestamp:       time.Now().UTC(),
		CorrelationID:   correlationID,
		SourceTopic:     sourceTopic,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal dead-letter record: %w", err)
	}
	// Dead-letter writes are attempted once without the circuit breaker: if
	// Kafka is down entirely, both the primary topic and dead-letter are
	// unreachable, and there's nothing more ingestion can do but leave the
	// source MQTT message unacked for redelivery.
	writeErr := k.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(correlationID),
		Value: payload,
	})
	if writeErr != nil {
		metricMessagesFailed.WithLabelValues("kafka_unreachable").Inc()
		return fmt.Errorf("dead-letter write also failed: %w", writeErr)
	}
	metricDLQMessages.Inc()
	metricMessagesProcessed.WithLabelValues("dead_letter").Inc()
	return nil
}

func (k *kafkaSink) writeWithProtection(ctx context.Context, w *kafka.Writer, key string, payload []byte) error {
	if !k.breaker.Allow() {
		return fmt.Errorf("circuit breaker open for kafka topic %s", w.Topic)
	}

	var headers []kafka.Header
	tracing.InjectKafka(ctx, &headers)

	err := reliability.RetryWithBackoff(ctx, k.maxRetries, k.retryDelay, func(d time.Duration) { time.Sleep(d) }, func() error {
		return w.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload, Headers: headers})
	})

	if err != nil {
		k.breaker.RecordFailure()
		return err
	}
	k.breaker.RecordSuccess()
	return nil
}

func (k *kafkaSink) breakerState() string {
	return k.breaker.State()
}
