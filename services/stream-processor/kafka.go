package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nithya-prakash/indusense/pkg/events"
	"github.com/nithya-prakash/indusense/pkg/reliability"
	"github.com/nithya-prakash/indusense/pkg/tracing"
	kafka "github.com/segmentio/kafka-go"
)

// kafkaIO's publish path applies retry-with-backoff plus a circuit breaker
// (pkg/reliability), matching the pattern already used for this service's
// InfluxDB writes (see influx.go) and for Kafka writes in ingestion — a
// broker outage should fail fast via the breaker rather than retry every
// message individually and pile up latency. dedup/deadLetter and the
// consumer-side fetch/commit are deliberately left unwrapped: a bad message
// failing validation isn't a "dependency down" situation the breaker is for,
// and if Kafka is down entirely there's nothing more to do but leave the
// source message uncommitted for redelivery once it recovers.
type kafkaIO struct {
	reader          *kafka.Reader
	processedWriter *kafka.Writer
	dlqWriter       *kafka.Writer

	breaker    *reliability.CircuitBreaker
	maxRetries int
	retryDelay time.Duration
}

func newKafkaIO(cfg Config) *kafkaIO {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.TopicTelemetryRaw,
		GroupID:        cfg.ConsumerGroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commit: only after a message is durably handled
	})

	newWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 50 * time.Millisecond,
			Async:        false,
		}
	}

	return &kafkaIO{
		reader:          reader,
		processedWriter: newWriter(cfg.TopicProcessed),
		dlqWriter:       newWriter(cfg.TopicDeadLetter),
		breaker:         reliability.NewCircuitBreaker(cfg.BreakerFailureThreshold, cfg.BreakerCooldown),
		maxRetries:      cfg.KafkaMaxRetries,
		retryDelay:      cfg.KafkaRetryBaseDelay,
	}
}

func (k *kafkaIO) breakerState() string {
	return k.breaker.State()
}

func (k *kafkaIO) close() {
	_ = k.reader.Close()
	_ = k.processedWriter.Close()
	_ = k.dlqWriter.Close()
}

func (k *kafkaIO) fetch(ctx context.Context) (kafka.Message, error) {
	return k.reader.FetchMessage(ctx)
}

func (k *kafkaIO) commit(ctx context.Context, msg kafka.Message) error {
	return k.reader.CommitMessages(ctx, msg)
}

func (k *kafkaIO) lag() int64 {
	return k.reader.Stats().Lag
}

// protectedWrite gates write behind the circuit breaker and retries it with
// backoff, recording the outcome — the reusable wiring between
// pkg/reliability's two primitives, factored out so it can be exercised
// directly in tests without a real Kafka broker (see kafka_test.go).
func (k *kafkaIO) protectedWrite(ctx context.Context, topic string, write func() error) error {
	if !k.breaker.Allow() {
		return fmt.Errorf("circuit breaker open for kafka topic %s", topic)
	}

	err := reliability.RetryWithBackoff(ctx, k.maxRetries, k.retryDelay, func(d time.Duration) { time.Sleep(d) }, write)
	if err != nil {
		k.breaker.RecordFailure()
		return err
	}
	k.breaker.RecordSuccess()
	return nil
}

func (k *kafkaIO) publishProcessed(ctx context.Context, key string, evt events.NormalizedTelemetryEvent) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return &reliability.ErrPermanent{Err: fmt.Errorf("marshal processed telemetry: %w", err)}
	}
	var headers []kafka.Header
	tracing.InjectKafka(ctx, &headers)

	return k.protectedWrite(ctx, k.processedWriter.Topic, func() error {
		return k.processedWriter.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload, Headers: headers})
	})
}

func (k *kafkaIO) deadLetter(ctx context.Context, rawPayload []byte, cause error, stage, correlationID string) error {
	record := events.DeadLetterRecord{
		OriginalPayload: string(rawPayload),
		Error:           cause.Error(),
		ErrorType:       events.ErrorTypeTransient,
		Service:         "stream-processor",
		ProcessingStage: stage,
		Timestamp:       time.Now().UTC(),
		CorrelationID:   correlationID,
		SourceTopic:     "telemetry.raw",
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal dead-letter record: %w", err)
	}
	return k.dlqWriter.WriteMessages(ctx, kafka.Message{Key: []byte(correlationID), Value: payload})
}
