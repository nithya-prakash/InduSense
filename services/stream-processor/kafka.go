package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nithya-prakash/indusense/pkg/events"
	kafka "github.com/segmentio/kafka-go"
)

type kafkaIO struct {
	reader          *kafka.Reader
	processedWriter *kafka.Writer
	dlqWriter       *kafka.Writer
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
	}
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

func (k *kafkaIO) publishProcessed(ctx context.Context, key string, evt events.NormalizedTelemetryEvent) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal processed telemetry: %w", err)
	}
	return k.processedWriter.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload})
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
