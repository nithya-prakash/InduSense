package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nithya-prakash/indusense/pkg/events"
	"github.com/nithya-prakash/indusense/pkg/tracing"
	kafka "github.com/segmentio/kafka-go"
)

type kafkaIO struct {
	anomalyReader *kafka.Reader
	eventsReader  *kafka.Reader
	alertWriter   *kafka.Writer
	dlqWriter     *kafka.Writer
}

func newKafkaIO(cfg Config) *kafkaIO {
	newReader := func(topic string) *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.KafkaBrokers,
			Topic:          topic,
			GroupID:        cfg.ConsumerGroupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
		})
	}
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
		anomalyReader: newReader(cfg.TopicAnomalies),
		eventsReader:  newReader(cfg.TopicDeviceEvents),
		alertWriter:   newWriter(cfg.TopicAlerts),
		dlqWriter:     newWriter(cfg.TopicDeadLetter),
	}
}

func (k *kafkaIO) close() {
	_ = k.anomalyReader.Close()
	_ = k.eventsReader.Close()
	_ = k.alertWriter.Close()
	_ = k.dlqWriter.Close()
}

func (k *kafkaIO) publishAlert(ctx context.Context, key string, evt events.AlertEvent) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal alert event: %w", err)
	}
	var headers []kafka.Header
	tracing.InjectKafka(ctx, &headers)
	return k.alertWriter.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload, Headers: headers})
}

func (k *kafkaIO) deadLetter(ctx context.Context, rawPayload []byte, cause error, stage, correlationID, sourceTopic string) error {
	record := events.DeadLetterRecord{
		OriginalPayload: string(rawPayload),
		Error:           cause.Error(),
		ErrorType:       events.ErrorTypeTransient,
		Service:         "alert-service",
		ProcessingStage: stage,
		Timestamp:       time.Now().UTC(),
		CorrelationID:   correlationID,
		SourceTopic:     sourceTopic,
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal dead-letter record: %w", err)
	}
	return k.dlqWriter.WriteMessages(ctx, kafka.Message{Key: []byte(correlationID), Value: payload})
}
