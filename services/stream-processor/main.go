// Command stream-processor consumes telemetry.raw, deduplicates by
// event_id, writes each raw reading to InfluxDB, maintains rolling windowed
// aggregates (moving average/stddev/min/max/rate-of-change) flushed to
// InfluxDB periodically, and republishes a cleaned copy to
// telemetry.processed for the anomaly detector.
//
// Consumption is deliberately single-goroutine per instance: FetchMessage
// and CommitMessages are called sequentially so offset commits always
// advance in the order messages were actually processed. Scaling beyond one
// instance's throughput is done the standard Kafka way — more partitions,
// more consumer-group instances — not more goroutines committing out of
// order within one instance.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/nithya-prakash/indusense/pkg/events"
	"github.com/nithya-prakash/indusense/pkg/logging"
	"github.com/nithya-prakash/indusense/pkg/tracing"
	kafka "github.com/segmentio/kafka-go"
)

var logger = logging.Init("stream-processor")

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Init(ctx, "stream-processor")
	if err != nil {
		log.Printf("stream-processor: tracing disabled: %v", err)
	}
	defer shutdownTracing(context.Background())

	kio := newKafkaIO(cfg)
	defer kio.close()

	dedup := newDeduplicator(cfg)
	defer dedup.close()

	influx := newInfluxSink(cfg)
	defer influx.close()

	if err := influx.ping(context.Background()); err != nil {
		log.Fatalf("stream-processor: cannot reach influxdb at startup: %v", err)
	}
	if err := dedup.ping(context.Background()); err != nil {
		log.Fatalf("stream-processor: cannot reach redis at startup: %v", err)
	}

	startHealthServer(cfg.HTTPPort, dedup, influx, kio)
	log.Printf("stream-processor: health/metrics server listening on :%s", cfg.HTTPPort)

	maxWindow := cfg.Windows[len(cfg.Windows)-1]
	registry := newSeriesRegistry(maxWindow)

	go runLagReporter(ctx, kio)
	go runWindowFlusher(ctx, cfg, registry, influx)

	log.Printf("stream-processor: consuming %s as group %s", cfg.TopicTelemetryRaw, cfg.ConsumerGroupID)
	consumeLoop(ctx, kio, dedup, influx, registry)
	log.Println("stream-processor: shutdown complete")
}

func consumeLoop(ctx context.Context, kio *kafkaIO, dedup *deduplicator, influx *influxSink, registry *seriesRegistry) {
	for {
		msg, err := kio.fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("stream-processor: fetch error: %v", err)
			continue
		}

		start := time.Now()
		shouldCommit := processMessage(ctx, kio, dedup, influx, registry, msg)
		metricProcessingLatency.Observe(time.Since(start).Seconds())

		if shouldCommit {
			if err := kio.commit(ctx, msg); err != nil {
				log.Printf("stream-processor: commit failed for offset %d: %v", msg.Offset, err)
			}
		} else {
			log.Printf("stream-processor: leaving offset %d uncommitted for reprocessing", msg.Offset)
		}
	}
}

// processMessage returns whether the offset should be committed. false means
// the message was neither durably processed nor dead-lettered, so it must be
// redelivered on the next fetch (after a restart or rebalance).
func processMessage(ctx context.Context, kio *kafkaIO, dedup *deduplicator, influx *influxSink, registry *seriesRegistry, msg kafka.Message) bool {
	metricMessagesConsumed.Inc()

	ctx = tracing.ExtractKafka(ctx, msg.Headers)
	ctx, span := tracing.Tracer("stream-processor").Start(ctx, "stream_processor.process")
	defer span.End()

	var evt events.NormalizedTelemetryEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		metricMessagesFailed.WithLabelValues("unmarshal").Inc()
		if dlqErr := kio.deadLetter(ctx, msg.Value, err, "unmarshal", ""); dlqErr != nil {
			logging.WithContext(ctx, logger).Error("dead-letter write failed for malformed message", "error", dlqErr)
			return false
		}
		metricDLQMessages.Inc()
		return true
	}

	firstSeen, err := dedup.claim(ctx, evt.EventID)
	if err != nil {
		// Redis is unreachable: fail safe by NOT committing, so this message
		// is retried once Redis recovers rather than silently processed
		// without dedup protection.
		metricMessagesFailed.WithLabelValues("dedup_unavailable").Inc()
		logging.WithContext(ctx, logger).Error("dedup check failed", "error", err, "event_id", evt.EventID, "device_id", evt.DeviceID, "organization_id", evt.OrganizationID)
		return false
	}
	if !firstSeen {
		metricDuplicateEvents.Inc()
		return true // known duplicate: safe to commit without reprocessing
	}

	key := seriesKey{
		FactoryID:        evt.FactoryID,
		ProductionLineID: evt.ProductionLineID,
		MachineID:        evt.MachineID,
		DeviceID:         evt.DeviceID,
		SensorID:         evt.SensorID,
		Metric:           evt.Metric,
	}

	if err := influx.writeRawPoint(ctx, key, evt.Timestamp, evt.Value); err != nil {
		metricMessagesFailed.WithLabelValues("influx_write").Inc()
		if dlqErr := kio.deadLetter(ctx, msg.Value, err, "influxdb_write", evt.EventID); dlqErr != nil {
			logging.WithContext(ctx, logger).Error("dead-letter write also failed, leaving unacked", "error", dlqErr, "event_id", evt.EventID, "device_id", evt.DeviceID, "organization_id", evt.OrganizationID)
			return false
		}
		metricDLQMessages.Inc()
		return true
	}

	registry.record(key, evt.Timestamp, evt.Value)

	if err := kio.publishProcessed(ctx, evt.DeviceID, evt); err != nil {
		metricMessagesFailed.WithLabelValues("republish").Inc()
		if dlqErr := kio.deadLetter(ctx, msg.Value, err, "republish_processed", evt.EventID); dlqErr != nil {
			logging.WithContext(ctx, logger).Error("dead-letter write also failed, leaving unacked", "error", dlqErr, "event_id", evt.EventID, "device_id", evt.DeviceID, "organization_id", evt.OrganizationID)
			return false
		}
		metricDLQMessages.Inc()
		return true
	}

	return true
}

// runWindowFlusher periodically computes and writes windowed aggregates for
// every tracked series. Aggregate writes are best-effort (logged, not
// dead-lettered): they're derived observability data recomputable from raw
// points already durably stored, not a primary business record.
func runWindowFlusher(ctx context.Context, cfg Config, registry *seriesRegistry, influx *influxSink) {
	windowLabels := make(map[time.Duration]string, len(cfg.Windows))
	for _, w := range cfg.Windows {
		windowLabels[w] = w.String()
	}

	ticker := time.NewTicker(cfg.WindowFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushOnce(ctx, cfg, registry, influx, windowLabels)
		}
	}
}

func flushOnce(ctx context.Context, cfg Config, registry *seriesRegistry, influx *influxSink, windowLabels map[time.Duration]string) {
	now := time.Now().UTC()
	var points []*write.Point

	for _, series := range registry.snapshot() {
		for _, w := range cfg.Windows {
			stats, ok := series.Buf.statsFor(now, w)
			if !ok {
				continue
			}
			points = append(points, buildAggregatePoint(series.Key, windowLabels[w], now, stats))
		}
	}

	if err := influx.writeAggregateBatch(ctx, points); err != nil {
		log.Printf("stream-processor: windowed aggregate flush failed (%d points dropped): %v", len(points), err)
		return
	}
	metricAggregatesWritten.Add(float64(len(points)))
}

func runLagReporter(ctx context.Context, kio *kafkaIO) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metricKafkaConsumerLag.Set(float64(kio.lag()))
		}
	}
}
