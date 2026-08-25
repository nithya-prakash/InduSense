// Command anomaly-detector consumes telemetry.processed and runs three
// independent detection levels on every reading — a rule-based operating-
// range check, a statistical EWMA z-score check, and a per-machine-type
// isolation forest over each device's multivariate feature vector —
// publishing a combined result to anomalies.detected whenever any of them
// fires.
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

	"github.com/google/uuid"
	"github.com/nithya-prakash/indusense/pkg/events"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cat, err := newCatalog(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("anomaly-detector: failed to load initial catalog: %v", err)
	}
	defer cat.close()
	go runCatalogRefresher(ctx, cfg, cat)

	kio := newKafkaIO(cfg)
	defer kio.close()

	trackers := newStatisticalTrackers(cfg.EWMAAlpha)
	fs := newFeatureStore(cfg.ForestTrainingBufferSize)
	forests := newForestRegistry()
	go runForestTrainer(ctx, cfg, fs, forests)
	go runLagReporter(ctx, kio)

	startHealthServer(cfg.HTTPPort, forests)
	log.Printf("anomaly-detector: health/metrics server listening on :%s", cfg.HTTPPort)

	log.Printf("anomaly-detector: consuming %s as group %s", cfg.TopicProcessed, cfg.ConsumerGroupID)
	consumeLoop(ctx, cfg, kio, cat, trackers, fs, forests)
	log.Println("anomaly-detector: shutdown complete")
}

func runCatalogRefresher(ctx context.Context, cfg Config, cat *catalog) {
	ticker := time.NewTicker(cfg.CatalogRefreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := cat.refresh(ctx); err != nil {
				log.Printf("anomaly-detector: catalog refresh failed (keeping stale data): %v", err)
			}
		}
	}
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

func consumeLoop(ctx context.Context, cfg Config, kio *kafkaIO, cat *catalog, trackers *statisticalTrackers, fs *featureStore, forests *forestRegistry) {
	for {
		msg, err := kio.fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("anomaly-detector: fetch error: %v", err)
			continue
		}

		start := time.Now()
		shouldCommit := processMessage(ctx, cfg, kio, cat, trackers, fs, forests, msg)
		metricProcessingLatency.Observe(time.Since(start).Seconds())

		if shouldCommit {
			if err := kio.commit(ctx, msg); err != nil {
				log.Printf("anomaly-detector: commit failed for offset %d: %v", msg.Offset, err)
			}
		} else {
			log.Printf("anomaly-detector: leaving offset %d uncommitted for reprocessing", msg.Offset)
		}
	}
}

func processMessage(ctx context.Context, cfg Config, kio *kafkaIO, cat *catalog, trackers *statisticalTrackers, fs *featureStore, forests *forestRegistry, msg kafka.Message) bool {
	metricMessagesConsumed.Inc()

	var evt events.NormalizedTelemetryEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		metricMessagesFailed.WithLabelValues("unmarshal").Inc()
		if dlqErr := kio.deadLetter(ctx, msg.Value, err, "unmarshal", ""); dlqErr != nil {
			log.Printf("anomaly-detector: dead-letter write failed for malformed message: %v", dlqErr)
			return false
		}
		metricDLQMessages.Inc()
		return true
	}

	info, hasInfo := cat.lookup(evt.DeviceID)
	rng, hasRange := metricRange{}, false
	var featureOrder []string
	machineType := ""
	if hasInfo {
		machineType = info.MachineType
		rng, hasRange = info.Ranges[evt.Metric]
		featureOrder = cat.featuresFor(machineType)
	}

	zScore, sampleCount := trackers.update(evt.DeviceID, evt.Metric, evt.Value)

	var forestScore float64
	var hasForest bool
	if hasInfo {
		if vector, ok := fs.observe(evt.DeviceID, machineType, evt.Metric, evt.Value, featureOrder); ok {
			if forest := forests.get(machineType); forest != nil {
				forestScore = forest.Score(vector)
				hasForest = true
			}
		}
	}

	results := runDetectors(evt.Value, rng, hasRange, zScore, sampleCount, cfg, forestScore, hasForest)
	if len(results) == 0 {
		return true
	}

	for _, r := range results {
		metricAnomaliesDetected.WithLabelValues(r.Method).Inc()
	}

	severity, score, methods, reason := combineDetections(results)
	anomaly := events.AnomalyDetected{
		AnomalyID:        uuid.NewString(),
		EventID:          evt.EventID,
		OrganizationID:   evt.OrganizationID,
		FactoryID:        evt.FactoryID,
		ProductionLineID: evt.ProductionLineID,
		MachineID:        evt.MachineID,
		DeviceID:         evt.DeviceID,
		SensorID:         evt.SensorID,
		Metric:           evt.Metric,
		Value:            evt.Value,
		Severity:         severity,
		Score:            score,
		Methods:          methods,
		Reason:           reason,
		DetectedAt:       time.Now().UTC(),
	}

	if err := kio.publishAnomaly(ctx, evt.DeviceID, anomaly); err != nil {
		metricMessagesFailed.WithLabelValues("publish_anomaly").Inc()
		if dlqErr := kio.deadLetter(ctx, msg.Value, err, "publish_anomaly", evt.EventID); dlqErr != nil {
			log.Printf("anomaly-detector: dead-letter write also failed for event_id=%s, leaving unacked: %v", evt.EventID, dlqErr)
			return false
		}
		metricDLQMessages.Inc()
	}

	return true
}
