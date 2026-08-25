package main

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/nithya-prakash/indusense/pkg/reliability"
)

// influxSink wraps the InfluxDB client with retry-with-backoff and a circuit
// breaker, matching the same pattern used for Kafka writes in ingestion —
// InfluxDB is just as much an external dependency that can be temporarily
// unavailable, and deserves the same fail-fast-during-an-outage treatment.
type influxSink struct {
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	org      string
	bucket   string

	breaker    *reliability.CircuitBreaker
	maxRetries int
	retryDelay time.Duration
}

func newInfluxSink(cfg Config) *influxSink {
	client := influxdb2.NewClient(cfg.InfluxURL, cfg.InfluxToken)
	return &influxSink{
		client:     client,
		writeAPI:   client.WriteAPIBlocking(cfg.InfluxOrg, cfg.InfluxBucket),
		org:        cfg.InfluxOrg,
		bucket:     cfg.InfluxBucket,
		breaker:    reliability.NewCircuitBreaker(cfg.BreakerFailureThreshold, cfg.BreakerCooldown),
		maxRetries: cfg.InfluxMaxRetries,
		retryDelay: cfg.InfluxRetryBaseDelay,
	}
}

func (s *influxSink) close() {
	s.client.Close()
}

func (s *influxSink) ping(ctx context.Context) error {
	_, err := s.client.Ping(ctx)
	return err
}

func (s *influxSink) breakerState() string {
	return s.breaker.State()
}

// writeRawPoint writes one sensor_telemetry point. Measurement/tags/fields
// match docs/DATABASE.md exactly: tags identify the hierarchy path + metric,
// the only field is value. Writing the same (tags, timestamp) twice is a
// safe no-op overwrite, which is what makes this naturally idempotent under
// at-least-once delivery.
func (s *influxSink) writeRawPoint(ctx context.Context, key seriesKey, at time.Time, value float64) error {
	p := write.NewPoint("sensor_telemetry",
		map[string]string{
			"factory_id":         key.FactoryID,
			"production_line_id": key.ProductionLineID,
			"machine_id":         key.MachineID,
			"device_id":          key.DeviceID,
			"sensor_id":          key.SensorID,
			"metric":             key.Metric,
		},
		map[string]any{"value": value},
		at,
	)
	return s.writeWithProtection(ctx, p)
}

// buildAggregatePoint constructs (without writing) one sensor_telemetry_agg
// point for a given window (e.g. "1m"), for batching by the caller.
func buildAggregatePoint(key seriesKey, window string, at time.Time, stats windowStats) *write.Point {
	return write.NewPoint("sensor_telemetry_agg",
		map[string]string{
			"factory_id":         key.FactoryID,
			"production_line_id": key.ProductionLineID,
			"machine_id":         key.MachineID,
			"device_id":          key.DeviceID,
			"sensor_id":          key.SensorID,
			"metric":             key.Metric,
			"window":             window,
		},
		map[string]any{
			"moving_avg":     stats.MovingAvg,
			"moving_stddev":  stats.MovingStdDev,
			"min":            stats.Min,
			"max":            stats.Max,
			"rate_of_change": stats.RateOfChange,
			"count":          stats.Count,
		},
		at,
	)
}

func (s *influxSink) writeWithProtection(ctx context.Context, points ...*write.Point) error {
	if !s.breaker.Allow() {
		return fmt.Errorf("circuit breaker open for influxdb")
	}

	err := reliability.RetryWithBackoff(ctx, s.maxRetries, s.retryDelay, func(d time.Duration) { time.Sleep(d) }, func() error {
		return s.writeAPI.WritePoint(ctx, points...)
	})

	if err != nil {
		s.breaker.RecordFailure()
		return err
	}
	s.breaker.RecordSuccess()
	return nil
}

// writeAggregateBatch writes many aggregate points (one per series x window,
// for a single flush tick) as a single InfluxDB call instead of one round
// trip per point — with up to ~1000 sensors x 5 windows per flush, that
// difference matters.
func (s *influxSink) writeAggregateBatch(ctx context.Context, points []*write.Point) error {
	if len(points) == 0 {
		return nil
	}
	return s.writeWithProtection(ctx, points...)
}
