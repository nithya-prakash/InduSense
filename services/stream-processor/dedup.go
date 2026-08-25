package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// deduplicator claims event_ids in Redis so a duplicate delivery (e.g. the
// MQTT-redelivery duplicate produced during a Kafka outage in Phase 4, or
// any at-least-once retry anywhere upstream) is processed exactly once here.
//
// The claim happens via SETNX *before* the event is processed, so a known
// duplicate is skipped without doing the InfluxDB write or windowed-stats
// work again. This is a deliberate tradeoff: if the process crashes between
// a successful claim and finishing the rest of processing, that one event
// will be treated as "already handled" on redelivery and under-counted in
// the windowed aggregates. That's judged acceptable here because windowed
// stats are informational, not a business-critical record — the InfluxDB
// raw point itself stays correct regardless (same measurement+tags+timestamp
// overwrites idempotently), and Postgres records (alerts/incidents) get
// their own uniqueness constraints downstream where duplicates truly cannot
// be tolerated.
type deduplicator struct {
	client *redis.Client
	ttl    time.Duration
}

func newDeduplicator(cfg Config) *deduplicator {
	return &deduplicator{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		}),
		ttl: cfg.DedupTTL,
	}
}

// claim returns true if this eventID has not been seen before (and is now
// marked as seen), or false if it's a duplicate.
func (d *deduplicator) claim(ctx context.Context, eventID string) (firstSeen bool, err error) {
	key := "dedup:telemetry:" + eventID
	ok, err := d.client.SetNX(ctx, key, 1, d.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis SETNX for dedup key %s: %w", key, err)
	}
	return ok, nil
}

func (d *deduplicator) ping(ctx context.Context) error {
	return d.client.Ping(ctx).Err()
}

func (d *deduplicator) close() error {
	return d.client.Close()
}
