package main

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type metricRange struct {
	Min, Max float64
}

type deviceInfo struct {
	MachineType string
	Ranges      map[string]metricRange // metric -> operating range, for rule-based detection
}

// catalog holds a periodically-refreshed snapshot of device metadata needed
// for detection: each device's machine type (for grouping isolation forests)
// and each sensor's operating range (for rule-based thresholds). It's kept
// in memory and refreshed on a timer rather than queried per-event, since
// this metadata changes rarely relative to telemetry volume.
type catalog struct {
	mu           sync.RWMutex
	devices      map[string]deviceInfo // device_id -> info
	featureOrder map[string][]string   // machine_type -> ordered metric names (the isolation forest's feature vector shape)
	pool         *pgxpool.Pool
}

func newCatalog(ctx context.Context, dsn string) (*catalog, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	c := &catalog{
		devices:      make(map[string]deviceInfo),
		featureOrder: make(map[string][]string),
		pool:         pool,
	}
	if err := c.refresh(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return c, nil
}

func (c *catalog) close() {
	c.pool.Close()
}

func (c *catalog) refresh(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT d.id, m.machine_type, s.metric,
		       COALESCE(s.min_operating_value, 0), COALESCE(s.max_operating_value, 100)
		FROM devices d
		JOIN machines m ON m.id = d.machine_id
		JOIN sensors s ON s.device_id = d.id
	`)
	if err != nil {
		return fmt.Errorf("query catalog: %w", err)
	}
	defer rows.Close()

	devices := make(map[string]deviceInfo)
	metricsByType := make(map[string]map[string]bool)

	for rows.Next() {
		var deviceID, machineType, metric string
		var min, max float64
		if err := rows.Scan(&deviceID, &machineType, &metric, &min, &max); err != nil {
			return fmt.Errorf("scan catalog row: %w", err)
		}

		info, ok := devices[deviceID]
		if !ok {
			info = deviceInfo{MachineType: machineType, Ranges: make(map[string]metricRange)}
		}
		info.Ranges[metric] = metricRange{Min: min, Max: max}
		devices[deviceID] = info

		if metricsByType[machineType] == nil {
			metricsByType[machineType] = make(map[string]bool)
		}
		metricsByType[machineType][metric] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate catalog rows: %w", err)
	}

	featureOrder := make(map[string][]string, len(metricsByType))
	for machineType, metrics := range metricsByType {
		ordered := make([]string, 0, len(metrics))
		for m := range metrics {
			ordered = append(ordered, m)
		}
		sort.Strings(ordered) // deterministic feature-vector dimension order
		featureOrder[machineType] = ordered
	}

	c.mu.Lock()
	c.devices = devices
	c.featureOrder = featureOrder
	c.mu.Unlock()
	return nil
}

func (c *catalog) lookup(deviceID string) (deviceInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.devices[deviceID]
	return info, ok
}

func (c *catalog) featuresFor(machineType string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.featureOrder[machineType]
}
