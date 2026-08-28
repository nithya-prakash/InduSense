package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// loadSensorCatalog reads every sensor along with the full organization ->
// factory -> machine -> device path needed to build MQTT topics and
// telemetry events.
// maxConns is explicit rather than pgxpool's own default (max(4, NumCPU) —
// see SIM_POSTGRES_MAX_CONNS) since this pool exists only for the one
// startup query below, not for sustained load.
func loadSensorCatalog(ctx context.Context, dsn string, limit, maxConns int) ([]SensorCatalogEntry, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	poolCfg.MaxConns = int32(maxConns)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `
		SELECT o.id, f.id, pl.id, m.id, d.id, s.id, s.metric, s.unit,
		       COALESCE(s.min_operating_value, 0), COALESCE(s.max_operating_value, 100)
		FROM sensors s
		JOIN devices d ON d.id = s.device_id
		JOIN machines m ON m.id = d.machine_id
		JOIN production_lines pl ON pl.id = m.production_line_id
		JOIN factories f ON f.id = pl.factory_id
		JOIN organizations o ON o.id = f.organization_id
		ORDER BY s.id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query sensor catalog: %w", err)
	}
	defer rows.Close()

	var entries []SensorCatalogEntry
	for rows.Next() {
		var e SensorCatalogEntry
		if err := rows.Scan(&e.OrganizationID, &e.FactoryID, &e.ProductionLineID, &e.MachineID, &e.DeviceID,
			&e.SensorID, &e.Metric, &e.Unit, &e.MinValue, &e.MaxValue); err != nil {
			return nil, fmt.Errorf("scan sensor row: %w", err)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sensor rows: %w", err)
	}
	return entries, nil
}
