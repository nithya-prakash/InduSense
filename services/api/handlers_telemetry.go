package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nithya-prakash/indusense/pkg/events"
)

type telemetryPoint struct {
	Time  string  `json:"time"`
	Value float64 `json:"value"`
}

// handleTelemetryLatest returns the most recent reading for a sensor. The
// device_id is verified against the caller's organization in Postgres
// before the InfluxDB query even runs — InfluxDB itself has no concept of
// this system's tenants, so the tenant check has to happen at this layer.
func handleTelemetryLatest(pool *pgxpool.Pool, queryAPI api.QueryAPI, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		deviceID, metric, ok := requireDeviceAndMetric(w, r, pool)
		if !ok {
			return
		}

		flux := fmt.Sprintf(`
			from(bucket: %q)
			  |> range(start: -24h)
			  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r.device_id == %q and r.metric == %q and r._field == "value")
			  |> last()
		`, bucket, deviceID, metric)

		result, err := queryAPI.Query(r.Context(), flux)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer result.Close()

		if !result.Next() {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "no telemetry found for this device/metric in the last 24h")
			return
		}
		rec := result.Record()
		value, _ := rec.Value().(float64)
		writeJSON(w, http.StatusOK, telemetryPoint{Time: rec.Time().Format(httpTimeFormat), Value: value})
	}
}

// handleTelemetryRange returns readings over a caller-selected window —
// one of the fixed presets (5m/1h/24h) or a custom start/end pair, matching
// the query shapes the spec calls out explicitly.
func handleTelemetryRange(pool *pgxpool.Pool, queryAPI api.QueryAPI, bucket string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		deviceID, metric, ok := requireDeviceAndMetric(w, r, pool)
		if !ok {
			return
		}

		rangeClause, err := buildRangeClause(r)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
			return
		}

		flux := fmt.Sprintf(`
			from(bucket: %q)
			  |> range(%s)
			  |> filter(fn: (r) => r._measurement == "sensor_telemetry" and r.device_id == %q and r.metric == %q and r._field == "value")
			  |> sort(columns: ["_time"])
		`, bucket, rangeClause, deviceID, metric)

		result, err := queryAPI.Query(r.Context(), flux)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer result.Close()

		var out []telemetryPoint
		for result.Next() {
			rec := result.Record()
			value, _ := rec.Value().(float64)
			out = append(out, telemetryPoint{Time: rec.Time().Format(httpTimeFormat), Value: value})
		}
		if result.Err() != nil {
			writeInternalError(w, r, result.Err())
			return
		}
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, len(out), 0))
	}
}

func buildRangeClause(r *http.Request) (string, error) {
	if start := r.URL.Query().Get("start"); start != "" {
		end := r.URL.Query().Get("end")
		if _, err := time.Parse(time.RFC3339, start); err != nil {
			return "", fmt.Errorf("start must be RFC3339")
		}
		if end == "" {
			return fmt.Sprintf("start: %s", start), nil
		}
		if _, err := time.Parse(time.RFC3339, end); err != nil {
			return "", fmt.Errorf("end must be RFC3339")
		}
		return fmt.Sprintf("start: %s, stop: %s", start, end), nil
	}

	switch r.URL.Query().Get("range") {
	case "5m", "":
		return "start: -5m", nil
	case "1h":
		return "start: -1h", nil
	case "24h":
		return "start: -24h", nil
	default:
		return "", fmt.Errorf("range must be one of 5m, 1h, 24h, or provide start (and optionally end) as RFC3339 timestamps")
	}
}

// requireDeviceAndMetric validates the query params and confirms the
// device belongs to the caller's organization, writing an error response
// and returning ok=false if not.
func requireDeviceAndMetric(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) (deviceID, metric string, ok bool) {
	claims := claimsFromContext(r.Context())
	deviceID = r.URL.Query().Get("device_id")
	metric = r.URL.Query().Get("metric")
	if deviceID == "" || metric == "" {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "device_id and metric query parameters are required")
		return "", "", false
	}
	// Validated before use in the Postgres lookup (avoids a raw Postgres
	// type-cast error surfacing as a 500) and before interpolation into the
	// Flux query string below (closes off any Flux-injection surface —
	// only a well-formed UUID ever reaches that string).
	if _, err := uuid.Parse(deviceID); err != nil {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "device_id must be a valid UUID")
		return "", "", false
	}
	if !events.ValidMetrics[metric] {
		writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "metric is not a recognized sensor metric")
		return "", "", false
	}

	var exists bool
	if err := pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1 AND organization_id = $2)`,
		deviceID, claims.OrganizationID).Scan(&exists); err != nil {
		writeInternalError(w, r, err)
		return "", "", false
	}
	if !exists {
		writeError(w, r, http.StatusNotFound, "NOT_FOUND", "device does not exist")
		return "", "", false
	}
	return deviceID, metric, true
}

func newInfluxQueryAPI(url, token, org string) api.QueryAPI {
	client := influxdb2.NewClient(url, token)
	return client.QueryAPI(org)
}
