package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nithya-prakash/indusense/pkg/auth"
)

type deviceDTO struct {
	ID              string `json:"id"`
	SerialNumber    string `json:"serial_number"`
	Status          string `json:"status"`
	FirmwareVersion string `json:"firmware_version"`
}

func handleListDevices(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		limit, offset := parseLimitOffset(r)
		statusFilter := r.URL.Query().Get("status")

		query := `SELECT id, serial_number, status, firmware_version FROM devices WHERE organization_id = $1`
		args := []any{claims.OrganizationID}
		if statusFilter != "" {
			query += " AND status = $2"
			args = append(args, statusFilter)
		}
		query += " ORDER BY serial_number LIMIT $" + strconv.Itoa(len(args)+1) + " OFFSET $" + strconv.Itoa(len(args)+2)
		args = append(args, limit, offset)

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer rows.Close()

		var out []deviceDTO
		for rows.Next() {
			var d deviceDTO
			if err := rows.Scan(&d.ID, &d.SerialNumber, &d.Status, &d.FirmwareVersion); err != nil {
				writeInternalError(w, r, err)
				return
			}
			out = append(out, d)
		}
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, limit, offset))
	}
}

func handleGetDevice(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		var d deviceDTO
		err := pool.QueryRow(r.Context(),
			`SELECT id, serial_number, status, firmware_version FROM devices WHERE id = $1 AND organization_id = $2`,
			id, claims.OrganizationID,
		).Scan(&d.ID, &d.SerialNumber, &d.Status, &d.FirmwareVersion)
		if err != nil {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "device does not exist")
			return
		}
		writeJSON(w, http.StatusOK, d)
	}
}

type sensorDTO struct {
	ID     string  `json:"id"`
	Metric string  `json:"metric"`
	Unit   string  `json:"unit"`
	Min    float64 `json:"min_operating_value"`
	Max    float64 `json:"max_operating_value"`
}

func handleListDeviceSensors(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		deviceID := r.PathValue("id")

		var exists bool
		if err := pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1 AND organization_id = $2)`,
			deviceID, claims.OrganizationID).Scan(&exists); err != nil {
			writeInternalError(w, r, err)
			return
		}
		if !exists {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "device does not exist")
			return
		}

		rows, err := pool.Query(r.Context(),
			`SELECT id, metric, unit, COALESCE(min_operating_value,0), COALESCE(max_operating_value,0) FROM sensors WHERE device_id = $1 ORDER BY metric`,
			deviceID,
		)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer rows.Close()

		var out []sensorDTO
		for rows.Next() {
			var s sensorDTO
			if err := rows.Scan(&s.ID, &s.Metric, &s.Unit, &s.Min, &s.Max); err != nil {
				writeInternalError(w, r, err)
				return
			}
			out = append(out, s)
		}
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, len(out), 0))
	}
}

type provisionDeviceRequest struct {
	MachineID       string `json:"machine_id"`
	SerialNumber    string `json:"serial_number"`
	FirmwareVersion string `json:"firmware_version"`
}

type provisionDeviceResponse struct {
	Device deviceDTO `json:"device"`
	// Secret is returned exactly once, at provisioning time — like a cloud
	// provider's access-key flow, it is never retrievable again afterward
	// since only its bcrypt hash is stored.
	Secret string `json:"secret"`
}

// handleProvisionDevice registers a new device and generates its shared
// secret, returning the plaintext exactly once — mirroring how the seed
// script provisions devices, but through the API and with device:write
// enforced instead of a trusted local script.
func handleProvisionDevice(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())

		var req provisionDeviceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "request body must be valid JSON")
			return
		}
		if req.MachineID == "" || req.SerialNumber == "" {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "machine_id and serial_number are required")
			return
		}

		var machineExists bool
		err := pool.QueryRow(r.Context(), `
			SELECT EXISTS(
				SELECT 1 FROM machines m
				JOIN production_lines pl ON pl.id = m.production_line_id
				JOIN factories f ON f.id = pl.factory_id
				WHERE m.id = $1 AND f.organization_id = $2
			)`, req.MachineID, claims.OrganizationID).Scan(&machineExists)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		if !machineExists {
			writeError(w, r, http.StatusBadRequest, "VALIDATION_ERROR", "machine_id does not belong to your organization")
			return
		}

		secret, err := randomHexSecret(24)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		hash, err := auth.HashPassword(secret)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}

		firmware := req.FirmwareVersion
		if firmware == "" {
			firmware = "unknown"
		}

		var d deviceDTO
		var deviceID string
		err = pool.QueryRow(r.Context(),
			`INSERT INTO devices (machine_id, organization_id, serial_number, status, firmware_version)
			 VALUES ($1, $2, $3, 'PROVISIONED', $4) RETURNING id, serial_number, status, firmware_version`,
			req.MachineID, claims.OrganizationID, req.SerialNumber, firmware,
		).Scan(&deviceID, &d.SerialNumber, &d.Status, &d.FirmwareVersion)
		if err != nil {
			writeError(w, r, http.StatusConflict, "CONFLICT", "a device with this serial_number already exists")
			return
		}
		d.ID = deviceID

		if _, err := pool.Exec(r.Context(),
			`INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active) VALUES ($1, 'shared_secret', $2, true)`,
			deviceID, hash,
		); err != nil {
			writeInternalError(w, r, err)
			return
		}

		writeJSON(w, http.StatusCreated, provisionDeviceResponse{Device: d, Secret: secret})
	}
}

// handleRotateCredentials deactivates the device's current credential and
// issues a new one, returning the new plaintext secret once.
func handleRotateCredentials(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		deviceID := r.PathValue("id")

		var exists bool
		if err := pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM devices WHERE id = $1 AND organization_id = $2)`,
			deviceID, claims.OrganizationID).Scan(&exists); err != nil {
			writeInternalError(w, r, err)
			return
		}
		if !exists {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "device does not exist")
			return
		}

		secret, err := randomHexSecret(24)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		hash, err := auth.HashPassword(secret)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}

		tx, err := pool.Begin(r.Context())
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer tx.Rollback(r.Context()) //nolint:errcheck

		if _, err := tx.Exec(r.Context(), `UPDATE device_credentials SET is_active = false WHERE device_id = $1 AND is_active = true`, deviceID); err != nil {
			writeInternalError(w, r, err)
			return
		}
		if _, err := tx.Exec(r.Context(),
			`INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active, rotated_at) VALUES ($1, 'shared_secret', $2, true, now())`,
			deviceID, hash,
		); err != nil {
			writeInternalError(w, r, err)
			return
		}
		if err := tx.Commit(r.Context()); err != nil {
			writeInternalError(w, r, err)
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"secret": secret})
	}
}

func handleDecommissionDevice(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		deviceID := r.PathValue("id")

		tag, err := pool.Exec(r.Context(),
			`UPDATE devices SET status = 'DECOMMISSIONED', decommissioned_at = now(), updated_at = now() WHERE id = $1 AND organization_id = $2`,
			deviceID, claims.OrganizationID,
		)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		if tag.RowsAffected() == 0 {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "device does not exist")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func randomHexSecret(nBytes int) (string, error) {
	buf := make([]byte, nBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
