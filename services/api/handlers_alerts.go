package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type alertDTO struct {
	ID          string     `json:"id"`
	Severity    string     `json:"severity"`
	Status      string     `json:"status"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	MachineID   string     `json:"machine_id,omitempty"`
	DeviceID    string     `json:"device_id,omitempty"`
	TriggeredAt time.Time  `json:"triggered_at"`
	ResolvedAt  *time.Time `json:"resolved_at,omitempty"`
}

func handleListAlerts(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		limit, offset := parseLimitOffset(r)

		query := `SELECT id, severity, status, title, description, COALESCE(machine_id::text,''), COALESCE(device_id::text,''), triggered_at, resolved_at
		          FROM alerts WHERE organization_id = $1`
		args := []any{claims.OrganizationID}

		if status := r.URL.Query().Get("status"); status != "" {
			args = append(args, status)
			query += " AND status = $2"
		}
		if severity := r.URL.Query().Get("severity"); severity != "" {
			args = append(args, severity)
			query += " AND severity = $" + strconv.Itoa(len(args))
		}
		query += " ORDER BY triggered_at DESC LIMIT $" + strconv.Itoa(len(args)+1) + " OFFSET $" + strconv.Itoa(len(args)+2)
		args = append(args, limit, offset)

		rows, err := pool.Query(r.Context(), query, args...)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer rows.Close()

		var out []alertDTO
		for rows.Next() {
			var a alertDTO
			if err := rows.Scan(&a.ID, &a.Severity, &a.Status, &a.Title, &a.Description, &a.MachineID, &a.DeviceID, &a.TriggeredAt, &a.ResolvedAt); err != nil {
				writeInternalError(w, r, err)
				return
			}
			out = append(out, a)
		}
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, limit, offset))
	}
}

func handleGetAlert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		var a alertDTO
		err := pool.QueryRow(r.Context(),
			`SELECT id, severity, status, title, description, COALESCE(machine_id::text,''), COALESCE(device_id::text,''), triggered_at, resolved_at
			 FROM alerts WHERE id = $1 AND organization_id = $2`,
			id, claims.OrganizationID,
		).Scan(&a.ID, &a.Severity, &a.Status, &a.Title, &a.Description, &a.MachineID, &a.DeviceID, &a.TriggeredAt, &a.ResolvedAt)
		if err != nil {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "alert does not exist")
			return
		}
		writeJSON(w, http.StatusOK, a)
	}
}

// handleAcknowledgeAlert moves an alert from OPEN to ACKNOWLEDGED — the
// same "someone is looking at this" signal as an incident acknowledgment,
// but for the alert record itself, before or independent of any incident.
func handleAcknowledgeAlert(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		tag, err := pool.Exec(r.Context(),
			`UPDATE alerts SET status = 'ACKNOWLEDGED', updated_at = now() WHERE id = $1 AND organization_id = $2 AND status = 'OPEN'`,
			id, claims.OrganizationID,
		)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		if tag.RowsAffected() == 0 {
			writeError(w, r, http.StatusConflict, "CONFLICT", "alert does not exist, belongs to another organization, or is not OPEN")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
