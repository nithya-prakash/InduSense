package main

import (
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

type factoryDTO struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	City    string `json:"city"`
	Country string `json:"country"`
}

// Every query in this file scopes by claims.OrganizationID from the
// validated JWT — never by anything the client passes in — which is what
// actually enforces multi-tenancy at the backend rather than trusting the
// frontend not to ask for someone else's data.
func handleListFactories(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		limit, offset := parseLimitOffset(r)

		rows, err := pool.Query(r.Context(),
			`SELECT id, name, city, country FROM factories WHERE organization_id = $1 ORDER BY name LIMIT $2 OFFSET $3`,
			claims.OrganizationID, limit, offset,
		)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer rows.Close()

		var out []factoryDTO
		for rows.Next() {
			var f factoryDTO
			if err := rows.Scan(&f.ID, &f.Name, &f.City, &f.Country); err != nil {
				writeInternalError(w, r, err)
				return
			}
			out = append(out, f)
		}
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, limit, offset))
	}
}

func handleGetFactory(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		var f factoryDTO
		err := pool.QueryRow(r.Context(),
			`SELECT id, name, city, country FROM factories WHERE id = $1 AND organization_id = $2`,
			id, claims.OrganizationID,
		).Scan(&f.ID, &f.Name, &f.City, &f.Country)
		if err != nil {
			// A factory belonging to another organization is reported as
			// not-found, not forbidden — never confirm that a cross-tenant
			// resource even exists.
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "factory does not exist")
			return
		}
		writeJSON(w, http.StatusOK, f)
	}
}

type productionLineDTO struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func handleListProductionLines(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		factoryID := r.PathValue("id")

		// Confirm the factory belongs to this tenant before listing its
		// children — otherwise an empty result for someone else's factory
		// ID would look identical to a valid-but-empty one, but a caller
		// probing IDs could still learn that a real factory ID exists
		// versus a wrong one if we skipped this and let a JOIN silently
		// return zero rows either way. Explicit is safer than implicit here.
		var exists bool
		if err := pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM factories WHERE id = $1 AND organization_id = $2)`,
			factoryID, claims.OrganizationID).Scan(&exists); err != nil {
			writeInternalError(w, r, err)
			return
		}
		if !exists {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "factory does not exist")
			return
		}

		rows, err := pool.Query(r.Context(), `SELECT id, name FROM production_lines WHERE factory_id = $1 ORDER BY name`, factoryID)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		defer rows.Close()

		var out []productionLineDTO
		for rows.Next() {
			var pl productionLineDTO
			if err := rows.Scan(&pl.ID, &pl.Name); err != nil {
				writeInternalError(w, r, err)
				return
			}
			out = append(out, pl)
		}
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, len(out), 0))
	}
}

type machineDTO struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	MachineType string `json:"machine_type"`
	Status      string `json:"status"`
}

func handleGetMachine(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		var m machineDTO
		err := pool.QueryRow(r.Context(), `
			SELECT m.id, m.name, m.machine_type, m.status
			FROM machines m
			JOIN production_lines pl ON pl.id = m.production_line_id
			JOIN factories f ON f.id = pl.factory_id
			WHERE m.id = $1 AND f.organization_id = $2
		`, id, claims.OrganizationID).Scan(&m.ID, &m.Name, &m.MachineType, &m.Status)
		if err != nil {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "machine does not exist")
			return
		}
		writeJSON(w, http.StatusOK, m)
	}
}

func handleListMachineDevices(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		machineID := r.PathValue("id")

		rows, err := pool.Query(r.Context(), `
			SELECT d.id, d.serial_number, d.status, d.firmware_version
			FROM devices d
			JOIN machines m ON m.id = d.machine_id
			JOIN production_lines pl ON pl.id = m.production_line_id
			JOIN factories f ON f.id = pl.factory_id
			WHERE m.id = $1 AND f.organization_id = $2
			ORDER BY d.serial_number
		`, machineID, claims.OrganizationID)
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
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, len(out), 0))
	}
}
