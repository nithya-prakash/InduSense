package main

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/nithya-prakash/indusense/pkg/incidents"
)

type incidentDTO struct {
	ID              string  `json:"id"`
	AlertID         string  `json:"alert_id,omitempty"`
	FactoryID       string  `json:"factory_id,omitempty"`
	MachineID       string  `json:"machine_id,omitempty"`
	DeviceID        string  `json:"device_id,omitempty"`
	SensorID        string  `json:"sensor_id,omitempty"`
	Severity        string  `json:"severity"`
	Status          string  `json:"status"`
	Title           string  `json:"title"`
	Description     string  `json:"description"`
	AssignedTo      string  `json:"assigned_to,omitempty"`
	ResolutionNotes string  `json:"resolution_notes,omitempty"`
	OpenedAt        string  `json:"opened_at"`
	ResolvedAt      *string `json:"resolved_at,omitempty"`
	ClosedAt        *string `json:"closed_at,omitempty"`
}

func toIncidentDTO(inc incidents.Incident) incidentDTO {
	dto := incidentDTO{
		ID: inc.ID, AlertID: inc.AlertID, FactoryID: inc.FactoryID, MachineID: inc.MachineID,
		DeviceID: inc.DeviceID, SensorID: inc.SensorID, Severity: inc.Severity, Status: inc.Status,
		Title: inc.Title, Description: inc.Description, AssignedTo: inc.AssignedTo,
		ResolutionNotes: inc.ResolutionNotes, OpenedAt: inc.OpenedAt.Format(httpTimeFormat),
	}
	if inc.ResolvedAt != nil {
		s := inc.ResolvedAt.Format(httpTimeFormat)
		dto.ResolvedAt = &s
	}
	if inc.ClosedAt != nil {
		s := inc.ClosedAt.Format(httpTimeFormat)
		dto.ClosedAt = &s
	}
	return dto
}

func handleListIncidents(store *incidents.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		limit, offset := parseLimitOffset(r)
		statusFilter := r.URL.Query().Get("status")

		list, err := store.List(r.Context(), claims.OrganizationID, statusFilter, limit, offset)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		out := make([]incidentDTO, len(list))
		for i, inc := range list {
			out[i] = toIncidentDTO(inc)
		}
		writeJSON(w, http.StatusOK, newPaginatedResponse(out, limit, offset))
	}
}

type incidentEventDTO struct {
	EventType string `json:"event_type"`
	OldValue  string `json:"old_value,omitempty"`
	NewValue  string `json:"new_value,omitempty"`
	Note      string `json:"note,omitempty"`
	CreatedAt string `json:"created_at"`
}

func handleGetIncident(store *incidents.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		inc, err := store.Get(r.Context(), claims.OrganizationID, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				writeError(w, r, http.StatusNotFound, "NOT_FOUND", "incident does not exist")
				return
			}
			writeInternalError(w, r, err)
			return
		}

		events, err := store.ListEvents(r.Context(), id)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}
		eventDTOs := make([]incidentEventDTO, len(events))
		for i, e := range events {
			eventDTOs[i] = incidentEventDTO{EventType: e.EventType, OldValue: e.OldValue, NewValue: e.NewValue, Note: e.Note, CreatedAt: e.CreatedAt.Format(httpTimeFormat)}
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"incident": toIncidentDTO(*inc),
			"history":  eventDTOs,
		})
	}
}

type transitionRequest struct {
	Status string `json:"status"`
	Note   string `json:"note"`
}

func handleTransitionIncident(store *incidents.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		var req transitionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Status == "" {
			writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "status is required")
			return
		}

		if _, err := store.Get(r.Context(), claims.OrganizationID, id); err != nil {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "incident does not exist")
			return
		}

		actorUserID := claims.UserID
		if err := store.Transition(r.Context(), id, req.Status, &actorUserID, req.Note); err != nil {
			writeError(w, r, http.StatusConflict, "INVALID_TRANSITION", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

type assignRequest struct {
	UserID string `json:"user_id"`
}

func handleAssignIncident(store *incidents.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		var req assignRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" {
			writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "user_id is required")
			return
		}

		if _, err := store.Get(r.Context(), claims.OrganizationID, id); err != nil {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "incident does not exist")
			return
		}

		actorUserID := claims.UserID
		if err := store.Assign(r.Context(), id, req.UserID, &actorUserID); err != nil {
			writeInternalError(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

type resolveRequest struct {
	ResolutionNotes string `json:"resolution_notes"`
}

func handleResolveIncident(store *incidents.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := claimsFromContext(r.Context())
		id := r.PathValue("id")

		var req resolveRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, r, http.StatusBadRequest, "INVALID_BODY", "request body must be valid JSON")
			return
		}

		if _, err := store.Get(r.Context(), claims.OrganizationID, id); err != nil {
			writeError(w, r, http.StatusNotFound, "NOT_FOUND", "incident does not exist")
			return
		}

		actorUserID := claims.UserID
		if err := store.Resolve(r.Context(), id, req.ResolutionNotes, &actorUserID); err != nil {
			writeError(w, r, http.StatusConflict, "INVALID_TRANSITION", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
