package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// errorResponse matches the spec's consistent error envelope exactly:
// {"error": {"code", "message", "request_id"}}. Internal error details
// (stack traces, raw DB errors) are never included in the response —
// they're logged server-side with the request ID for correlation.
type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: errorBody{
		Code:      code,
		Message:   message,
		RequestID: requestIDFromContext(r.Context()),
	}})
}

// writeInternalError logs the real error (server-side only) and returns a
// generic message to the client — never leaking internals, per spec.
func writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("api: internal error [request_id=%s]: %v", requestIDFromContext(r.Context()), err)
	writeError(w, r, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type paginatedResponse[T any] struct {
	Items         []T `json:"items"`
	Limit         int `json:"limit"`
	Offset        int `json:"offset"`
	ReturnedCount int `json:"returned_count"`
}

func newPaginatedResponse[T any](items []T, limit, offset int) paginatedResponse[T] {
	if items == nil {
		items = []T{}
	}
	return paginatedResponse[T]{Items: items, Limit: limit, Offset: offset, ReturnedCount: len(items)}
}
