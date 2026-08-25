// Package audit writes security-sensitive actions to the audit_logs table —
// logins, credential rotations, alert/incident modifications, admin
// actions, DLQ retry/discard — so they can be reconstructed later
// regardless of which service performed them.
package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Entry mirrors the audit_logs schema. OrganizationID/UserID/ResourceID/
// IPAddress/RequestID are pointers because several audit-worthy actions
// (a failed login with an unknown email, a system-initiated action) have no
// value for one or more of these.
type Entry struct {
	OrganizationID *string
	UserID         *string
	Action         string
	ResourceType   string
	ResourceID     *string
	IPAddress      *string
	RequestID      *string
	Result         string // SUCCESS | FAILURE
	Metadata       map[string]any
}

const (
	ResultSuccess = "SUCCESS"
	ResultFailure = "FAILURE"
)

type Logger struct {
	pool *pgxpool.Pool
}

func NewLogger(pool *pgxpool.Pool) *Logger {
	return &Logger{pool: pool}
}

func (l *Logger) Log(ctx context.Context, e Entry) error {
	metadata := e.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal audit metadata: %w", err)
	}

	_, err = l.pool.Exec(ctx,
		`INSERT INTO audit_logs (organization_id, user_id, action, resource_type, resource_id, ip_address, request_id, result, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		e.OrganizationID, e.UserID, e.Action, e.ResourceType, e.ResourceID, e.IPAddress, e.RequestID, e.Result, metadataJSON,
	)
	if err != nil {
		return fmt.Errorf("insert audit log entry (action=%s): %w", e.Action, err)
	}
	return nil
}
