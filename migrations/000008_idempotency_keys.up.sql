-- Durable idempotency ledger. Used by any consumer/service that must guarantee
-- "process this event_id/request exactly once" on top of at-least-once delivery.
-- Redis may cache lookups for hot paths, but this table is the source of truth.
CREATE TABLE idempotency_keys (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key               TEXT NOT NULL,
    scope             TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'COMPLETED'
                          CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    response_snapshot JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '7 days'),
    UNIQUE (scope, key)
);
CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys(expires_at);
