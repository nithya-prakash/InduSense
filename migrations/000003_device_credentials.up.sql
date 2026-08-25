CREATE TABLE device_credentials (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id       UUID NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    credential_type TEXT NOT NULL DEFAULT 'shared_secret'
                        CHECK (credential_type IN ('shared_secret', 'certificate')),
    credential_hash TEXT NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    rotated_at      TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_device_credentials_device_id ON device_credentials(device_id);
-- Only one active credential per device at a time; rotation deactivates the old one.
CREATE UNIQUE INDEX idx_device_credentials_one_active
    ON device_credentials(device_id) WHERE is_active;
