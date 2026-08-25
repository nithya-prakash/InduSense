CREATE TABLE incidents (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id   UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    alert_id          UUID REFERENCES alerts(id) ON DELETE SET NULL,
    machine_id        UUID REFERENCES machines(id) ON DELETE SET NULL,
    device_id         UUID REFERENCES devices(id) ON DELETE SET NULL,
    sensor_id         UUID REFERENCES sensors(id) ON DELETE SET NULL,
    severity          TEXT NOT NULL
                          CHECK (severity IN ('INFO', 'WARNING', 'HIGH', 'CRITICAL')),
    status            TEXT NOT NULL DEFAULT 'OPEN'
                          CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING', 'RESOLVED', 'CLOSED')),
    title             TEXT NOT NULL,
    description       TEXT NOT NULL DEFAULT '',
    assigned_to       UUID REFERENCES users(id) ON DELETE SET NULL,
    resolution_notes  TEXT,
    opened_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,
    closed_at         TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_incidents_organization_id ON incidents(organization_id);
CREATE INDEX idx_incidents_status ON incidents(status);
CREATE INDEX idx_incidents_alert_id ON incidents(alert_id);
-- Prevent unlimited incidents from repeated alerts on the same machine: only one
-- open/active incident per machine at a time (the alert engine attaches new
-- alerts to the existing incident instead of opening a duplicate one).
CREATE UNIQUE INDEX idx_incidents_one_active_per_machine
    ON incidents(machine_id) WHERE status IN ('OPEN', 'ACKNOWLEDGED', 'INVESTIGATING');

CREATE TABLE incident_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_id     UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL
                        CHECK (event_type IN ('STATUS_CHANGE', 'COMMENT', 'ASSIGNMENT', 'ALERT_ATTACHED')),
    actor_user_id   UUID REFERENCES users(id) ON DELETE SET NULL,
    old_value       TEXT,
    new_value       TEXT,
    note            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_incident_events_incident_id ON incident_events(incident_id);
