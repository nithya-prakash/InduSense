CREATE TABLE alert_rules (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id   UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    metric            TEXT NOT NULL,
    condition         TEXT NOT NULL
                          CHECK (condition IN ('GREATER_THAN', 'LESS_THAN', 'OUTSIDE_RANGE', 'ANOMALY_COUNT')),
    threshold_value   NUMERIC,
    threshold_min     NUMERIC,
    threshold_max     NUMERIC,
    severity          TEXT NOT NULL
                          CHECK (severity IN ('INFO', 'WARNING', 'HIGH', 'CRITICAL')),
    cooldown_seconds  INTEGER NOT NULL DEFAULT 300,
    machine_id        UUID REFERENCES machines(id) ON DELETE CASCADE,
    device_id         UUID REFERENCES devices(id) ON DELETE CASCADE,
    sensor_id         UUID REFERENCES sensors(id) ON DELETE CASCADE,
    is_active         BOOLEAN NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_alert_rules_organization_id ON alert_rules(organization_id);
CREATE INDEX idx_alert_rules_is_active ON alert_rules(is_active);

CREATE TABLE alerts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    alert_rule_id   UUID REFERENCES alert_rules(id) ON DELETE SET NULL,
    factory_id      UUID REFERENCES factories(id) ON DELETE SET NULL,
    machine_id      UUID REFERENCES machines(id) ON DELETE SET NULL,
    device_id       UUID REFERENCES devices(id) ON DELETE SET NULL,
    sensor_id       UUID REFERENCES sensors(id) ON DELETE SET NULL,
    severity        TEXT NOT NULL
                        CHECK (severity IN ('INFO', 'WARNING', 'HIGH', 'CRITICAL')),
    status          TEXT NOT NULL DEFAULT 'OPEN'
                        CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'SUPPRESSED', 'RESOLVED')),
    title           TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    dedupe_key      TEXT NOT NULL,
    triggered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_alerts_organization_id ON alerts(organization_id);
CREATE INDEX idx_alerts_status ON alerts(status);
CREATE INDEX idx_alerts_device_id ON alerts(device_id);
-- Deduplication / cooldown: prevent duplicate open alerts for the same rule+dedupe_key.
CREATE UNIQUE INDEX idx_alerts_open_dedupe ON alerts(alert_rule_id, dedupe_key) WHERE status = 'OPEN';
