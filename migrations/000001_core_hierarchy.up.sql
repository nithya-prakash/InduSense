CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE organizations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE factories (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    city            TEXT,
    country         TEXT NOT NULL DEFAULT 'DE',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, name)
);
CREATE INDEX idx_factories_organization_id ON factories(organization_id);

CREATE TABLE production_lines (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    factory_id  UUID NOT NULL REFERENCES factories(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (factory_id, name)
);
CREATE INDEX idx_production_lines_factory_id ON production_lines(factory_id);

CREATE TABLE machines (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    production_line_id  UUID NOT NULL REFERENCES production_lines(id) ON DELETE CASCADE,
    name                TEXT NOT NULL,
    machine_type        TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'IDLE'
                            CHECK (status IN ('RUNNING', 'IDLE', 'MAINTENANCE', 'STOPPED', 'FAULT')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (production_line_id, name)
);
CREATE INDEX idx_machines_production_line_id ON machines(production_line_id);
CREATE INDEX idx_machines_status ON machines(status);

CREATE TABLE devices (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    machine_id          UUID NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    organization_id     UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    serial_number       TEXT NOT NULL UNIQUE,
    status              TEXT NOT NULL DEFAULT 'PROVISIONED'
                            CHECK (status IN ('PROVISIONED', 'ACTIVE', 'OFFLINE', 'MAINTENANCE', 'DECOMMISSIONED')),
    firmware_version    TEXT,
    provisioned_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at        TIMESTAMPTZ,
    decommissioned_at   TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_devices_machine_id ON devices(machine_id);
CREATE INDEX idx_devices_organization_id ON devices(organization_id);
CREATE INDEX idx_devices_status ON devices(status);

CREATE TABLE sensors (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id           UUID NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    metric              TEXT NOT NULL
                            CHECK (metric IN ('temperature', 'vibration', 'pressure', 'rpm',
                                               'current', 'voltage', 'power', 'humidity',
                                               'acoustic_level')),
    unit                TEXT NOT NULL,
    min_operating_value NUMERIC,
    max_operating_value NUMERIC,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (device_id, metric)
);
CREATE INDEX idx_sensors_device_id ON sensors(device_id);
