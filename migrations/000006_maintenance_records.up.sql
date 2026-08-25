CREATE TABLE maintenance_records (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    machine_id        UUID NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    device_id         UUID REFERENCES devices(id) ON DELETE SET NULL,
    performed_by      UUID REFERENCES users(id) ON DELETE SET NULL,
    maintenance_type  TEXT NOT NULL
                          CHECK (maintenance_type IN ('PREVENTIVE', 'CORRECTIVE', 'INSPECTION')),
    description       TEXT NOT NULL DEFAULT '',
    performed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_due_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_maintenance_records_machine_id ON maintenance_records(machine_id);
