-- Denormalized factory_id on incidents, matching the same convenience
-- column already on alerts — lets the API filter/display incidents by
-- factory without a join through machines -> production_lines -> factories
-- on every list query.
ALTER TABLE incidents ADD COLUMN factory_id UUID REFERENCES factories(id) ON DELETE SET NULL;
CREATE INDEX idx_incidents_factory_id ON incidents(factory_id);
