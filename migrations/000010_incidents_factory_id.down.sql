DROP INDEX IF EXISTS idx_incidents_factory_id;
ALTER TABLE incidents DROP COLUMN IF EXISTS factory_id;
