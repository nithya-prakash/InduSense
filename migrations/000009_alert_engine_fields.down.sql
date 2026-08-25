ALTER TABLE alerts DROP COLUMN IF EXISTS last_escalated_at;
ALTER TABLE alerts DROP COLUMN IF EXISTS escalation_level;
ALTER TABLE alert_rules DROP COLUMN IF EXISTS window_seconds;
