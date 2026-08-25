-- ANOMALY_COUNT rules need a counting window ("N anomalies within 5 minutes");
-- the other condition types ignore this column.
ALTER TABLE alert_rules ADD COLUMN window_seconds INTEGER NOT NULL DEFAULT 300;

-- Escalation policy support: an unacknowledged alert that stays open past a
-- threshold gets bumped to the next severity rung and re-notified.
ALTER TABLE alerts ADD COLUMN escalation_level INTEGER NOT NULL DEFAULT 0;
ALTER TABLE alerts ADD COLUMN last_escalated_at TIMESTAMPTZ;
