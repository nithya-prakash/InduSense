"""Command alert-service consumes anomalies.detected (and device.events for
unexpected-shutdown alerts), matches incoming events against
organization-configured alert rules, and creates/escalates alerts in
Postgres with deduplication and cooldown to prevent alert storms —
notifying via whichever providers are configured (console always, webhook
optionally).

Python port of main.go.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
import uuid

from confluent_kafka import KafkaException, Message
from psycopg_pool import ConnectionPool

from anomalycount import AnomalyCountTracker
from conditions import condition_matches
from config import Config, load_config
from health import start_health_server
from kafka_io import KafkaIO
from metrics import (
    alerts_attached_to_incident_total,
    alerts_escalated_total,
    alerts_generated_total,
    alerts_suppressed_total,
    anomalies_consumed_total,
    dlq_messages_total,
    incidents_created_total,
    incidents_open_by_severity,
    incidents_open_total,
    messages_failed_total,
)
from notify import ConsoleProvider, NotificationProvider, Notification, WebhookProvider, notify_all
from rules import RuleCache
from shared import incidents as incidents_module
from shared import logging_utils, tracing
from shared.events import ALERT_EVENT_CREATED, ALERT_EVENT_ESCALATED, AlertEvent, AnomalyDetected, NormalizedMachineEvent, utc_now
from store import CREATED, SUPPRESSED_COOLDOWN, SUPPRESSED_OPEN, Alert, AlertStore, next_severity

MACHINE_SHUTDOWN_METRIC = "machine_status"

logger = logging_utils.init("alert-service")


def main() -> None:
    cfg = load_config()

    shutdown_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown_event.set())
    signal.signal(signal.SIGTERM, lambda *_: shutdown_event.set())

    shutdown_tracing = tracing.init("alert-service")

    # autocommit=True: every statement is its own committed transaction,
    # matching pgx.Pool's per-call behavior in the Go version (no explicit
    # transaction wrapping there either).
    pool = ConnectionPool(cfg.postgres_dsn, min_size=1, max_size=cfg.postgres_max_conns, open=True, kwargs={"autocommit": True})

    try:
        rules = RuleCache(pool)
    except Exception as exc:  # noqa: BLE001
        logger.error("alert-service: load initial rule cache: %s", exc)
        sys.exit(1)

    rule_thread = threading.Thread(target=_run_rule_refresher, args=(shutdown_event, cfg, rules), daemon=False)
    rule_thread.start()

    store = AlertStore(pool)
    incident_store = incidents_module.Store(pool, None)
    counter = AnomalyCountTracker()
    kio = KafkaIO(cfg)

    providers = _build_providers(cfg)
    logger.info("alert-service: notification providers: %s", [p.name() for p in providers])

    escalation_thread = threading.Thread(target=_run_escalation_sweeper, args=(shutdown_event, cfg, store, kio, providers), daemon=False)
    gauge_thread = threading.Thread(target=_run_incident_gauge_refresher, args=(shutdown_event, pool), daemon=False)
    escalation_thread.start()
    gauge_thread.start()

    start_health_server(cfg.http_port, pool)
    logger.info("alert-service: health/metrics server listening on :%s", cfg.http_port)

    anomaly_thread = threading.Thread(
        target=_consume_anomalies, args=(shutdown_event, cfg, kio, rules, store, incident_store, counter, providers), daemon=False
    )
    events_thread = threading.Thread(
        target=_consume_device_events, args=(shutdown_event, cfg, kio, rules, store, incident_store, providers), daemon=False
    )
    anomaly_thread.start()
    events_thread.start()

    logger.info("alert-service: consuming %s and %s as group %s", cfg.topic_anomalies, cfg.topic_device_events, cfg.consumer_group_id)

    anomaly_thread.join()
    events_thread.join()
    rule_thread.join()
    escalation_thread.join()
    gauge_thread.join()
    kio.close()
    pool.close()
    shutdown_tracing()
    logger.info("alert-service: shutdown complete")


def _build_providers(cfg: Config) -> list[NotificationProvider]:
    providers: list[NotificationProvider] = []
    if cfg.notification_console_enabled:
        providers.append(ConsoleProvider())
    if cfg.notification_webhook_url:
        providers.append(WebhookProvider(cfg.notification_webhook_url))
    return providers


def _run_rule_refresher(shutdown_event: threading.Event, cfg: Config, rules: RuleCache) -> None:
    while not shutdown_event.wait(timeout=cfg.rule_refresh_every_seconds):
        try:
            rules.refresh()
        except Exception as exc:  # noqa: BLE001
            logger.error("alert-service: rule cache refresh failed (keeping stale rules): %s", exc)


def _consume_anomalies(shutdown_event, cfg, kio: KafkaIO, rules, store, incident_store, counter, providers) -> None:
    while not shutdown_event.is_set():
        try:
            msg = kio.anomaly_consumer.poll(1.0)
        except KafkaException as exc:
            logger.error("alert-service: anomaly fetch error: %s", exc)
            continue
        if msg is None:
            continue
        if msg.error():
            logger.error("alert-service: anomaly fetch error: %s", msg.error())
            continue

        should_commit = _process_anomaly(cfg, kio, rules, store, incident_store, counter, providers, msg)
        if should_commit:
            try:
                kio.anomaly_consumer.commit(message=msg, asynchronous=False)
            except KafkaException as exc:
                logger.error("alert-service: anomaly commit failed for offset %d: %s", msg.offset(), exc)


def _process_anomaly(cfg: Config, kio: KafkaIO, rules: RuleCache, store: AlertStore, incident_store, counter: AnomalyCountTracker, providers, msg: Message) -> bool:
    anomalies_consumed_total.inc()

    tracing.extract_kafka(msg.headers())
    with tracing.tracer("alert-service").start_as_current_span("alert_service.process_anomaly"):
        try:
            anomaly = AnomalyDetected.model_validate_json(msg.value())
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="unmarshal").inc()
            return _dlq_or_hold(kio, msg.value(), exc, "unmarshal", "", cfg.topic_anomalies)

        for rule in rules.rules_for(anomaly.organization_id, anomaly.metric):
            if not rule.scope_matches(anomaly.machine_id, anomaly.device_id, anomaly.sensor_id):
                continue

            count = 0
            if rule.condition == "ANOMALY_COUNT":
                key = f"{rule.id}|{anomaly.device_id}"
                count = counter.record(key, anomaly.detected_at, rule.window_seconds)

            if not condition_matches(rule, anomaly.value, count):
                continue

            dedupe_key = f"{anomaly.device_id}:{anomaly.metric}"
            alert = Alert(
                organization_id=anomaly.organization_id,
                severity=rule.severity,
                factory_id=anomaly.factory_id,
                machine_id=anomaly.machine_id,
                device_id=anomaly.device_id,
                sensor_id=anomaly.sensor_id,
                title=rule.name,
                description=anomaly.reason,
            )

            try:
                result, created = store.create_if_due(rule.id, rule.cooldown_seconds, alert, dedupe_key)
            except Exception as exc:  # noqa: BLE001
                messages_failed_total.labels(reason="store_alert").inc()
                return _dlq_or_hold(kio, msg.value(), exc, "create_alert", anomaly.event_id, cfg.topic_anomalies)

            if result == CREATED:
                alerts_generated_total.labels(severity=created.severity).inc()
                logger.info(
                    "alert created from anomaly",
                    extra={
                        "event_id": anomaly.event_id, "device_id": anomaly.device_id, "organization_id": anomaly.organization_id,
                        "severity": created.severity, "rule_id": rule.id,
                    },
                )
                _open_or_attach_incident(incident_store, created)
                _publish_and_notify(kio, providers, created, rule.id, ALERT_EVENT_CREATED, False)
            elif result == SUPPRESSED_OPEN:
                alerts_suppressed_total.labels(reason="open").inc()
            elif result == SUPPRESSED_COOLDOWN:
                alerts_suppressed_total.labels(reason="cooldown").inc()

    return True


def _consume_device_events(shutdown_event, cfg, kio: KafkaIO, rules, store, incident_store, providers) -> None:
    while not shutdown_event.is_set():
        try:
            msg = kio.events_consumer.poll(1.0)
        except KafkaException as exc:
            logger.error("alert-service: device event fetch error: %s", exc)
            continue
        if msg is None:
            continue
        if msg.error():
            logger.error("alert-service: device event fetch error: %s", msg.error())
            continue

        should_commit = _process_device_event(cfg, kio, rules, store, incident_store, providers, msg)
        if should_commit:
            try:
                kio.events_consumer.commit(message=msg, asynchronous=False)
            except KafkaException as exc:
                logger.error("alert-service: device event commit failed for offset %d: %s", msg.offset(), exc)


def _process_device_event(cfg: Config, kio: KafkaIO, rules: RuleCache, store: AlertStore, incident_store, providers, msg: Message) -> bool:
    tracing.extract_kafka(msg.headers())
    with tracing.tracer("alert-service").start_as_current_span("alert_service.process_device_event"):
        try:
            evt = NormalizedMachineEvent.model_validate_json(msg.value())
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="unmarshal").inc()
            return _dlq_or_hold(kio, msg.value(), exc, "unmarshal", "", cfg.topic_device_events)

        if evt.event_type != "MACHINE_STOPPED":
            return True  # only unexpected shutdown is an alert-worthy device event today

        shutdown_rules = rules.rules_for(evt.organization_id, MACHINE_SHUTDOWN_METRIC)
        if not shutdown_rules:
            return True  # no sentinel rule seeded for this org -- nothing to dedupe/cooldown against
        rule = shutdown_rules[0]

        dedupe_key = f"shutdown:{evt.machine_id}"
        alert = Alert(
            organization_id=evt.organization_id,
            severity=rule.severity,
            factory_id=evt.factory_id,
            machine_id=evt.machine_id,
            device_id=evt.device_id,
            title="Unexpected machine shutdown",
            description=f"machine {evt.machine_id} stopped unexpectedly",
        )

        try:
            result, created = store.create_if_due(rule.id, rule.cooldown_seconds, alert, dedupe_key)
        except Exception as exc:  # noqa: BLE001
            messages_failed_total.labels(reason="store_alert").inc()
            return _dlq_or_hold(kio, msg.value(), exc, "create_alert", evt.correlation_id, cfg.topic_device_events)

        if result == CREATED:
            alerts_generated_total.labels(severity=created.severity).inc()
            logger.info(
                "alert created from device event",
                extra={
                    "event_id": evt.correlation_id, "device_id": evt.device_id, "organization_id": evt.organization_id,
                    "severity": created.severity, "rule_id": rule.id,
                },
            )
            _open_or_attach_incident(incident_store, created)
            _publish_and_notify(kio, providers, created, rule.id, ALERT_EVENT_CREATED, False)
        elif result == SUPPRESSED_OPEN:
            alerts_suppressed_total.labels(reason="open").inc()
        elif result == SUPPRESSED_COOLDOWN:
            alerts_suppressed_total.labels(reason="cooldown").inc()

    return True


def _open_or_attach_incident(incident_store, alert: Alert) -> None:
    """Links a newly-created alert to an incident, logging but not failing
    message processing on error — an incident-linkage failure shouldn't
    cause an already-durably-recorded alert to be redelivered."""
    ref = incidents_module.AlertRef(
        id=alert.id, organization_id=alert.organization_id, severity=alert.severity,
        factory_id=alert.factory_id, machine_id=alert.machine_id, device_id=alert.device_id, sensor_id=alert.sensor_id,
        title=alert.title, description=alert.description,
    )
    try:
        incident_id, created = incident_store.open_or_attach(ref)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "failed to link alert to an incident",
            extra={"error": str(exc), "alert_id": alert.id, "device_id": alert.device_id, "organization_id": alert.organization_id},
        )
        return
    if created:
        incidents_created_total.inc()
        logger.info(
            "incident opened",
            extra={"incident_id": incident_id, "alert_id": alert.id, "device_id": alert.device_id, "organization_id": alert.organization_id},
        )
    else:
        alerts_attached_to_incident_total.inc()
        logger.info(
            "alert attached to existing incident",
            extra={"incident_id": incident_id, "alert_id": alert.id, "device_id": alert.device_id, "organization_id": alert.organization_id},
        )


def _run_incident_gauge_refresher(shutdown_event: threading.Event, pool: ConnectionPool) -> None:
    while not shutdown_event.wait(timeout=15.0):
        try:
            with pool.connection() as conn:
                count = conn.execute("SELECT count(*) FROM incidents WHERE status NOT IN ('RESOLVED', 'CLOSED')").fetchone()[0]
                incidents_open_total.set(count)

                rows = conn.execute(
                    "SELECT severity, count(*) FROM incidents WHERE status NOT IN ('RESOLVED', 'CLOSED') GROUP BY severity"
                ).fetchall()
        except Exception as exc:  # noqa: BLE001
            logger.error("alert-service: incident gauge refresh failed: %s", exc)
            continue

        seen = set()
        for severity, n in rows:
            incidents_open_by_severity.labels(severity=severity).set(n)
            seen.add(severity)
        for s in ("INFO", "WARNING", "HIGH", "CRITICAL"):
            if s not in seen:
                incidents_open_by_severity.labels(severity=s).set(0)


def _run_escalation_sweeper(shutdown_event: threading.Event, cfg: Config, store: AlertStore, kio: KafkaIO, providers) -> None:
    while not shutdown_event.wait(timeout=cfg.escalation_check_every_seconds):
        try:
            candidates = store.due_for_escalation(cfg.escalation_after_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.error("alert-service: escalation sweep query failed: %s", exc)
            continue

        for a in candidates:
            new_severity = next_severity(a.severity)
            if new_severity == a.severity:
                continue  # already at the top of the ladder
            try:
                store.escalate(a.id, new_severity)
            except Exception as exc:  # noqa: BLE001
                logger.error("alert-service: escalate alert %s failed: %s", a.id, exc)
                continue
            a.severity = new_severity
            a.escalation_level += 1
            alerts_escalated_total.inc()
            _publish_and_notify(kio, providers, a, "", ALERT_EVENT_ESCALATED, True)
            logger.info("alert-service: escalated alert %s to %s (level %d)", a.id, new_severity, a.escalation_level)


def _publish_and_notify(kio: KafkaIO, providers, a: Alert, rule_id: str, event_type: str, is_escalation: bool) -> None:
    evt = AlertEvent(
        alert_id=a.id,
        event_type=event_type,
        organization_id=a.organization_id,
        alert_rule_id=rule_id,
        factory_id=a.factory_id,
        machine_id=a.machine_id,
        device_id=a.device_id,
        sensor_id=a.sensor_id,
        severity=a.severity,
        status="OPEN",
        title=a.title,
        description=a.description,
        escalation_level=a.escalation_level,
        triggered_at=a.triggered_at,
        timestamp=utc_now(),
    )
    try:
        kio.publish_alert(a.device_id, evt)
    except Exception as exc:  # noqa: BLE001
        logger.error("alert-service: failed to publish alert event for alert %s: %s", a.id, exc)

    notify_all(
        providers,
        Notification(
            alert_id=a.id, title=a.title, description=a.description, severity=a.severity,
            machine_id=a.machine_id, device_id=a.device_id, triggered_at=a.triggered_at, is_escalation=is_escalation,
        ),
    )


def _dlq_or_hold(kio: KafkaIO, payload: bytes, cause: Exception, stage: str, correlation_id: str, source_topic: str) -> bool:
    """Routes a message to dead-letter and reports whether the caller
    should commit the offset (True) or leave it for redelivery (False,
    only when the dead-letter write itself also failed)."""
    try:
        kio.dead_letter(payload, cause, stage, correlation_id, source_topic)
    except Exception as exc:  # noqa: BLE001
        logger.error("alert-service: dead-letter write failed, leaving message unacked: %s", exc)
        return False
    dlq_messages_total.inc()
    return True


if __name__ == "__main__":
    main()
