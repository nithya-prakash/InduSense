"""Notification providers, the Python port of notify.go."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import requests

from metrics import notifications_failed_total, notifications_sent_total
from shared.reliability import CircuitBreaker, ErrPermanent, retry_with_backoff

_logger = logging.getLogger("alert-service")


@dataclass
class Notification:
    """The provider-agnostic shape every NotificationProvider sends —
    deliberately independent of the Postgres Alert row or the Kafka
    AlertEvent so providers don't need to know about either."""

    alert_id: str
    title: str
    description: str
    severity: str
    machine_id: str
    device_id: str
    triggered_at: datetime
    is_escalation: bool = False


class NotificationProvider(Protocol):
    """Intentionally minimal so new channels (email, Slack, PagerDuty,
    ...) can be added without touching the alert engine itself — see
    docs/RELIABILITY.md for why console/webhook are the only ones actually
    implemented here."""

    def send(self, n: Notification) -> None: ...
    def name(self) -> str: ...


class ConsoleProvider:
    """Logs the notification. It's the default in local development and
    CI, requiring no external service."""

    def name(self) -> str:
        return "console"

    def send(self, n: Notification) -> None:
        tag = "ALERT ESCALATED" if n.is_escalation else "ALERT"
        _logger.info(
            "[%s] severity=%s title=%r machine=%s device=%s alert_id=%s: %s",
            tag, n.severity, n.title, n.machine_id, n.device_id, n.alert_id, n.description,
        )


class WebhookProvider:
    """POSTs a JSON payload to a configured URL — the local, no-paid-service
    stand-in for Slack/PagerDuty/etc. A failure here is logged and
    counted, never dead-lettered: the alert itself is already durably
    recorded in Postgres by the time notification is attempted, so a
    webhook outage means a missed notification, not lost alert data."""

    def __init__(self, url: str):
        self._url = url
        self._breaker = CircuitBreaker(5, 30.0)

    def name(self) -> str:
        return "webhook"

    def send(self, n: Notification) -> None:
        if not self._breaker.allow():
            raise RuntimeError("webhook circuit breaker open")

        payload = {
            "alert_id": n.alert_id,
            "title": n.title,
            "description": n.description,
            "severity": n.severity,
            "machine_id": n.machine_id,
            "device_id": n.device_id,
            "triggered_at": n.triggered_at.isoformat(),
            "is_escalation": n.is_escalation,
        }

        def attempt() -> None:
            try:
                resp = requests.post(self._url, json=payload, timeout=5.0)
            except requests.RequestException as exc:
                raise RuntimeError(str(exc)) from exc
            if resp.status_code >= 500:
                raise RuntimeError(f"webhook returned {resp.status_code}")
            if resp.status_code >= 400:
                raise ErrPermanent(f"webhook returned {resp.status_code}")

        try:
            retry_with_backoff(3, 0.5, attempt)
        except Exception:
            self._breaker.record_failure()
            raise
        self._breaker.record_success()


def notify_all(providers: list[NotificationProvider], n: Notification) -> None:
    """Fans a notification out to every configured provider, independently
    — one provider's failure never blocks another's."""
    for p in providers:
        try:
            p.send(n)
        except Exception as exc:  # noqa: BLE001
            notifications_failed_total.labels(provider=p.name()).inc()
            _logger.error("alert-service: notification via %s failed: %s", p.name(), exc)
        else:
            notifications_sent_total.labels(provider=p.name()).inc()
