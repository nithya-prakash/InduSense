"""Wires each service into a single OpenTelemetry TracerProvider exporting
to Jaeger over OTLP/HTTP, plus a Kafka header carrier so trace context
survives the hop across Kafka — otel's built-in propagators only know how
to inject/extract HTTP-style text map carriers, so producers/consumers need
an adapter. Python port of pkg/tracing.

confluent-kafka represents message headers as a list of (str, bytes) tuples
(or None), unlike segmentio/kafka-go's []kafka.Header — the carrier below
adapts that shape instead.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.propagators.textmap import Getter, Setter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_logger = logging.getLogger(__name__)

KafkaHeaders = list[tuple[str, bytes]]


def init(service_name: str) -> Callable[[], None]:
    """Configures the global TracerProvider for the calling service. Reads
    OTEL_EXPORTER_OTLP_ENDPOINT (defaulting to http://localhost:4318,
    matching .env.example) and never raises: if the exporter can't be
    built, tracing is disabled and a no-op shutdown is returned, since a
    missing collector must never take down a service whose job is
    ingesting real telemetry.
    """
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")

    # The exporter wants the full OTLP path; OTEL_EXPORTER_OTLP_ENDPOINT is
    # an OTel-standard *base* URL with no path, so it needs "/v1/traces"
    # appended explicitly — the exact 404-against-Jaeger gotcha the Go
    # version's comment documents, ported here as the same fix rather than
    # rediscovering it.
    parsed = urlparse(endpoint)
    traces_endpoint = endpoint
    if parsed.scheme and parsed.netloc and not parsed.path.rstrip("/").endswith("/v1/traces"):
        traces_endpoint = endpoint.rstrip("/") + "/v1/traces"

    try:
        exporter = OTLPSpanExporter(endpoint=traces_endpoint)
        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: service_name}))
        provider.add_span_processor(BatchSpanProcessor(exporter, schedule_delay_millis=2000))
        trace.set_tracer_provider(provider)
        return lambda: provider.shutdown()
    except Exception as exc:  # noqa: BLE001 - startup must never fail on a missing collector
        _logger.warning("tracing disabled: %s", exc)
        return lambda: None


def tracer(name: str) -> trace.Tracer:
    """Thin convenience so call sites don't import opentelemetry directly."""
    return trace.get_tracer(name)


class _KafkaHeaderGetter(Getter[KafkaHeaders]):
    def get(self, carrier: KafkaHeaders, key: str) -> list[str] | None:
        for h_key, h_value in carrier:
            if h_key == key:
                return [h_value.decode("utf-8")]
        return None

    def keys(self, carrier: KafkaHeaders) -> list[str]:
        return [h_key for h_key, _ in carrier]


class _KafkaHeaderSetter(Setter[KafkaHeaders]):
    def set(self, carrier: KafkaHeaders, key: str, value: str) -> None:
        for i, (h_key, _) in enumerate(carrier):
            if h_key == key:
                carrier[i] = (key, value.encode("utf-8"))
                return
        carrier.append((key, value.encode("utf-8")))


_getter = _KafkaHeaderGetter()
_setter = _KafkaHeaderSetter()


def inject_kafka(headers: KafkaHeaders) -> None:
    """Writes the current span context into headers (in place), so a
    consumer on the other side of the broker can continue the same trace."""
    inject(headers, setter=_setter)


def extract_kafka(headers: KafkaHeaders | None) -> Any:
    """Reads any propagated span context out of a consumed message's
    headers and returns an OTel context a consumer can start child spans
    from. If no trace context is present, the consumer's span simply
    becomes a new trace root."""
    return extract(headers or [], getter=_getter)
