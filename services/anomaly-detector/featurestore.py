"""Per-device feature-vector assembly and per-machine-type training buffers,
the Python port of featurestore.go."""

from __future__ import annotations

import threading


class FeatureStore:
    """Tracks the most recently seen value of each metric per device (for
    assembling a device's multivariate feature vector from asynchronous
    single-metric telemetry events — "last known value" per metric,
    updated as readings arrive) and a rolling per-machine-type buffer of
    complete feature vectors used to (re)train that machine type's
    isolation forest."""

    def __init__(self, buffer_size: int):
        self._lock = threading.Lock()
        self._latest: dict[str, dict[str, float]] = {}
        self._training_data: dict[str, list[list[float]]] = {}
        self._buffer_size = buffer_size

    def observe(self, device_id: str, machine_type: str, metric: str, value: float, feature_order: list[str]) -> tuple[list[float] | None, bool]:
        """Records a new reading and, if the device now has a value for
        every metric in its machine type's feature order, returns the
        assembled feature vector (ok=True) and appends it to that machine
        type's training buffer."""
        with self._lock:
            values = self._latest.setdefault(device_id, {})
            values[metric] = value

            if not feature_order:
                return None, False

            vector = []
            for m in feature_order:
                if m not in values:
                    return None, False  # still waiting on at least one metric for this device
                vector.append(values[m])

            buf = self._training_data.setdefault(machine_type, [])
            buf.append(vector)
            if len(buf) > self._buffer_size:
                del buf[: len(buf) - self._buffer_size]

            return vector, True

    def training_snapshot(self, machine_type: str) -> list[list[float]]:
        """Returns a copy of the current training buffer for a machine
        type, safe to hand to fit_isolation_forest without holding the
        lock during training."""
        with self._lock:
            return list(self._training_data.get(machine_type, []))

    def machine_types_with_data(self) -> list[str]:
        """Returns every machine type that currently has at least one
        training sample, for the periodic retrain loop to iterate over."""
        with self._lock:
            return list(self._training_data.keys())
