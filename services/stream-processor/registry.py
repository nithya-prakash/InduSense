"""Per-(device, metric) series tracking, the Python port of registry.go."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from window import SeriesBuffer


@dataclass(frozen=True)
class SeriesKey:
    """Identifies one (device, metric) time series and carries the tag
    metadata InfluxDB needs — captured once when the series is first seen,
    since it doesn't change across samples for the same device+metric."""

    factory_id: str = ""
    production_line_id: str = ""
    machine_id: str = ""
    device_id: str = ""
    sensor_id: str = ""
    metric: str = ""

    def id(self) -> str:
        return f"{self.device_id}|{self.metric}"


@dataclass
class TrackedSeries:
    key: SeriesKey
    buf: SeriesBuffer


class SeriesRegistry:
    """Tracks one SeriesBuffer per (device, metric) pair seen so far.
    Bounded by the number of distinct sensors in the system (1000 in this
    deployment) — it does not grow with message volume."""

    def __init__(self, max_window_seconds: float):
        self._lock = threading.Lock()
        self._buffers: dict[str, SeriesBuffer] = {}
        self._keys: dict[str, SeriesKey] = {}
        self._max_window_seconds = max_window_seconds

    def record(self, key: SeriesKey, at, value: float) -> None:
        series_id = key.id()
        with self._lock:
            buf = self._buffers.get(series_id)
            if buf is None:
                buf = SeriesBuffer(self._max_window_seconds)
                self._buffers[series_id] = buf
                self._keys[series_id] = key
        buf.add(at, value)

    def snapshot(self) -> list[TrackedSeries]:
        """Returns every tracked series' key and buffer, for a periodic
        flush to iterate without holding the registry lock during I/O."""
        with self._lock:
            return [TrackedSeries(key=self._keys[sid], buf=buf) for sid, buf in self._buffers.items()]
