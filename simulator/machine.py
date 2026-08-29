"""Per-device running/stopped state, the Python port of machine.go."""

from __future__ import annotations

import random
import threading


class MachineController:
    """Holds the shared running/stopped state for every sensor that
    belongs to one device. While stopped, sensors on that device produce
    no telemetry (a "missing readings" gap), matching how a real machine
    shutdown silences its instrumentation."""

    def __init__(self):
        self._lock = threading.Lock()
        self._running = True

    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def set_running(self, running: bool) -> None:
        with self._lock:
            self._running = running


def should_toggle(rng: random.Random, running: bool) -> bool:
    """Decides, once per tick, whether a machine controller flips state.
    Stopping is rare (unplanned-shutdown-like); recovering from a stop is
    much more likely per tick so outages stay short relative to uptime."""
    if running:
        return rng.random() < 0.0005  # ~1 in 2000 ticks
    return rng.random() < 0.05  # recovers within ~20 ticks on average
