"""Fault injection decisions, the Python port of faults.go."""

from __future__ import annotations

import random
from dataclasses import dataclass

from config import Config


@dataclass
class FaultDecision:
    """Captures which fault-injection behaviors apply to a single
    generated sample, decided independently so they can combine (e.g. a
    delayed duplicate)."""

    duplicate: bool = False
    out_of_order: bool = False
    delayed: bool = False
    delay_for: float = 0.0


def decide_faults(rng: random.Random, cfg: Config) -> FaultDecision:
    d = FaultDecision()
    if rng.random() < cfg.duplicate_rate:
        d.duplicate = True
    if rng.random() < cfg.network_delay_rate:
        d.delayed = True
        # 100ms-5s jitter, representative of a congested network link.
        d.delay_for = 0.1 + rng.random() * 4.9
    if rng.random() < cfg.out_of_order_rate:
        d.out_of_order = True
        if not d.delayed:
            # Guarantee the delay is long enough that the *next* tick for
            # this sensor is published first, producing a genuine
            # out-of-order arrival rather than just jitter.
            d.delayed = True
            d.delay_for = 0.5
    return d


def sensor_should_fail(rng: random.Random, currently_failed: bool, failure_rate: float) -> bool:
    """Decides, once per tick, whether a healthy sensor transitions into
    a failed (non-reporting) state, and whether a failed sensor recovers
    this tick."""
    if currently_failed:
        # ~10% chance per tick to recover, i.e. average outage spans ~10 ticks.
        return rng.random() >= 0.10
    return rng.random() < failure_rate
