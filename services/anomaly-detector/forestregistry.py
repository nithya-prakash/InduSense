"""Per-machine-type isolation forest registry and periodic retrainer, the
Python port of forestregistry.go."""

from __future__ import annotations

import logging
import random
import threading

from config import Config
from featurestore import FeatureStore
from isolationforest import IsolationForest, fit_isolation_forest
from metrics import isolation_forests_trained_total

_logger = logging.getLogger("anomaly-detector")

MIN_TRAINING_SAMPLES = 60


class ForestRegistry:
    """Holds the current isolation forest for each machine type and is
    periodically refreshed from the featureStore's rolling buffers by
    run_forest_trainer. A machine type has no forest at all until its
    buffer has accumulated a reasonable minimum of samples — before that,
    level 3 detection simply doesn't fire for that machine type yet
    (levels 1 and 2 still do)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._forests: dict[str, IsolationForest] = {}

    def get(self, machine_type: str) -> IsolationForest | None:
        with self._lock:
            return self._forests.get(machine_type)

    def set(self, machine_type: str, forest: IsolationForest) -> None:
        with self._lock:
            self._forests[machine_type] = forest

    def trained_machine_types(self) -> list[str]:
        with self._lock:
            return list(self._forests.keys())


def run_forest_trainer(shutdown_event: threading.Event, cfg: Config, fs: FeatureStore, registry: ForestRegistry) -> None:
    rng = random.Random()

    while not shutdown_event.wait(timeout=cfg.forest_retrain_every_seconds):
        for machine_type in fs.machine_types_with_data():
            data = fs.training_snapshot(machine_type)
            if len(data) < MIN_TRAINING_SAMPLES:
                continue
            forest = fit_isolation_forest(data, cfg.forest_num_trees, cfg.forest_subsample_size, rng)
            registry.set(machine_type, forest)
            isolation_forests_trained_total.inc()
            _logger.info("anomaly-detector: retrained isolation forest for machine_type=%s on %d samples", machine_type, len(data))
