"""Command simulator generates realistic telemetry for every sensor seeded
in Postgres and publishes it over MQTT, with configurable fault injection
(duplicates, out-of-order delivery, network delay, sensor failure, and
machine shutdowns) so downstream services have real, imperfect data to
contend with instead of a clean synthetic stream.

Python port of main.go, with one deliberate concurrency-model change: Go
gives each sensor (up to SENSOR_COUNT, 1000 by default) and each unique
machine its own goroutine -- goroutines cost only a few KB each, so 1000+
of them is unremarkable. A native OS thread per sensor would be a real
scalability concern in Python (each one costs a full OS thread stack), so
this port instead runs every sensor's and machine's timer loop as an
asyncio task on a single event loop -- Python's idiomatic answer to "many
independent, mostly-idle, timer-driven waiters." The actual MQTT publish
still needs a real blocking wait for broker confirmation (paho-mqtt is a
threaded client, not asyncio-native), so that part keeps Go's original
shape: a bounded pool of OS threads (SIM_PUBLISHER_WORKERS, default 32)
draining a thread-safe queue that the asyncio tasks feed into.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import random
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field

from config import Config, load_config
from faults import FaultDecision, decide_faults, sensor_should_fail
from loader import load_sensor_catalog
from machine import MachineController, should_toggle
from model import SensorCatalogEntry
from publisher import MQTTPublisher
from sensorgen import SensorGenerator
from shared.events import MachineEvent, MachineStatusEvent, TelemetryEvent, utc_now

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("simulator")


@dataclass
class PublishJob:
    topic: str
    payload: bytes


@dataclass
class Stats:
    _lock: threading.Lock = field(default_factory=threading.Lock)
    published: int = 0
    duplicates: int = 0
    delayed: int = 0
    out_of_order: int = 0
    anomalies: int = 0
    dropped: int = 0
    sensor_failed: int = 0
    publish_errs: int = 0

    def inc(self, field_name: str, by: int = 1) -> None:
        with self._lock:
            setattr(self, field_name, getattr(self, field_name) + by)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "published": self.published, "duplicates": self.duplicates, "delayed": self.delayed,
                "out_of_order": self.out_of_order, "anomalies": self.anomalies, "dropped": self.dropped,
                "sensor_failed": self.sensor_failed, "publish_errs": self.publish_errs,
            }


def main() -> None:
    cfg = load_config()

    shutdown_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: shutdown_event.set())
    signal.signal(signal.SIGTERM, lambda *_: shutdown_event.set())

    logger.info("simulator: loading sensor catalog from postgres (limit=%d)", cfg.sensor_count)
    catalog = load_sensor_catalog(cfg.postgres_dsn, cfg.sensor_count, cfg.postgres_max_conns)
    if not catalog:
        logger.error("simulator: no sensors found -- run `make seed` first")
        raise SystemExit(1)
    logger.info("simulator: loaded %d sensors", len(catalog))

    pub = MQTTPublisher(cfg.mqtt_broker_url, cfg.mqtt_client_id, cfg.mqtt_qos)

    jobs: "queue.Queue[PublishJob]" = queue.Queue(maxsize=cfg.queue_capacity)
    stats = Stats()

    worker_threads = [
        threading.Thread(target=_publisher_worker, args=(shutdown_event, jobs, pub, stats), daemon=False)
        for _ in range(cfg.publisher_workers)
    ]
    for t in worker_threads:
        t.start()

    stats_thread = threading.Thread(target=_run_stats_reporter, args=(shutdown_event, stats), daemon=False)
    stats_thread.start()

    try:
        asyncio.run(_run_simulation(shutdown_event, cfg, catalog, jobs, stats))
    finally:
        logger.info("simulator: shutdown signal received, draining in-flight sensors...")
        for t in worker_threads:
            t.join()
        stats_thread.join()
        pub.disconnect()

        final = stats.snapshot()
        logger.info(
            "simulator: final stats: published=%d duplicates=%d delayed=%d out_of_order=%d anomalies=%d dropped=%d publish_errors=%d",
            final["published"], final["duplicates"], final["delayed"], final["out_of_order"],
            final["anomalies"], final["dropped"], final["publish_errs"],
        )


def _publisher_worker(shutdown_event: threading.Event, jobs: "queue.Queue[PublishJob]", pub: MQTTPublisher, stats: Stats) -> None:
    while True:
        try:
            job = jobs.get(timeout=0.5)
        except queue.Empty:
            if shutdown_event.is_set() and jobs.empty():
                return
            continue
        try:
            pub.publish(job.topic, job.payload, timeout=5.0)
        except Exception:  # noqa: BLE001
            stats.inc("publish_errs")
        else:
            stats.inc("published")


def _run_stats_reporter(shutdown_event: threading.Event, stats: Stats) -> None:
    while not shutdown_event.wait(timeout=5.0):
        s = stats.snapshot()
        logger.info(
            "simulator stats: published=%d duplicates=%d delayed=%d out_of_order=%d anomalies=%d dropped=%d sensor_failures_active=%d publish_errors=%d",
            s["published"], s["duplicates"], s["delayed"], s["out_of_order"],
            s["anomalies"], s["dropped"], s["sensor_failed"], s["publish_errs"],
        )


async def _run_simulation(shutdown_event: threading.Event, cfg: Config, catalog: list[SensorCatalogEntry], jobs: "queue.Queue[PublishJob]", stats: Stats) -> None:
    def enqueue(topic: str, payload_model) -> None:
        payload = payload_model.model_dump_json().encode("utf-8")
        try:
            jobs.put_nowait(PublishJob(topic=topic, payload=payload))
        except queue.Full:
            # Backpressure: queue is saturated, drop rather than grow
            # unbounded or block the event loop waiting for room.
            stats.inc("dropped")

    machines: dict[str, MachineController] = {}

    def get_machine(device_id: str) -> MachineController:
        mc = machines.get(device_id)
        if mc is None:
            mc = MachineController()
            machines[device_id] = mc
        return mc

    interval = len(catalog) / max(cfg.messages_per_sec, 1)
    if interval <= 0:
        interval = 0.01
    logger.info("simulator: target rate %d msg/s across %d sensors (~%.3fs per sensor)", cfg.messages_per_sec, len(catalog), interval)

    sensor_tasks = []
    for idx, entry in enumerate(catalog):
        seed = time.monotonic_ns() + idx * 7919
        rng = random.Random(seed)
        gen = SensorGenerator(rng, entry.min_value, entry.max_value, cfg.anomaly_rate)
        mc = get_machine(entry.device_id)
        sensor_tasks.append(asyncio.create_task(_run_sensor(shutdown_event, cfg, entry, rng, gen, mc, enqueue, stats, interval)))

    machine_tasks = []
    seen_devices: set[str] = set()
    for e in catalog:
        if e.device_id not in seen_devices:
            seen_devices.add(e.device_id)
            rng = random.Random(time.monotonic_ns())
            machine_tasks.append(asyncio.create_task(
                _run_machine_controller(shutdown_event, e.organization_id, e.device_id, e.factory_id, e.machine_id, get_machine(e.device_id), enqueue, rng)
            ))

    await asyncio.gather(*sensor_tasks, *machine_tasks)


async def _wait_or_shutdown(shutdown_event: threading.Event, seconds: float) -> bool:
    """Returns True if shutdown_event fired before `seconds` elapsed."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while loop.time() < deadline:
        if shutdown_event.is_set():
            return True
        await asyncio.sleep(min(0.05, deadline - loop.time()))
    return shutdown_event.is_set()


async def _run_sensor(
    shutdown_event: threading.Event, cfg: Config, entry: SensorCatalogEntry, rng: random.Random,
    gen: SensorGenerator, mc: MachineController, enqueue, stats: Stats, base_interval: float,
) -> None:
    telemetry_topic = f"factory/{entry.factory_id}/machine/{entry.machine_id}/sensor/{entry.sensor_id}/telemetry"
    events_topic = f"factory/{entry.factory_id}/machine/{entry.machine_id}/events"

    seq = 0
    failed = False

    def jitter() -> float:
        frac = 0.8 + rng.random() * 0.4  # +/-20%
        return base_interval * frac

    while not await _wait_or_shutdown(shutdown_event, jitter()):
        if not mc.is_running():
            continue  # machine is shut down: no readings, i.e. a missing-reading gap

        was_failed = failed
        failed = sensor_should_fail(rng, failed, cfg.sensor_failure_rate)
        if failed != was_failed:
            if failed:
                stats.inc("sensor_failed")
                enqueue(events_topic, MachineEvent(
                    organization_id=entry.organization_id, factory_id=entry.factory_id, machine_id=entry.machine_id,
                    device_id=entry.device_id, sensor_id=entry.sensor_id, event_type="SENSOR_FAILURE", timestamp=utc_now(),
                ))
            else:
                enqueue(events_topic, MachineEvent(
                    organization_id=entry.organization_id, factory_id=entry.factory_id, machine_id=entry.machine_id,
                    device_id=entry.device_id, sensor_id=entry.sensor_id, event_type="SENSOR_RECOVERED", timestamp=utc_now(),
                ))
        if failed:
            continue  # sensor is down: missing reading

        seq += 1
        value, is_anomaly = gen.next()
        if is_anomaly:
            stats.inc("anomalies")

        evt = TelemetryEvent(
            event_id=str(uuid.uuid4()), organization_id=entry.organization_id, factory_id=entry.factory_id,
            production_line_id=entry.production_line_id, machine_id=entry.machine_id, device_id=entry.device_id,
            sensor_id=entry.sensor_id, timestamp=utc_now(), sequence_number=seq, metric=entry.metric,
            value=value, unit=entry.unit,
        )

        fd = decide_faults(rng, cfg)
        await _publish_event(evt, telemetry_topic, fd, enqueue, stats)


async def _publish_event(evt: TelemetryEvent, topic: str, fd: FaultDecision, enqueue, stats: Stats) -> None:
    def send(e: TelemetryEvent) -> None:
        enqueue(topic, e)

    if fd.delayed:
        stats.inc("delayed")
        if fd.out_of_order:
            stats.inc("out_of_order")

        async def delayed_send() -> None:
            await asyncio.sleep(fd.delay_for)
            send(evt)
            if fd.duplicate:
                stats.inc("duplicates")
                send(evt)

        asyncio.create_task(delayed_send())
        return

    send(evt)
    if fd.duplicate:
        stats.inc("duplicates")
        send(evt)


async def _run_machine_controller(
    shutdown_event: threading.Event, organization_id: str, device_id: str, factory_id: str, machine_id: str,
    mc: MachineController, enqueue, rng: random.Random,
) -> None:
    status_topic = f"factory/{factory_id}/machine/{machine_id}/status"
    events_topic = f"factory/{factory_id}/machine/{machine_id}/events"

    while not await _wait_or_shutdown(shutdown_event, 2.0):
        running = mc.is_running()
        if should_toggle(rng, running):
            mc.set_running(not running)
            new_status = "STOPPED" if running else "RUNNING"
            enqueue(status_topic, MachineStatusEvent(
                organization_id=organization_id, factory_id=factory_id, machine_id=machine_id,
                status=new_status, timestamp=utc_now(),
            ))
            enqueue(events_topic, MachineEvent(
                organization_id=organization_id, factory_id=factory_id, machine_id=machine_id, device_id=device_id,
                event_type=f"MACHINE_{new_status}", timestamp=utc_now(),
            ))


if __name__ == "__main__":
    main()
