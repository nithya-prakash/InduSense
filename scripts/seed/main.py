"""Command seed populates the factory hierarchy (organization, factories,
production lines, machines, devices, sensors) used by the sensor simulator
and the rest of the platform. It is idempotent: re-running it against an
already-seeded database is a no-op (it checks for an existing organization
slug before inserting anything).

Python port of scripts/seed/main.go.
"""

from __future__ import annotations

import logging
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from psycopg_pool import ConnectionPool

from shared import auth

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("seed")


@dataclass
class MetricSpec:
    unit: str
    min_value: float
    max_value: float


METRIC_SPECS: dict[str, MetricSpec] = {
    "temperature": MetricSpec("celsius", 20, 90),
    "vibration": MetricSpec("mm_s", 0, 10),
    "pressure": MetricSpec("bar", 0, 250),
    "rpm": MetricSpec("rpm", 0, 3000),
    "current": MetricSpec("ampere", 0, 100),
    "voltage": MetricSpec("volt", 200, 400),
    "power": MetricSpec("kilowatt", 0, 50),
    "humidity": MetricSpec("percent", 10, 80),
    "acoustic_level": MetricSpec("decibel", 40, 110),
}


@dataclass
class MachineProfile:
    """Maps a realistic German-manufacturing machine type to the five
    sensor metrics it is instrumented with."""

    machine_type: str
    metrics: tuple[str, str, str, str, str]


MACHINE_PROFILES: list[MachineProfile] = [
    MachineProfile("CNC_MILLING_MACHINE", ("temperature", "vibration", "rpm", "current", "power")),
    MachineProfile("HYDRAULIC_PRESS", ("temperature", "pressure", "vibration", "current", "power")),
    MachineProfile("CONVEYOR_BELT", ("temperature", "current", "vibration", "power", "rpm")),
    MachineProfile("WELDING_ROBOT", ("temperature", "current", "voltage", "power", "vibration")),
    MachineProfile("AIR_COMPRESSOR", ("temperature", "pressure", "vibration", "current", "power")),
]

GERMAN_FACTORIES = [
    ("Berlin Plant", "Berlin"),
    ("Dresden Plant", "Dresden"),
    ("Munich Plant", "Munich"),
    ("Hamburg Plant", "Hamburg"),
]

# demo_password is the local-development-only password for every seeded
# demo user. It is not a secret -- this is throwaway data for a local
# Docker Compose environment, documented in the README, never used
# anywhere real credentials would be.
DEMO_PASSWORD = "ChangeMe123!"


def env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    if value:
        try:
            return int(value)
        except ValueError:
            pass
    return default


def random_status(weights: dict[str, int]) -> str:
    total = sum(weights.values())
    roll = secrets.randbelow(total)
    for status, w in weights.items():
        if roll < w:
            return status
        roll -= w
    return next(iter(weights))


def main() -> None:
    lines_per_factory = env_int("SEED_LINES_PER_FACTORY", 5)
    machines_per_line = env_int("SEED_MACHINES_PER_LINE", 10)

    dsn = os.environ.get("SEED_POSTGRES_DSN") or "postgres://indusense:indusense_dev_password@localhost:5432/indusense?sslmode=disable"

    # A one-shot CLI tool doesn't need many connections; still explicit
    # (not a library default) and still configurable, per the same
    # convention as the long-running services.
    pool = ConnectionPool(dsn, min_size=1, max_size=env_int("SEED_POSTGRES_MAX_CONNS", 4), open=True, kwargs={"autocommit": True}, timeout=60.0)

    with pool.connection() as conn:
        conn.execute("SELECT 1")  # fail fast if unreachable, matching Go's explicit Ping

    org_slug = "musterfabrik-gmbh"

    with pool.connection() as conn:
        row = conn.execute("SELECT id FROM organizations WHERE slug = %s", (org_slug,)).fetchone()

    if row is not None:
        org_id = str(row[0])
        logger.info("organization %r already seeded (id=%s) -- skipping hierarchy, seed is idempotent", org_slug, org_id)
        _seed_supporting_data(pool, org_id)
        return

    org_id = _seed_factory_hierarchy(pool, org_slug, lines_per_factory, machines_per_line)
    _seed_supporting_data(pool, org_id)


def _seed_factory_hierarchy(pool: ConnectionPool, org_slug: str, lines_per_factory: int, machines_per_line: int) -> str:
    factory_count = line_count = machine_count = device_count = sensor_count = cred_count = 0

    with pool.connection() as conn:
        with conn.transaction():
            org_id = str(conn.execute(
                "INSERT INTO organizations (name, slug) VALUES (%s, %s) RETURNING id", ("Musterfabrik GmbH", org_slug)
            ).fetchone()[0])
            logger.info("created organization Musterfabrik GmbH (id=%s)", org_id)

            for factory_name, city in GERMAN_FACTORIES:
                factory_id = str(conn.execute(
                    "INSERT INTO factories (organization_id, name, city, country) VALUES (%s, %s, %s, 'DE') RETURNING id",
                    (org_id, factory_name, city),
                ).fetchone()[0])
                factory_count += 1

                for line_idx in range(1, lines_per_factory + 1):
                    line_name = f"Line {line_idx:02d}"
                    line_id = str(conn.execute(
                        "INSERT INTO production_lines (factory_id, name) VALUES (%s, %s) RETURNING id", (factory_id, line_name)
                    ).fetchone()[0])
                    line_count += 1

                    for machine_idx in range(1, machines_per_line + 1):
                        profile = MACHINE_PROFILES[(machine_idx - 1) % len(MACHINE_PROFILES)]
                        machine_name = f"{city}-{line_name}-M{machine_idx:03d}"
                        machine_status = random_status({"RUNNING": 70, "IDLE": 15, "MAINTENANCE": 8, "FAULT": 5, "STOPPED": 2})

                        machine_id = str(conn.execute(
                            "INSERT INTO machines (production_line_id, name, machine_type, status) VALUES (%s, %s, %s, %s) RETURNING id",
                            (line_id, machine_name, profile.machine_type, machine_status),
                        ).fetchone()[0])
                        machine_count += 1

                        serial = f"SN-{city}-{profile.machine_type}-{line_idx:02d}-{machine_idx:03d}"
                        device_status = random_status({"ACTIVE": 85, "OFFLINE": 8, "MAINTENANCE": 5, "PROVISIONED": 2})

                        activated_at = None
                        if device_status in ("ACTIVE", "OFFLINE", "MAINTENANCE"):
                            activated_at = datetime.now(timezone.utc) - timedelta(hours=machine_idx)

                        device_id = str(conn.execute(
                            """
                            INSERT INTO devices (machine_id, organization_id, serial_number, status, firmware_version, activated_at)
                            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                            """,
                            (machine_id, org_id, serial, device_status, "fw-1.4.2", activated_at),
                        ).fetchone()[0])
                        device_count += 1

                        secret = secrets.token_hex(24)
                        credential_hash = auth.hash_password(secret)
                        conn.execute(
                            "INSERT INTO device_credentials (device_id, credential_type, credential_hash, is_active) VALUES (%s, 'shared_secret', %s, true)",
                            (device_id, credential_hash),
                        )
                        cred_count += 1

                        for metric in profile.metrics:
                            spec = METRIC_SPECS[metric]
                            conn.execute(
                                "INSERT INTO sensors (device_id, metric, unit, min_operating_value, max_operating_value) VALUES (%s, %s, %s, %s, %s)",
                                (device_id, metric, spec.unit, spec.min_value, spec.max_value),
                            )
                            sensor_count += 1

    logger.info(
        "seed complete: %d factories, %d production lines, %d machines, %d devices, %d device credentials, %d sensors",
        factory_count, line_count, machine_count, device_count, cred_count, sensor_count,
    )
    return org_id


def _seed_supporting_data(pool: ConnectionPool, org_id: str) -> None:
    """Seeds everything that isn't part of the core factory hierarchy
    transaction: alert rules, RBAC roles/permissions, demo users, and a
    second organization for tenant-isolation testing. Each step is
    independently idempotent."""
    _seed_alert_rules_if_missing(pool, org_id)
    _seed_rbac_if_missing(pool)
    _seed_users_if_missing(pool, org_id)
    _seed_second_organization_if_missing(pool)


@dataclass
class AlertRuleSeed:
    name: str
    metric: str
    condition: str
    threshold_value: float | None
    severity: str
    cooldown_secs: int
    window_secs: int


def _seed_alert_rules_if_missing(pool: ConnectionPool, org_id: str) -> None:
    """Inserts a handful of representative, organization-wide alert rules
    (scoped by metric only, not to a specific machine/device/sensor)
    matching the examples from the spec: a hard temperature threshold, a
    vibration threshold, a power-spike threshold, and an anomaly-count
    rule ("three anomalies within five minutes"). Idempotent: skipped
    entirely if this organization already has any alert rules."""
    with pool.connection() as conn:
        existing = conn.execute("SELECT count(*) FROM alert_rules WHERE organization_id = %s", (org_id,)).fetchone()[0]
        if existing > 0:
            logger.info("organization already has %d alert rule(s) -- skipping, seed is idempotent", existing)
            return

        rules = [
            AlertRuleSeed("High temperature", "temperature", "GREATER_THAN", 90.0, "CRITICAL", 300, 300),
            AlertRuleSeed("Excessive vibration", "vibration", "GREATER_THAN", 8.0, "HIGH", 180, 300),
            AlertRuleSeed("Power spike", "power", "GREATER_THAN", 45.0, "WARNING", 180, 300),
            AlertRuleSeed("Repeated temperature anomalies", "temperature", "ANOMALY_COUNT", 3.0, "HIGH", 300, 300),
            # Sentinel rule for the alert-service's direct machine-shutdown
            # handler: "machine_status" isn't a real telemetry metric, so
            # this rule is never matched against sensor readings -- it
            # exists only to give shutdown alerts a stable alert_rule_id
            # to dedupe/cooldown against.
            AlertRuleSeed("Unexpected machine shutdown", "machine_status", "ANOMALY_COUNT", 1.0, "WARNING", 300, 300),
        ]

        for r in rules:
            conn.execute(
                """
                INSERT INTO alert_rules (organization_id, name, metric, condition, threshold_value, threshold_min, threshold_max, severity, cooldown_seconds, window_seconds)
                VALUES (%s, %s, %s, %s, %s, NULL, NULL, %s, %s, %s)
                """,
                (org_id, r.name, r.metric, r.condition, r.threshold_value, r.severity, r.cooldown_secs, r.window_secs),
            )
        logger.info("seeded %d alert rules", len(rules))


PERMISSION_DESCRIPTIONS: dict[str, str] = {
    auth.PERM_DEVICES_READ: "View device inventory and status",
    auth.PERM_DEVICES_WRITE: "Provision, update, and decommission devices",
    auth.PERM_TELEMETRY_READ: "View sensor telemetry and historical readings",
    auth.PERM_ALERTS_READ: "View alerts",
    auth.PERM_ALERTS_MANAGE: "Acknowledge, suppress, and configure alert rules",
    auth.PERM_INCIDENTS_READ: "View incidents",
    auth.PERM_INCIDENTS_MANAGE: "Assign, transition, and resolve incidents",
    auth.PERM_FACTORIES_READ: "View factories, production lines, and machines",
    auth.PERM_FACTORIES_MANAGE: "Create and modify factories, production lines, and machines",
    auth.PERM_USERS_MANAGE: "Create users and modify role assignments",
    auth.PERM_SYSTEM_ADMIN: "Full administrative access, including platform configuration",
}


def _seed_rbac_if_missing(pool: ConnectionPool) -> None:
    """Seeds roles, permissions, and role_permissions directly from
    shared.auth.ROLE_PERMISSIONS -- the same map the runtime uses to
    resolve a logged-in user's permissions -- so this reference table can
    never drift from what's actually enforced."""
    with pool.connection() as conn:
        existing = conn.execute("SELECT count(*) FROM roles").fetchone()[0]
        if existing > 0:
            logger.info("roles already seeded -- skipping RBAC seed, seed is idempotent")
            return

        role_ids: dict[str, str] = {}
        for role in auth.ALL_ROLES:
            role_ids[role] = str(conn.execute(
                "INSERT INTO roles (name, description) VALUES (%s, %s) RETURNING id", (role, f"Seeded role: {role}")
            ).fetchone()[0])

        perm_ids: dict[str, str] = {}
        for code, desc in PERMISSION_DESCRIPTIONS.items():
            perm_ids[code] = str(conn.execute(
                "INSERT INTO permissions (code, description) VALUES (%s, %s) RETURNING id", (code, desc)
            ).fetchone()[0])

        link_count = 0
        for role, perms in auth.ROLE_PERMISSIONS.items():
            for perm in perms:
                conn.execute(
                    "INSERT INTO role_permissions (role_id, permission_id) VALUES (%s, %s)", (role_ids[role], perm_ids[perm])
                )
                link_count += 1

        logger.info("seeded %d roles, %d permissions, %d role_permissions links", len(role_ids), len(perm_ids), link_count)


def _seed_users_if_missing(pool: ConnectionPool, org_id: str) -> None:
    """Creates one demo user per role, scoped to org_id, with a
    bcrypt-hashed password (never storing the plaintext) and the
    corresponding user_roles link."""
    with pool.connection() as conn:
        existing = conn.execute("SELECT count(*) FROM users WHERE organization_id = %s", (org_id,)).fetchone()[0]
        if existing > 0:
            logger.info("organization already has %d user(s) -- skipping user seed, seed is idempotent", existing)
            return

    auth.validate_password_strength(DEMO_PASSWORD)
    password_hash = auth.hash_password(DEMO_PASSWORD)

    with pool.connection() as conn:
        for role in auth.ALL_ROLES:
            email = f"{role.lower()}@musterfabrik-gmbh.de"
            full_name = f"Demo {role.replace('_', ' ').title()}"

            user_id = str(conn.execute(
                "INSERT INTO users (organization_id, email, password_hash, full_name) VALUES (%s, %s, %s, %s) RETURNING id",
                (org_id, email, password_hash, full_name),
            ).fetchone()[0])

            role_id = str(conn.execute("SELECT id FROM roles WHERE name = %s", (role,)).fetchone()[0])
            conn.execute("INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s)", (user_id, role_id))

    logger.info("seeded %d demo users (one per role), password %r -- LOCAL DEV ONLY", len(auth.ALL_ROLES), DEMO_PASSWORD)


def _seed_second_organization_if_missing(pool: ConnectionPool) -> None:
    """Creates a second, minimal organization so multi-tenancy has
    something real to isolate against -- tests and manual verification can
    prove Organization A's data is invisible to Organization B's users,
    rather than that claim being untestable because only one organization
    exists."""
    slug = "zweite-firma-gmbh"

    with pool.connection() as conn:
        row = conn.execute("SELECT id FROM organizations WHERE slug = %s", (slug,)).fetchone()
        if row is not None:
            logger.info("second organization %r already seeded -- skipping, seed is idempotent", slug)
            return

        org_id = str(conn.execute(
            "INSERT INTO organizations (name, slug) VALUES ('Zweite Firma GmbH', %s) RETURNING id", (slug,)
        ).fetchone()[0])

        factory_id = str(conn.execute(
            "INSERT INTO factories (organization_id, name, city) VALUES (%s, 'Stuttgart Plant', 'Stuttgart') RETURNING id", (org_id,)
        ).fetchone()[0])
        line_id = str(conn.execute(
            "INSERT INTO production_lines (factory_id, name) VALUES (%s, 'Line 01') RETURNING id", (factory_id,)
        ).fetchone()[0])
        conn.execute(
            "INSERT INTO machines (production_line_id, name, machine_type) VALUES (%s, 'Stuttgart-Line01-M001', 'CNC_MILLING_MACHINE')",
            (line_id,),
        )

        password_hash = auth.hash_password(DEMO_PASSWORD)
        admin_user_id = str(conn.execute(
            "INSERT INTO users (organization_id, email, password_hash, full_name) VALUES (%s, 'admin@zweite-firma-gmbh.de', %s, 'Demo Admin') RETURNING id",
            (org_id, password_hash),
        ).fetchone()[0])
        admin_role_id = str(conn.execute("SELECT id FROM roles WHERE name = %s", (auth.ROLE_ADMIN,)).fetchone()[0])
        conn.execute("INSERT INTO user_roles (user_id, role_id) VALUES (%s, %s)", (admin_user_id, admin_role_id))

    logger.info("seeded second organization %r for tenant-isolation testing (org_id=%s)", slug, org_id)


if __name__ == "__main__":
    main()
