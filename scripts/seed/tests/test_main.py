from main import MACHINE_PROFILES, METRIC_SPECS, random_status
from shared import auth


def test_machine_profiles_reference_known_metrics():
    for profile in MACHINE_PROFILES:
        for metric in profile.metrics:
            assert metric in METRIC_SPECS, f"machine profile {profile.machine_type} references unknown metric {metric!r}"


def test_random_status_only_returns_weighted_keys():
    weights = {"A": 1, "B": 1, "C": 1}
    seen = set()
    for _ in range(500):
        status = random_status(weights)
        assert status in weights, f"random_status returned unexpected value {status!r}"
        seen.add(status)
    assert len(seen) == len(weights), f"expected all {len(weights)} statuses to appear across 500 draws, saw {len(seen)}"


def test_seeded_password_passes_strength_validation():
    """The demo password itself must satisfy the same strength check the
    real signup/password-change path would enforce -- if this ever
    regresses, seeding would raise at startup, which is the point of
    checking it explicitly rather than just trusting the literal."""
    from main import DEMO_PASSWORD

    auth.validate_password_strength(DEMO_PASSWORD)  # must not raise
