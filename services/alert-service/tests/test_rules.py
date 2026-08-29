from rules import AlertRule


def test_scope_matches_wildcard_when_none():
    rule = AlertRule()  # no scoping at all -> matches anything
    assert rule.scope_matches("m1", "d1", "s1")


def test_scope_matches_narrows_to_specific_machine():
    rule = AlertRule(machine_id="machine-1")
    assert rule.scope_matches("machine-1", "any-device", "any-sensor")
    assert not rule.scope_matches("machine-2", "any-device", "any-sensor")


def test_scope_matches_requires_all_set_fields_to_match():
    rule = AlertRule(machine_id="machine-1", device_id="device-1")
    assert rule.scope_matches("machine-1", "device-1", "anything")
    assert not rule.scope_matches("machine-1", "device-2", "anything")
