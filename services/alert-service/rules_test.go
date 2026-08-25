package main

import "testing"

func TestScopeMatchesWildcardWhenNil(t *testing.T) {
	rule := AlertRule{} // no scoping at all -> matches anything
	if !rule.scopeMatches("m1", "d1", "s1") {
		t.Error("a rule with no scope fields set should match any machine/device/sensor")
	}
}

func TestScopeMatchesNarrowsToSpecificMachine(t *testing.T) {
	m := "machine-1"
	rule := AlertRule{MachineID: &m}
	if !rule.scopeMatches("machine-1", "any-device", "any-sensor") {
		t.Error("expected match on the configured machine_id")
	}
	if rule.scopeMatches("machine-2", "any-device", "any-sensor") {
		t.Error("expected no match on a different machine_id")
	}
}

func TestScopeMatchesRequiresAllSetFieldsToMatch(t *testing.T) {
	m, d := "machine-1", "device-1"
	rule := AlertRule{MachineID: &m, DeviceID: &d}
	if !rule.scopeMatches("machine-1", "device-1", "anything") {
		t.Error("expected match when both machine_id and device_id match")
	}
	if rule.scopeMatches("machine-1", "device-2", "anything") {
		t.Error("expected no match when device_id differs even though machine_id matches")
	}
}
