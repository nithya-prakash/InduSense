package main

import "testing"

func TestMachineProfilesReferenceKnownMetrics(t *testing.T) {
	for _, profile := range machineProfiles {
		for _, metric := range profile.metrics {
			if _, ok := metricSpecs[metric]; !ok {
				t.Errorf("machine profile %s references unknown metric %q", profile.machineType, metric)
			}
		}
	}
}

func TestRandomStatusOnlyReturnsWeightedKeys(t *testing.T) {
	weights := map[string]int{"A": 1, "B": 1, "C": 1}
	seen := map[string]bool{}
	for i := 0; i < 500; i++ {
		status := randomStatus(weights)
		if _, ok := weights[status]; !ok {
			t.Fatalf("randomStatus returned unexpected value %q", status)
		}
		seen[status] = true
	}
	if len(seen) != len(weights) {
		t.Errorf("expected all %d statuses to appear across 500 draws, saw %d", len(weights), len(seen))
	}
}

func TestRandomHexSecretIsUniqueAndCorrectLength(t *testing.T) {
	a, err := randomHexSecret(24)
	if err != nil {
		t.Fatalf("randomHexSecret: %v", err)
	}
	b, err := randomHexSecret(24)
	if err != nil {
		t.Fatalf("randomHexSecret: %v", err)
	}
	if len(a) != 48 { // 24 bytes -> 48 hex chars
		t.Errorf("expected 48 hex chars, got %d (%q)", len(a), a)
	}
	if a == b {
		t.Errorf("expected two independently generated secrets to differ")
	}
}
