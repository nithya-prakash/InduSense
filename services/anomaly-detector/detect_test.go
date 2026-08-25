package main

import (
	"testing"

	"github.com/nithya-prakash/indusense/pkg/events"
)

func testDetectConfig() Config {
	return Config{MinSamplesForZScore: 30, ZScoreThreshold: 3.0, ForestScoreThreshold: 0.62}
}

func TestRunDetectorsFiresRuleOnly(t *testing.T) {
	results := runDetectors(
		1000, metricRange{Min: 0, Max: 100}, true, // rule fires hard
		0.5, 100, testDetectConfig(), // stat: no fire
		0.1, true, // forest: below threshold, no fire
	)
	if len(results) != 1 || results[0].Method != "RULE" {
		t.Fatalf("expected exactly one RULE detection, got %+v", results)
	}
}

func TestRunDetectorsCanFireMultipleMethods(t *testing.T) {
	results := runDetectors(
		1000, metricRange{Min: 0, Max: 100}, true, // rule fires
		10.0, 100, testDetectConfig(), // stat fires (z=10 > 3)
		0.9, true, // forest fires (0.9 > 0.62 threshold)
	)
	if len(results) != 3 {
		t.Fatalf("expected all three detectors to fire, got %+v", results)
	}
}

func TestCombineDetectionsTakesWorstSeverityAndMaxScore(t *testing.T) {
	results := []detection{
		{Method: "RULE", Severity: events.SeverityWarning, Score: 0.2, Reason: "r1"},
		{Method: "ISOLATION_FOREST", Severity: events.SeverityCritical, Score: 0.9, Reason: "r2"},
		{Method: "STATISTICAL", Severity: events.SeverityHigh, Score: 0.5, Reason: "r3"},
	}
	severity, score, methods, reason := combineDetections(results)

	if severity != events.SeverityCritical {
		t.Errorf("severity = %s, want CRITICAL", severity)
	}
	if score != 0.9 {
		t.Errorf("score = %v, want 0.9 (max)", score)
	}
	if len(methods) != 3 {
		t.Errorf("expected 3 methods, got %v", methods)
	}
	if reason == "" {
		t.Error("expected a non-empty combined reason")
	}
}

func TestCombineDetectionsEmptyInputYieldsInfoSeverityZeroScore(t *testing.T) {
	severity, score, methods, _ := combineDetections(nil)
	if severity != events.SeverityInfo {
		t.Errorf("severity = %s, want INFO for no detections", severity)
	}
	if score != 0 || len(methods) != 0 {
		t.Errorf("expected zero score and no methods, got score=%v methods=%v", score, methods)
	}
}

func TestIsolationCheckSeverityBands(t *testing.T) {
	if fired, _, _ := isolationCheck(0.5, 0.62); fired {
		t.Error("0.5 should not fire against threshold 0.62")
	}
	_, sev, _ := isolationCheck(0.76, 0.62)
	if sev != events.SeverityCritical {
		t.Errorf("expected CRITICAL for score 0.76, got %s", sev)
	}
}
