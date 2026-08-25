package main

import "testing"

func TestConditionMatchesGreaterThan(t *testing.T) {
	rule := AlertRule{Condition: "GREATER_THAN", ThresholdValue: ptrF(90)}
	if !conditionMatches(rule, 91, 0) {
		t.Error("expected 91 > 90 to match")
	}
	if conditionMatches(rule, 90, 0) {
		t.Error("expected 90 > 90 to not match (strict)")
	}
}

func TestConditionMatchesLessThan(t *testing.T) {
	rule := AlertRule{Condition: "LESS_THAN", ThresholdValue: ptrF(10)}
	if !conditionMatches(rule, 5, 0) {
		t.Error("expected 5 < 10 to match")
	}
}

func TestConditionMatchesOutsideRange(t *testing.T) {
	rule := AlertRule{Condition: "OUTSIDE_RANGE", ThresholdMin: ptrF(20), ThresholdMax: ptrF(90)}
	if conditionMatches(rule, 50, 0) {
		t.Error("50 is inside [20,90], should not match")
	}
	if !conditionMatches(rule, 100, 0) {
		t.Error("100 is outside [20,90], should match")
	}
	if !conditionMatches(rule, 5, 0) {
		t.Error("5 is outside [20,90], should match")
	}
}

func TestConditionMatchesAnomalyCount(t *testing.T) {
	rule := AlertRule{Condition: "ANOMALY_COUNT", ThresholdValue: ptrF(3)}
	if conditionMatches(rule, 0, 2) {
		t.Error("count=2 should not match threshold 3")
	}
	if !conditionMatches(rule, 0, 3) {
		t.Error("count=3 should match threshold 3")
	}
}

func TestConditionMatchesNilThresholdNeverMatches(t *testing.T) {
	rule := AlertRule{Condition: "GREATER_THAN"}
	if conditionMatches(rule, 1000, 0) {
		t.Error("a rule with no threshold configured should never match")
	}
}

func TestConditionMatchesUnknownConditionNeverMatches(t *testing.T) {
	rule := AlertRule{Condition: "SOMETHING_ELSE", ThresholdValue: ptrF(0)}
	if conditionMatches(rule, 1000, 0) {
		t.Error("an unrecognized condition should never match")
	}
}

func ptrF(f float64) *float64 { return &f }
