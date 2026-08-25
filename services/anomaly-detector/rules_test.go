package main

import (
	"testing"

	"github.com/nithya-prakash/indusense/pkg/events"
)

func TestRuleCheckPassesWithinRange(t *testing.T) {
	fired, _, _ := ruleCheck(50, metricRange{Min: 20, Max: 90})
	if fired {
		t.Fatal("expected no anomaly for a value within range")
	}
}

func TestRuleCheckFiresAboveMax(t *testing.T) {
	fired, severity, score := ruleCheck(100, metricRange{Min: 20, Max: 90})
	if !fired {
		t.Fatal("expected anomaly for a value above max")
	}
	if score <= 0 {
		t.Errorf("expected positive score, got %v", score)
	}
	if severity == "" {
		t.Error("expected a non-empty severity")
	}
}

func TestRuleCheckSeverityScalesWithOvershoot(t *testing.T) {
	r := metricRange{Min: 0, Max: 100}
	_, sevSmall, _ := ruleCheck(105, r) // 5% overshoot
	_, sevBig, _ := ruleCheck(300, r)   // 200% overshoot

	rank := map[string]int{events.SeverityWarning: 1, events.SeverityHigh: 2, events.SeverityCritical: 3}
	if rank[sevBig] <= rank[sevSmall] {
		t.Errorf("expected larger overshoot to produce >= severity: small=%s big=%s", sevSmall, sevBig)
	}
}

func TestRuleCheckDegenerateRangeNeverFires(t *testing.T) {
	fired, _, _ := ruleCheck(999, metricRange{Min: 5, Max: 5})
	if fired {
		t.Fatal("a zero-span range should never fire (nothing meaningful to compare against)")
	}
}
