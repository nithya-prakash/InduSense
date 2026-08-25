package main

import (
	"fmt"

	"github.com/nithya-prakash/indusense/pkg/events"
)

type detection struct {
	Method   string
	Severity string
	Score    float64
	Reason   string
}

var severityRank = map[string]int{
	events.SeverityInfo:     0,
	events.SeverityWarning:  1,
	events.SeverityHigh:     2,
	events.SeverityCritical: 3,
}

// runDetectors applies all three detection levels to one telemetry sample
// and returns every method that fired. Running all three independently
// (rather than short-circuiting on the first hit) is deliberate: the
// combined anomaly record should report every corroborating signal, since a
// reading flagged by both the rule engine and the isolation forest is more
// actionable than one flagged by either alone.
func runDetectors(
	value float64,
	rng metricRange,
	hasRange bool,
	zScore float64,
	sampleCount int,
	cfg Config,
	forestScore float64,
	hasForest bool,
) []detection {
	var results []detection

	if hasRange {
		if fired, severity, score := ruleCheck(value, rng); fired {
			results = append(results, detection{
				Method:   "RULE",
				Severity: severity,
				Score:    score,
				Reason:   fmt.Sprintf("value %.2f outside safe operating range [%.2f, %.2f]", value, rng.Min, rng.Max),
			})
		}
	}

	if fired, severity, score := statCheck(zScore, sampleCount, cfg.MinSamplesForZScore, cfg.ZScoreThreshold); fired {
		results = append(results, detection{
			Method:   "STATISTICAL",
			Severity: severity,
			Score:    score,
			Reason:   fmt.Sprintf("z-score %.2f exceeds threshold %.2f against rolling baseline", zScore, cfg.ZScoreThreshold),
		})
	}

	if hasForest {
		if fired, severity, score := isolationCheck(forestScore, cfg.ForestScoreThreshold); fired {
			results = append(results, detection{
				Method:   "ISOLATION_FOREST",
				Severity: severity,
				Score:    score,
				Reason:   fmt.Sprintf("isolation forest anomaly score %.3f exceeds threshold %.3f", forestScore, cfg.ForestScoreThreshold),
			})
		}
	}

	return results
}

// combineDetections folds multiple firing detectors into one anomaly
// record: worst severity wins, score is the max across methods, and the
// reason lists every contributing method so a human reading the alert can
// see the full picture.
func combineDetections(results []detection) (severity string, score float64, methods []string, reason string) {
	severity = events.SeverityInfo
	for _, r := range results {
		methods = append(methods, r.Method)
		if r.Score > score {
			score = r.Score
		}
		if severityRank[r.Severity] > severityRank[severity] {
			severity = r.Severity
		}
		if reason == "" {
			reason = r.Reason
		} else {
			reason = reason + "; " + r.Reason
		}
	}
	return severity, score, methods, reason
}

func isolationCheck(score, threshold float64) (fired bool, severity string, outScore float64) {
	if score < threshold {
		return false, "", 0
	}
	switch {
	case score >= 0.75:
		severity = events.SeverityCritical
	case score >= threshold+0.06:
		severity = events.SeverityHigh
	default:
		severity = events.SeverityWarning
	}
	return true, severity, score
}
