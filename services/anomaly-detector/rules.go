package main

import "github.com/nithya-prakash/indusense/pkg/events"

// ruleCheck flags a reading that falls outside its sensor's known safe
// operating range (temperature > threshold, pressure outside safe range,
// etc. — all expressed the same way: value outside [min, max]). Severity
// scales with how far outside the range the value falls, as a fraction of
// the range's own span, so a wildly-out-of-range spike reads as more severe
// than a reading just past the boundary.
func ruleCheck(value float64, r metricRange) (fired bool, severity string, score float64) {
	span := r.Max - r.Min
	if span <= 0 {
		return false, "", 0
	}

	var overshoot float64
	switch {
	case value > r.Max:
		overshoot = (value - r.Max) / span
	case value < r.Min:
		overshoot = (r.Min - value) / span
	default:
		return false, "", 0
	}

	score = clamp01(overshoot)
	switch {
	case overshoot >= 0.5:
		severity = events.SeverityCritical
	case overshoot >= 0.2:
		severity = events.SeverityHigh
	default:
		severity = events.SeverityWarning
	}
	return true, severity, score
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
