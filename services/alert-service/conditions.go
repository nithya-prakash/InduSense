package main

// conditionMatches evaluates one alert rule's condition against an incoming
// anomaly. For GREATER_THAN/LESS_THAN/OUTSIDE_RANGE, value is the anomaly's
// raw reading. For ANOMALY_COUNT, countInWindow is how many qualifying
// anomalies have occurred for this rule's scope within its window, and
// value is ignored.
func conditionMatches(rule AlertRule, value float64, countInWindow int) bool {
	switch rule.Condition {
	case "GREATER_THAN":
		return rule.ThresholdValue != nil && value > *rule.ThresholdValue
	case "LESS_THAN":
		return rule.ThresholdValue != nil && value < *rule.ThresholdValue
	case "OUTSIDE_RANGE":
		return rule.ThresholdMin != nil && rule.ThresholdMax != nil &&
			(value < *rule.ThresholdMin || value > *rule.ThresholdMax)
	case "ANOMALY_COUNT":
		return rule.ThresholdValue != nil && float64(countInWindow) >= *rule.ThresholdValue
	default:
		return false
	}
}
