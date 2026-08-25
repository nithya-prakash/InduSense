package main

import "testing"

func TestIsValidTransitionAllowsDocumentedPaths(t *testing.T) {
	cases := []struct{ from, to string }{
		{"OPEN", "ACKNOWLEDGED"},
		{"OPEN", "INVESTIGATING"},
		{"OPEN", "RESOLVED"},
		{"ACKNOWLEDGED", "INVESTIGATING"},
		{"ACKNOWLEDGED", "RESOLVED"},
		{"INVESTIGATING", "RESOLVED"},
		{"RESOLVED", "CLOSED"},
		{"RESOLVED", "INVESTIGATING"}, // reopen a recurrence
	}
	for _, c := range cases {
		if !isValidTransition(c.from, c.to) {
			t.Errorf("expected %s -> %s to be a valid transition", c.from, c.to)
		}
	}
}

func TestIsValidTransitionRejectsInvalidPaths(t *testing.T) {
	cases := []struct{ from, to string }{
		{"CLOSED", "OPEN"}, // terminal state, no way back
		{"CLOSED", "RESOLVED"},
		{"OPEN", "CLOSED"},           // must resolve first
		{"RESOLVED", "ACKNOWLEDGED"}, // can only reopen to INVESTIGATING, not back to ACKNOWLEDGED
		{"INVESTIGATING", "OPEN"},    // no going backward
	}
	for _, c := range cases {
		if isValidTransition(c.from, c.to) {
			t.Errorf("expected %s -> %s to be rejected", c.from, c.to)
		}
	}
}

func TestIsValidTransitionClosedIsTerminal(t *testing.T) {
	if len(validTransitions["CLOSED"]) != 0 {
		t.Error("CLOSED should have no valid outgoing transitions")
	}
}
