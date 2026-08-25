package incidents

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
		if !IsValidTransition(c.from, c.to) {
			t.Errorf("expected %s -> %s to be a valid transition", c.from, c.to)
		}
	}
}

func TestIsValidTransitionRejectsInvalidPaths(t *testing.T) {
	cases := []struct{ from, to string }{
		{"CLOSED", "OPEN"},
		{"CLOSED", "RESOLVED"},
		{"OPEN", "CLOSED"},
		{"RESOLVED", "ACKNOWLEDGED"},
		{"INVESTIGATING", "OPEN"},
	}
	for _, c := range cases {
		if IsValidTransition(c.from, c.to) {
			t.Errorf("expected %s -> %s to be rejected", c.from, c.to)
		}
	}
}

func TestIsValidTransitionClosedIsTerminal(t *testing.T) {
	if len(validTransitions["CLOSED"]) != 0 {
		t.Error("CLOSED should have no valid outgoing transitions")
	}
}
