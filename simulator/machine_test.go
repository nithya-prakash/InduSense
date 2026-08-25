package main

import (
	"math/rand"
	"testing"
)

func TestMachineControllerDefaultsToRunning(t *testing.T) {
	mc := newMachineController()
	if !mc.isRunning() {
		t.Fatal("a new machine controller should start in the running state")
	}
}

func TestShouldToggleRecoversFasterThanItStops(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	stopTriggers, recoverTriggers := 0, 0
	const trials = 100000
	for i := 0; i < trials; i++ {
		if shouldToggle(rng, true) {
			stopTriggers++
		}
		if shouldToggle(rng, false) {
			recoverTriggers++
		}
	}
	if recoverTriggers <= stopTriggers {
		t.Errorf("expected recovery to trigger far more often than stopping (stop=%d, recover=%d)",
			stopTriggers, recoverTriggers)
	}
}
