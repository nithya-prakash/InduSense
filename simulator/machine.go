package main

import (
	"math/rand"
	"sync/atomic"
)

const (
	machineStateRunning int32 = iota
	machineStateStopped
)

// machineController holds the shared running/stopped state for every sensor
// that belongs to one device. While stopped, sensors on that device produce
// no telemetry (a "missing readings" gap), matching how a real machine
// shutdown silences its instrumentation.
type machineController struct {
	state int32 // atomic: machineStateRunning | machineStateStopped
}

func newMachineController() *machineController {
	return &machineController{state: machineStateRunning}
}

func (m *machineController) isRunning() bool {
	return atomic.LoadInt32(&m.state) == machineStateRunning
}

func (m *machineController) setRunning(running bool) {
	if running {
		atomic.StoreInt32(&m.state, machineStateRunning)
	} else {
		atomic.StoreInt32(&m.state, machineStateStopped)
	}
}

// shouldToggle decides, once per tick, whether a machine controller flips
// state. Stopping is rare (unplanned-shutdown-like); recovering from a stop
// is much more likely per tick so outages stay short relative to uptime.
func shouldToggle(rng *rand.Rand, running bool) bool {
	if running {
		return rng.Float64() < 0.0005 // ~1 in 2000 ticks
	}
	return rng.Float64() < 0.05 // recovers within ~20 ticks on average
}
