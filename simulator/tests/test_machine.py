import random

from machine import MachineController, should_toggle


def test_machine_controller_defaults_to_running():
    mc = MachineController()
    assert mc.is_running()


def test_should_toggle_recovers_faster_than_it_stops():
    rng = random.Random(9)
    stop_triggers = recover_triggers = 0
    trials = 100000
    for _ in range(trials):
        if should_toggle(rng, True):
            stop_triggers += 1
        if should_toggle(rng, False):
            recover_triggers += 1
    assert recover_triggers > stop_triggers, f"expected recovery to trigger far more often than stopping (stop={stop_triggers}, recover={recover_triggers})"
