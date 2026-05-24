package goaria

import "testing"

func TestSegmentedConcurrencyControllerIgnoresThrottleBurst(t *testing.T) {
	controller := newSegmentedConcurrencyController(5)
	limit, generation := controller.launchState()

	controller.onThrottle(limit, generation, 5)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after first throttle = %d, want %d", got, want)
	}
	if got, want := controller.safeLimit, 2; got != want {
		t.Fatalf("safe limit after first throttle = %d, want %d", got, want)
	}

	controller.onThrottle(limit, generation, 5)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after stale burst throttle = %d, want %d", got, want)
	}
	if got, want := controller.generation, generation+1; got != want {
		t.Fatalf("generation after stale burst throttle = %d, want %d", got, want)
	}
}

func TestSegmentedConcurrencyControllerRejectedProbeReturnsToSafeLimit(t *testing.T) {
	controller := newSegmentedConcurrencyController(5)
	limit, generation := controller.launchState()
	controller.onThrottle(limit, generation, 5)

	driveSegmentedControllerSuccesses(controller, 5, controller.successThreshold())
	if got, want := controller.limit(), 3; got != want {
		t.Fatalf("limit after probe window = %d, want %d", got, want)
	}

	limit, generation = controller.launchState()
	controller.onThrottle(limit, generation, 5)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after rejected probe = %d, want %d", got, want)
	}
	if got, want := controller.safeLimit, 2; got != want {
		t.Fatalf("safe limit after rejected probe = %d, want %d", got, want)
	}
	if got, want := controller.probePenalty, 2; got != want {
		t.Fatalf("probe penalty after rejected probe = %d, want %d", got, want)
	}
}

func TestSegmentedConcurrencyControllerBacksOffRepeatedProbes(t *testing.T) {
	controller := newSegmentedConcurrencyController(5)
	limit, generation := controller.launchState()
	controller.onThrottle(limit, generation, 5)
	driveSegmentedControllerSuccesses(controller, 5, controller.successThreshold())
	limit, generation = controller.launchState()
	controller.onThrottle(limit, generation, 5)

	threshold := controller.successThreshold()
	driveSegmentedControllerSuccesses(controller, 5, threshold-1)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit before penalized probe window completes = %d, want %d", got, want)
	}
	driveSegmentedControllerSuccesses(controller, 5, 1)
	if got, want := controller.limit(), 3; got != want {
		t.Fatalf("limit after penalized probe window completes = %d, want %d", got, want)
	}
}

func TestSegmentedConcurrencyControllerCeilingChanges(t *testing.T) {
	controller := newSegmentedConcurrencyController(2)
	controller.clampCeiling(5)
	if got, want := controller.limit(), 5; got != want {
		t.Fatalf("non-adaptive limit after ceiling increase = %d, want %d", got, want)
	}

	limit, generation := controller.launchState()
	controller.onThrottle(limit, generation, 5)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("adaptive limit after initial throttle = %d, want %d", got, want)
	}

	controller.clampCeiling(1)
	if got, want := controller.limit(), 1; got != want {
		t.Fatalf("adaptive limit after ceiling decrease = %d, want %d", got, want)
	}
	controller.clampCeiling(5)
	if got, want := controller.limit(), 1; got != want {
		t.Fatalf("adaptive limit after ceiling increase = %d, want %d", got, want)
	}

	driveSegmentedControllerSuccesses(controller, 5, controller.successThreshold())
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("adaptive limit after probing raised ceiling = %d, want %d", got, want)
	}
}

func driveSegmentedControllerSuccesses(controller *segmentedConcurrencyController, ceiling, count int) {
	limit, generation := controller.launchState()
	for i := 0; i < count; i++ {
		controller.onSuccess(limit, generation, ceiling)
	}
}
