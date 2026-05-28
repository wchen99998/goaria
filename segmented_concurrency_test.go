package goaria

import "testing"

func TestSegmentedConcurrencyControllerStartsAtOneAndRamps(t *testing.T) {
	controller := newSegmentedConcurrencyController(5)
	if got, want := controller.limit(), 1; got != want {
		t.Fatalf("initial limit = %d, want %d", got, want)
	}

	for want := 2; want <= 5; want++ {
		driveSegmentedControllerSuccessWindow(controller, 5)
		if got := controller.limit(); got != want {
			t.Fatalf("limit after success window = %d, want %d", got, want)
		}
	}

	driveSegmentedControllerSuccessWindow(controller, 5)
	if got, want := controller.limit(), 5; got != want {
		t.Fatalf("limit after ceiling success window = %d, want %d", got, want)
	}
}

func TestSegmentedConcurrencyControllerHonorsLoweredCeiling(t *testing.T) {
	controller := newSegmentedConcurrencyController(4)
	rampSegmentedControllerTo(t, controller, 4, 4)
	limit, generation := controller.launchState()

	controller.clampCeiling(2)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after lowered ceiling = %d, want %d", got, want)
	}

	for i := 0; i < limit; i++ {
		controller.onSuccess(limit, generation, 2)
	}
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after stale successes from lowered ceiling = %d, want %d", got, want)
	}

	driveSegmentedControllerSuccessWindow(controller, 2)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after success window at lowered ceiling = %d, want %d", got, want)
	}
}

func TestSegmentedConcurrencyControllerRejectedProbeSticksToLastGoodLimit(t *testing.T) {
	controller := newSegmentedConcurrencyController(5)
	rampSegmentedControllerTo(t, controller, 5, 3)

	limit, generation := controller.launchState()
	controller.onThrottle(limit, generation, 5)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after rejected probe = %d, want %d", got, want)
	}
	if got, want := controller.rejectedProbeLimit, 3; got != want {
		t.Fatalf("rejected probe limit = %d, want %d", got, want)
	}

	for i := 0; i < 3; i++ {
		driveSegmentedControllerSuccessWindow(controller, 5)
		if got, want := controller.limit(), 2; got != want {
			t.Fatalf("limit after post-rejection success window %d = %d, want %d", i+1, got, want)
		}
	}
}

func TestSegmentedConcurrencyControllerThrottleAtOneDoesNotCapFutureRamp(t *testing.T) {
	controller := newSegmentedConcurrencyController(4)
	limit, generation := controller.launchState()
	controller.onThrottle(limit, generation, 4)
	if got, want := controller.limit(), 1; got != want {
		t.Fatalf("limit after baseline throttle = %d, want %d", got, want)
	}
	if got, want := controller.rejectedProbeLimit, 0; got != want {
		t.Fatalf("rejected probe limit after baseline throttle = %d, want %d", got, want)
	}

	driveSegmentedControllerSuccessWindow(controller, 4)
	if got, want := controller.limit(), 2; got != want {
		t.Fatalf("limit after post-throttle success window = %d, want %d", got, want)
	}
}

func TestSegmentedConcurrencyControllerRetryableErrorResetsSuccessWindow(t *testing.T) {
	controller := newSegmentedConcurrencyController(4)
	rampSegmentedControllerTo(t, controller, 4, 3)

	limit, generation := controller.launchState()
	controller.onSuccess(limit, generation, 4)
	controller.onRetryableError(limit, generation, 4)
	for i := 0; i < limit-1; i++ {
		controller.onSuccess(limit, generation, 4)
	}
	if got, want := controller.limit(), 3; got != want {
		t.Fatalf("limit after stale pre-error successes = %d, want %d", got, want)
	}

	driveSegmentedControllerSuccessWindow(controller, 4)
	if got, want := controller.limit(), 4; got != want {
		t.Fatalf("limit after fresh success window = %d, want %d", got, want)
	}
}

func rampSegmentedControllerTo(t *testing.T, controller *segmentedConcurrencyController, ceiling, want int) {
	t.Helper()
	for controller.limit() < want {
		driveSegmentedControllerSuccessWindow(controller, ceiling)
	}
	if got := controller.limit(); got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}

func driveSegmentedControllerSuccessWindow(controller *segmentedConcurrencyController, ceiling int) {
	limit, generation := controller.launchState()
	for i := 0; i < limit; i++ {
		controller.onSuccess(limit, generation, ceiling)
	}
}
