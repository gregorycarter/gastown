package cmd

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

func TestClassifyDispatchFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want dispatchFailureClass
	}{
		{
			name: "nil",
			err:  nil,
			want: dispatchFailureWork,
		},
		{
			name: "respawn limit",
			err:  errors.New("respawn limit reached for bt-abc (3 attempts). This bead keeps failing"),
			want: dispatchFailureRespawnLimit,
		},
		{
			name: "respawn limit wrapped by sling",
			err:  fmt.Errorf("sling failed: %w", errors.New("respawn limit reached for bt-abc (3 attempts)")),
			want: dispatchFailureRespawnLimit,
		},
		{
			name: "polecat directory cap",
			err:  errors.New("rig bridge_town_core has 30 polecat directories (max 30). Resolve recovery-needed polecats"),
			want: dispatchFailureCapacity,
		},
		{
			name: "admission denied",
			err:  errors.New("polecat admission denied: capacity full (max=4 occupied=4)"),
			want: dispatchFailureCapacity,
		},
		{
			name: "typed admission error",
			err:  fmt.Errorf("sling failed: %w", &polecatCapacityAdmissionError{}),
			want: dispatchFailureCapacity,
		},
		{
			name: "dolt connection refused",
			err:  errors.New("bd show bt-abc: dial tcp 127.0.0.1:3307: connect: connection refused"),
			want: dispatchFailureTransport,
		},
		{
			name: "database not found",
			err:  errors.New("bd ready: database not found: bridge_town_core"),
			want: dispatchFailureTransport,
		},
		{
			name: "timeout",
			err:  errors.New("bd blocked: context deadline exceeded"),
			want: dispatchFailureTransport,
		},
		{
			name: "genuine work failure",
			err:  errors.New("formula \"mol-polecat-work\" failed to cook: unknown variable"),
			want: dispatchFailureWork,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyDispatchFailure(tc.err)
			if got != tc.want {
				t.Fatalf("classifyDispatchFailure(%v) = %s, want %s", tc.err, got, tc.want)
			}
			if tc.want == dispatchFailureWork && !got.countsTowardCircuitBreaker() {
				t.Error("work failures must count toward the circuit breaker")
			}
			if tc.want != dispatchFailureWork && got.countsTowardCircuitBreaker() {
				t.Errorf("%s must not count toward the circuit breaker", got)
			}
		})
	}
}

func TestDecayDispatchFailures(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	t.Run("stale counter resets", func(t *testing.T) {
		fields := &capacity.SlingContextFields{
			DispatchFailures: 2,
			LastFailure:      "boom",
			LastFailureAt:    now.Add(-90 * time.Minute).Format(time.RFC3339),
		}
		if !decayDispatchFailures(fields, now) {
			t.Fatal("expected decay")
		}
		if fields.DispatchFailures != 0 || fields.LastFailure != "" || fields.LastFailureAt != "" {
			t.Fatalf("counter not cleared: %+v", fields)
		}
	})

	t.Run("recent counter kept", func(t *testing.T) {
		fields := &capacity.SlingContextFields{
			DispatchFailures: 2,
			LastFailureAt:    now.Add(-5 * time.Minute).Format(time.RFC3339),
		}
		if decayDispatchFailures(fields, now) {
			t.Fatal("recent failures must not decay")
		}
		if fields.DispatchFailures != 2 {
			t.Fatalf("DispatchFailures = %d, want 2", fields.DispatchFailures)
		}
	})

	t.Run("no timestamp is left alone", func(t *testing.T) {
		fields := &capacity.SlingContextFields{DispatchFailures: 2}
		if decayDispatchFailures(fields, now) {
			t.Fatal("a counter with no timestamp must not decay")
		}
	})

	t.Run("zero counter and nil are no-ops", func(t *testing.T) {
		if decayDispatchFailures(&capacity.SlingContextFields{}, now) {
			t.Error("zero counter should not decay")
		}
		if decayDispatchFailures(nil, now) {
			t.Error("nil fields should not decay")
		}
	})
}

func TestShouldReportInfraFailureOncePerContext(t *testing.T) {
	resetInfraEscalationStateForTest()
	defer resetInfraEscalationStateForTest()

	if !shouldReportInfraFailure("bt-wisp-ctx1") {
		t.Fatal("first report should fire")
	}
	if shouldReportInfraFailure("bt-wisp-ctx1") {
		t.Fatal("second report for the same context should be suppressed")
	}
	if !shouldReportInfraFailure("bt-wisp-ctx2") {
		t.Fatal("a different context should still report")
	}
}
