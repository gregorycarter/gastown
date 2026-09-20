package cmd

import (
	"errors"
	"testing"

	"github.com/steveyegge/gastown/internal/polecat"
)

func TestDirectSessionStartCannotExceedRigCap(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\nhisn-d\n")
	started := false
	err := startPolecatSessionWithAdmission(town, "hisn", "preserved", "hisn-work", func() error {
		started = true
		return nil
	})
	var denied *polecatCapacityAdmissionError
	if !errors.As(err, &denied) || started {
		t.Fatalf("direct recovery bypassed full rig cap: started=%v error=%v", started, err)
	}
}

func TestBulkRestorationSharesAdmissionAndPreservesExistingSessions(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\nhisn-d\n")
	unknown := errors.New("tmux unavailable")
	for _, tt := range []struct {
		name string
		live bool
		err  error
	}{
		{name: "stopped worker at cap"},
		{name: "already running", live: true},
		{name: "unknown session state", err: unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			started := false
			err := restorePolecatSessionWithAdmission(town, "hisn", "preserved", "hisn-work",
				func() (bool, error) { return tt.live, tt.err },
				func() error { started = true; return nil })
			if started {
				t.Fatal("restoration started an extra worker or replaced a live session")
			}
			var denied *polecatCapacityAdmissionError
			switch {
			case tt.err != nil:
				if !errors.Is(err, unknown) {
					t.Fatalf("unknown state not preserved: %v", err)
				}
			case tt.live:
				if !errors.Is(err, polecat.ErrSessionRunning) {
					t.Fatalf("existing session not recognized at cap: %v", err)
				}
			default:
				if !errors.As(err, &denied) {
					t.Fatalf("full cap not enforced: %v", err)
				}
			}
		})
	}
}

func TestBulkRestorationReservesSlotUntilStartupCompletes(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\n")
	started := false
	err := restorePolecatSessionWithAdmission(town, "hisn", "preserved", "hisn-work",
		func() (bool, error) { return false, nil },
		func() error {
			started = true
			other, _, err := acquirePolecatAdmission(town, "hisn", "hisn-other", "test")
			if other != nil {
				other.Release()
			}
			var denied *polecatCapacityAdmissionError
			if !errors.As(err, &denied) {
				t.Fatalf("bulk startup did not reserve the last slot: %v", err)
			}
			return nil
		})
	if err != nil || !started {
		t.Fatalf("available slot did not restore worker: started=%v error=%v", started, err)
	}
}

func TestDirectSessionStartHoldsAndReleasesReservation(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\n")
	startFailure := errors.New("startup failed")
	for _, outcome := range []error{startFailure, nil} {
		err := startPolecatSessionWithAdmission(town, "hisn", "preserved", "hisn-work", func() error {
			// Startup has not created a session yet; its reservation must still
			// prevent a concurrent scheduler or second recovery from starting.
			other, _, err := acquirePolecatAdmission(town, "hisn", "hisn-other", "test")
			if other != nil {
				other.Release()
			}
			var denied *polecatCapacityAdmissionError
			if !errors.As(err, &denied) {
				t.Fatalf("startup reservation missing: %v", err)
			}
			return outcome
		})
		if !errors.Is(err, outcome) {
			t.Fatalf("startup error not preserved: got=%v want=%v", err, outcome)
		}
		available, _, err := acquirePolecatAdmission(town, "hisn", "hisn-after", "test")
		if err != nil {
			t.Fatalf("startup leaked reservation: %v", err)
		}
		available.Release()
	}
}
