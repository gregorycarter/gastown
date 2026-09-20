package cmd

import (
	"errors"
	"testing"
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
