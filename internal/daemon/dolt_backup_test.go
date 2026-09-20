package daemon

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestDoltBackupPersistentBackoff(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	last := now.Add(-time.Hour)
	s := doltBackupState{LastSuccess: last}
	for i := 1; i <= 9; i++ {
		s = nextDoltBackupState(s, now, errors.New("permission denied"))
		if s.NextAttempt.Sub(now) > 4*time.Hour || s.NextAttempt.Sub(now) < 15*time.Minute || !s.LastSuccess.Equal(last) {
			t.Fatal(s)
		}
	}
	file := filepath.Join(t.TempDir(), "state.json")
	if err := writeDoltBackupState(file, map[string]doltBackupState{"hisn": s}); err != nil {
		t.Fatal(err)
	}
	loaded, err := readDoltBackupState(file)
	if err != nil || !loaded["hisn"].NextAttempt.Equal(s.NextAttempt) {
		t.Fatal(loaded, err)
	}
	s = nextDoltBackupState(loaded["hisn"], now, nil)
	if s.Failures != 0 || !s.NextAttempt.IsZero() || !s.LastSuccess.Equal(now) {
		t.Fatal(s)
	}
}
func TestDoltBackupPermanentFailure(t *testing.T) {
	for _, message := range []string{"operation not permitted", "Permission denied", "read-only file system"} {
		if !permanentBackupFailure(errors.New(message)) {
			t.Fatal(message)
		}
	}
	if permanentBackupFailure(errors.New("temporary lock")) {
		t.Fatal("transient lock must retain one retry")
	}
}
