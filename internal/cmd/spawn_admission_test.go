package cmd

import (
	"errors"
	"os"
	"testing"
)

func TestSpawnAdmissionSurvivesAllocationUntilDeferredStartup(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\n")
	handle, _, err := acquirePolecatAdmission(town, "hisn", "hisn-work", "spawn-or-reuse")
	if err != nil {
		t.Fatal(err)
	}
	info := &SpawnedPolecatInfo{RigName: "hisn", PolecatName: "new"}
	transferSpawnAdmission(info, nil, handle)
	defer info.ReleaseAdmission()
	// The allocation has returned, but formula/hook setup has not started the
	// session. A competing dispatch must not steal that in-flight slot.
	other, _, err := acquirePolecatAdmission(town, "hisn", "hisn-other", "test")
	if other != nil {
		other.Release()
	}
	var denied *polecatCapacityAdmissionError
	if !errors.As(err, &denied) {
		t.Fatalf("allocation released its in-flight slot: %v", err)
	}
	info.ReleaseAdmission() // Also the caller's pre-start error/rollback path.
	info.ReleaseAdmission() // StartSession and caller both release safely.
	available, _, err := acquirePolecatAdmission(town, "hisn", "hisn-after", "test")
	if err != nil {
		t.Fatalf("dispatch leaked its slot: %v", err)
	}
	available.Release()
}

func TestSpawnAdmissionReleasedOnAllocationFailure(t *testing.T) {
	for _, withPartial := range []bool{false, true} {
		town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\n")
		handle, _, err := acquirePolecatAdmission(town, "hisn", "hisn-work", "spawn-or-reuse")
		if err != nil {
			t.Fatal(err)
		}
		var info *SpawnedPolecatInfo
		if withPartial {
			info = &SpawnedPolecatInfo{}
		}
		transferSpawnAdmission(info, errors.New("allocation failed"), handle)
		available, _, err := acquirePolecatAdmission(town, "hisn", "hisn-after", "test")
		if err != nil {
			t.Fatalf("failed allocation leaked its slot: %v", err)
		}
		available.Release()
	}
}

func TestSpawnAdmissionCallerOwnedAndAbsentAreSafe(t *testing.T) {
	var absent *SpawnedPolecatInfo
	absent.ReleaseAdmission()
	info := &SpawnedPolecatInfo{}
	transferSpawnAdmission(info, nil, nil) // Formula caller owns the reservation.
	info.ReleaseAdmission()
}

func TestSpawnAdmissionReleasedWhenDeferredStartupFails(t *testing.T) {
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\n")
	handle, _, err := acquirePolecatAdmission(town, "hisn", "hisn-work", "spawn-or-reuse")
	if err != nil {
		t.Fatal(err)
	}
	info := &SpawnedPolecatInfo{RigName: "hisn", PolecatName: "new"}
	transferSpawnAdmission(info, nil, handle)
	defer info.ReleaseAdmission()
	// Fail before any real session or worktree access, after allocation returned.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	t.Setenv("GT_ROOT", "")
	t.Setenv("GT_TOWN_ROOT", "")
	if _, err := info.StartSession(); err == nil {
		t.Fatal("expected missing-town startup failure")
	}
	available, _, err := acquirePolecatAdmission(town, "hisn", "hisn-after", "test")
	if err != nil {
		t.Fatalf("startup failure leaked its reservation: %v", err)
	}
	available.Release()
}
