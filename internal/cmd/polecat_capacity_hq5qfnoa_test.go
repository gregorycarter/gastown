package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestReservation(t *testing.T, dir, id string, pid int, created time.Time) {
	t.Helper()
	r := polecatAdmissionReservation{
		ID: id, PID: pid, Rig: "gastown", Bead: "gt-" + id, Operation: "test", CreatedAt: created,
	}
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal reservation: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, id+".json"), data, 0644); err != nil {
		t.Fatalf("write reservation: %v", err)
	}
}

// hq-5qfnoa: a dispatch killed by the 5 m timeout is SIGKILLed, so its
// reservation's PID is dead with no Release. Past the short handoff grace it is
// reaped at once, not after the 30 m TTL.
func TestDeadPIDReservationReapedAfterGrace(t *testing.T) {
	townRoot := setupPolecatCapacityTestTown(t, 1)
	dir := polecatAdmissionDir(townRoot)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Dead PID, older than the grace but far younger than the TTL.
	writeTestReservation(t, dir, "deadgrace", 99999999, time.Now().Add(-(polecatAdmissionDeadPIDGrace + time.Minute)))
	snapshot, err := polecatCapacitySnapshotForTown(townRoot)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Reservations != 0 {
		t.Fatalf("dead-pid reservation past the grace should be reaped: %+v", snapshot)
	}
	if _, err := os.Stat(filepath.Join(dir, "deadgrace.json")); !os.IsNotExist(err) {
		t.Fatalf("reservation still exists: %v", err)
	}
}

// Within the handoff grace a dead-pid reservation is kept, so reaping never
// frees a slot out from under a just-spawned polecat that is still registering.
func TestDeadPIDReservationKeptWithinGrace(t *testing.T) {
	townRoot := setupPolecatCapacityTestTown(t, 1)
	dir := polecatAdmissionDir(townRoot)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeTestReservation(t, dir, "deadyoung", 99999999, time.Now().Add(-30*time.Second))
	snapshot, err := polecatCapacitySnapshotForTown(townRoot)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Reservations != 1 {
		t.Fatalf("dead-pid reservation within the handoff grace should be kept: %+v", snapshot)
	}
}

// A live-but-stuck dispatch keeps its slot for the full TTL; the shorter dead-pid
// grace must not apply to a live PID.
func TestLivePIDReservationKeptWithinTTL(t *testing.T) {
	townRoot := setupPolecatCapacityTestTown(t, 1)
	dir := polecatAdmissionDir(townRoot)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Live PID (this test process), older than the dead-pid grace but under the TTL.
	writeTestReservation(t, dir, "live", os.Getpid(), time.Now().Add(-5*time.Minute))
	snapshot, err := polecatCapacitySnapshotForTown(townRoot)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snapshot.Reservations != 1 {
		t.Fatalf("a live reservation within the TTL should be kept: %+v", snapshot)
	}
}
