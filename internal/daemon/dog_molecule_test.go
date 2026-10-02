package daemon

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// fakeBdRecorder installs a bd stand-in that records every invocation.
func fakeBdRecorder(t *testing.T) (bdPath, marker string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell stub")
	}
	dir := t.TempDir()
	marker = filepath.Join(dir, "invoked")
	bdPath = filepath.Join(dir, "bd")
	script := "#!/bin/sh\necho \"$@\" >> " + marker + "\necho 'Spawned wisp: hq-wisp-fake1'\n"
	if err := os.WriteFile(bdPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bdPath, marker
}

// Dog runs must not create wisps: pouring a persistent molecule on every
// periodic run leaked ~2,500 wisps/day in hq (blocked step chains could not be
// closed in listing order, and the root then refused to close).
func TestDogMolDoesNotCreateWisps(t *testing.T) {
	bdPath, marker := fakeBdRecorder(t)
	d := &Daemon{logger: log.New(io.Discard, "", 0), bdPath: bdPath}

	mol := d.pourDogMolecule("mol-dog-doctor", map[string]string{"port": "3307"})
	mol.closeStep("probe")
	mol.failStep("inspect", "boom")
	mol.closeStep("report")
	mol.close()

	if _, err := os.Stat(marker); err == nil {
		got, _ := os.ReadFile(marker)
		t.Fatalf("dog molecule invoked bd (would create wisps): %s", got)
	}
}

func TestDogMolLogsFailedStepsOnceOnClose(t *testing.T) {
	var buf bytes.Buffer
	d := &Daemon{logger: log.New(&buf, "", 0)}

	mol := d.pourDogMolecule("mol-dog-jsonl", nil)
	mol.closeStep("export")
	mol.failStep("push", "spike detected")
	mol.closeStep("push") // a later close must not hide the failure
	mol.close()
	mol.close() // idempotent: deferred close plus explicit close

	out := buf.String()
	if strings.Count(out, "dog_run:") != 1 {
		t.Fatalf("want exactly one dog_run line, got %q", out)
	}
	if !strings.Contains(out, "mol-dog-jsonl") || !strings.Contains(out, "push: spike detected") {
		t.Fatalf("missing formula or failure reason: %q", out)
	}
}

func TestDogMolQuietOnSuccess(t *testing.T) {
	var buf bytes.Buffer
	d := &Daemon{logger: log.New(&buf, "", 0)}

	mol := d.pourDogMolecule("mol-dog-checkpoint", nil)
	mol.closeStep("scan")
	mol.closeStep("checkpoint")
	mol.closeStep("report")
	mol.close()

	if buf.Len() != 0 {
		t.Fatalf("successful run should not log, got %q", buf.String())
	}
}

func TestDogMolNilSafe(t *testing.T) {
	var dm *dogMol
	dm.closeStep("scan")
	dm.failStep("scan", "x")
	dm.close()

	var d *Daemon
	mol := d.pourDogMolecule("mol-dog-doctor", nil)
	mol.failStep("probe", "")
	mol.close() // no logger: must not panic
}

// runDoctorDog fired every 5 minutes and poured a molecule that no agent ever
// executed; the tick must now be bead-free.
func TestRunDoctorDogCreatesNoWisps(t *testing.T) {
	bdPath, marker := fakeBdRecorder(t)
	d := &Daemon{
		logger: log.New(io.Discard, "", 0),
		bdPath: bdPath,
		patrolConfig: &DaemonPatrolConfig{Patrols: &PatrolsConfig{
			DoctorDog: &DoctorDogConfig{Enabled: true},
		}},
	}

	d.runDoctorDog()

	if _, err := os.Stat(marker); err == nil {
		got, _ := os.ReadFile(marker)
		t.Fatalf("runDoctorDog invoked bd: %s", got)
	}
}
