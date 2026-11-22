package deacon

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

// heldExitError returns a real *exec.ExitError with ExitCodeDispatchHeld.
func heldExitError(t *testing.T) error {
	t.Helper()
	err := exec.Command("sh", "-c", "exit 75").Run()
	if !capacity.IsDispatchHeldExit(err) {
		t.Fatalf("precondition: %v is not a held exit", err)
	}
	return err
}

// hq-mw7902 review: --force never overrides a hold, and a held refusal is a
// skip, not a failed redispatch attempt.
func TestRedispatchHeldBeadIsSkipNotFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stub")
	}
	townRoot := t.TempDir()
	binDir := filepath.Join(townRoot, "bin")
	for _, dir := range []string{binDir, filepath.Join(townRoot, ".beads"), filepath.Join(townRoot, "gastown")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"), []byte(`{"prefix":"gt-","path":"gastown"}`+"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	bd := "#!/bin/sh\necho '[{\"id\":\"gt-held\",\"status\":\"open\"}]'\n"
	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(bd), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	held := heldExitError(t)
	prev := slingBeadFn
	slingBeadFn = func(string, string, string, string) error { return held }
	t.Cleanup(func() { slingBeadFn = prev })

	result := Redispatch(townRoot, "gt-held", "gastown", 3, 0)
	if result.Action != "skipped" || result.Error != nil {
		t.Fatalf("Redispatch = %+v, want skipped", result)
	}
	state, err := LoadRedispatchState(townRoot)
	if err != nil {
		t.Fatal(err)
	}
	if got := state.GetBeadState("gt-held").AttemptCount; got != 0 {
		t.Fatalf("held skip recorded %d attempts, want 0", got)
	}
}
