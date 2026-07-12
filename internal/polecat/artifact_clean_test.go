package polecat

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/artifact"
)

func TestFormatArtifactHookResultIncludesMachineAccounting(t *testing.T) {
	result := artifact.Result{
		HookPoint: "polecat-pre-reuse", BytesConsidered: 10, BytesFreed: 7,
		PathsCleaned: []artifact.PathResult{}, PathsSkipped: []artifact.PathResult{},
	}
	line := formatArtifactHookResult(result)
	const prefix = "artifact-clean: "
	if !strings.HasPrefix(line, prefix) {
		t.Fatalf("missing log prefix: %q", line)
	}
	var got artifact.Result
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, prefix)), &got); err != nil {
		t.Fatalf("hook accounting is not JSON: %v", err)
	}
	if got.HookPoint != "polecat-pre-reuse" || got.BytesConsidered != 10 || got.BytesFreed != 7 {
		t.Fatalf("hook accounting lost fields: %+v", got)
	}
}

func TestExecuteArtifactCleanupHookDryRunApplyAndSafety(t *testing.T) {
	newWorktree := func(t *testing.T) string {
		t.Helper()
		root := t.TempDir()
		cmd := exec.Command("git", "init", "-q")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git init: %v: %s", err, out)
		}
		if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("dist/\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(root, "dist"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "dist", "bundle"), []byte("generated"), 0o644); err != nil {
			t.Fatal(err)
		}
		return root
	}

	policy := artifact.DefaultPolicy()
	policy.Enabled = true
	policy.OnPolecatReuse = true
	policy.Paths = []string{"dist"}
	cleanState := func() artifact.SafetyState { return artifact.SafetyState{MRVerified: true} }

	dryRoot := newWorktree(t)
	result, ran, err := executeArtifactCleanupHook(dryRoot, policy, cleanState)
	if err != nil || !ran || !result.DryRun || len(result.PathsCleaned) != 1 {
		t.Fatalf("dry-run hook failed: ran=%t result=%+v err=%v", ran, result, err)
	}
	if _, err := os.Stat(filepath.Join(dryRoot, "dist")); err != nil {
		t.Fatal("dry-run hook removed the artifact")
	}

	applyRoot := newWorktree(t)
	policy.Mode = artifact.ModeApply
	result, ran, err = executeArtifactCleanupHook(applyRoot, policy, cleanState)
	if err != nil || !ran || result.BytesFreed == 0 {
		t.Fatalf("apply hook failed: ran=%t result=%+v err=%v", ran, result, err)
	}
	if _, err := os.Stat(filepath.Join(applyRoot, "dist")); !os.IsNotExist(err) {
		t.Fatal("apply hook did not remove the artifact")
	}

	unsafeRoot := newWorktree(t)
	unsafeState := func() artifact.SafetyState { return artifact.SafetyState{MRVerified: true, ActiveMR: true} }
	result, _, err = executeArtifactCleanupHook(unsafeRoot, policy, unsafeState)
	if err != nil || !result.Refused {
		t.Fatalf("unsafe hook was not refused: result=%+v err=%v", result, err)
	}
	if _, err := os.Stat(filepath.Join(unsafeRoot, "dist")); err != nil {
		t.Fatal("unsafe hook removed the artifact")
	}
}
