//go:build integration

package polecat

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/git"
	"github.com/steveyegge/gastown/internal/rig"
	"github.com/steveyegge/gastown/internal/testutil"
	"github.com/steveyegge/gastown/internal/tmux"
)

// runGitOrFatal runs a git command in dir, failing the test on error. Used to
// build a real bare-ish repo + worktree so RemoveWithOptions' worktree/stash
// handling is exercised for real, not mocked.
func runGitOrFatal(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v (dir=%s): %v\n%s", args, dir, err, output)
	}
}

// setupReconcileTestRig creates a townRoot/rig/mayor-rig git repo plus beads
// DB, wired the same way the other polecat manager integration tests are, and
// returns the Manager and rig for a not-yet-created polecat named "toast".
func setupReconcileTestRig(t *testing.T, tm *tmux.Tmux) (*Manager, *rig.Rig, string) {
	t.Helper()
	testutil.RequireDoltContainer(t)
	if _, err := exec.LookPath("bd"); err != nil {
		t.Skip("bd not installed, skipping integration test")
	}

	n := polecatManagerIntegrationCounter.Add(1)
	prefix := fmt.Sprintf("pmrc%d", n)

	townRoot := t.TempDir()
	rigName := "testrig"
	rigPath := filepath.Join(townRoot, rigName)
	mayorRigPath := filepath.Join(rigPath, "mayor", "rig")

	if err := os.MkdirAll(mayorRigPath, 0755); err != nil {
		t.Fatalf("mkdir mayor rig path: %v", err)
	}

	// Real git repo at mayor/rig so `git worktree add` / `git worktree remove`
	// (exercised by RemoveWithOptions) behave exactly as they do in production.
	runGitOrFatal(t, mayorRigPath, "init", "-q", "-b", "main")
	runGitOrFatal(t, mayorRigPath, "config", "user.email", "test@example.com")
	runGitOrFatal(t, mayorRigPath, "config", "user.name", "Test")
	if err := os.WriteFile(filepath.Join(mayorRigPath, "README.md"), []byte("test\n"), 0644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runGitOrFatal(t, mayorRigPath, "add", "README.md")
	runGitOrFatal(t, mayorRigPath, "commit", "-q", "-m", "initial")

	rigBeadsDir := filepath.Join(rigPath, ".beads")
	if err := os.MkdirAll(rigBeadsDir, 0755); err != nil {
		t.Fatalf("mkdir rig .beads: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rigBeadsDir, "redirect"), []byte("mayor/rig/.beads\n"), 0644); err != nil {
		t.Fatalf("write rig redirect: %v", err)
	}

	townBeadsDir := filepath.Join(townRoot, ".beads")
	if err := os.MkdirAll(townBeadsDir, 0755); err != nil {
		t.Fatalf("mkdir town .beads: %v", err)
	}
	routes := []beads.Route{
		{Prefix: "hq-", Path: "."},
		{Prefix: prefix + "-", Path: filepath.Join(rigName, "mayor", "rig")},
	}
	if err := beads.WriteRoutes(townBeadsDir, routes); err != nil {
		t.Fatalf("write routes: %v", err)
	}

	initBeadsDBWithPrefix(t, mayorRigPath, prefix)

	r := &rig.Rig{Name: rigName, Path: rigPath}
	mgr := NewManager(r, git.NewGit(rigPath), tm)

	// Simulate the reaper having already auto-closed toast's agent bead as
	// stale (gt-gjxb): create + immediately close it, mirroring
	// reaper.AutoClose's UPDATE ... SET status='closed'.
	agentID := mgr.agentBeadID("toast")
	assignee := mgr.assigneeID("toast")
	if _, err := mgr.beads.CreateOrReopenAgentBead(agentID, assignee, &beads.AgentFields{
		AgentState: string(beads.AgentStateIdle),
	}); err != nil {
		t.Fatalf("create agent bead: %v", err)
	}
	closed := "closed"
	if err := mgr.beads.Update(agentID, beads.UpdateOptions{Status: &closed}); err != nil {
		t.Fatalf("close agent bead: %v", err)
	}

	return mgr, r, rigPath
}

// TestReconcileStaleClosedWorktree_RemovesOrphanedStash reproduces the gt-gjxb
// scenario end to end: a polecat's agent bead is already closed (as the
// reaper's AutoClose would leave it), its worktree has an intentional
// stash (the exact thing DecideWorkstate treats as a capacity-blocking
// signal), and no tmux session is running. Reconciliation must remove the
// worktree despite the stash so the capacity slot stops leaking.
func TestReconcileStaleClosedWorktree_RemovesOrphanedStash(t *testing.T) {
	mgr, _, rigPath := setupReconcileTestRig(t, nil)

	worktreePath := filepath.Join(rigPath, "polecats", "toast", "testrig")
	if err := os.MkdirAll(filepath.Dir(worktreePath), 0755); err != nil {
		t.Fatalf("mkdir polecat dir: %v", err)
	}
	mayorRigPath := filepath.Join(rigPath, "mayor", "rig")
	runGitOrFatal(t, mayorRigPath, "worktree", "add", "-q", "-b", "polecat/toast/stale", worktreePath, "main")

	// Leave an unresolved stash — the intentional capacity-blocking signal
	// from workstate.go that RemoveWithOptions normally refuses to bypass.
	if err := os.WriteFile(filepath.Join(worktreePath, "wip.txt"), []byte("work in progress\n"), 0644); err != nil {
		t.Fatalf("write wip file: %v", err)
	}
	runGitOrFatal(t, worktreePath, "add", "wip.txt")
	runGitOrFatal(t, worktreePath, "stash", "push", "-q", "-m", "abandoned work")

	if !mgr.exists("toast") {
		t.Fatalf("precondition: expected toast polecat dir to exist before reconcile")
	}

	// Sanity check: a normal (non-nuclear) removal must still refuse to
	// proceed while the stash is present — otherwise this test would prove
	// nothing about the nuclear bypass path.
	if err := mgr.Remove("toast", true); err == nil {
		t.Fatalf("Remove(force=true) unexpectedly succeeded despite leftover stash")
	}

	reconciled, err := mgr.ReconcileStaleClosedWorktree("toast")
	if err != nil {
		t.Fatalf("ReconcileStaleClosedWorktree: %v", err)
	}
	if !reconciled {
		t.Fatalf("ReconcileStaleClosedWorktree reconciled = false, want true")
	}
	if mgr.exists("toast") {
		t.Fatalf("polecat dir for toast still exists after reconciliation")
	}
}

// TestReconcileStaleClosedWorktree_NoWorktreeIsNoop covers the case where the
// agent bead was closed but the worktree was already cleaned up some other
// way — reconciliation should be a harmless no-op, not an error.
func TestReconcileStaleClosedWorktree_NoWorktreeIsNoop(t *testing.T) {
	mgr, _, _ := setupReconcileTestRig(t, nil)

	reconciled, err := mgr.ReconcileStaleClosedWorktree("toast")
	if err != nil {
		t.Fatalf("ReconcileStaleClosedWorktree: %v", err)
	}
	if reconciled {
		t.Fatalf("ReconcileStaleClosedWorktree reconciled = true, want false (no worktree present)")
	}
}

// TestReconcileStaleClosedWorktree_SkipsLiveSession guards against destroying
// real work: if a tmux session is still alive for the polecat, the earlier
// bead closure was premature (or the polecat has since been respawned), so
// reconciliation must leave the worktree alone rather than nuking it.
func TestReconcileStaleClosedWorktree_SkipsLiveSession(t *testing.T) {
	requireTmuxIntegration(t)
	tm := tmux.NewTmux()
	mgr, r, rigPath := setupReconcileTestRig(t, tm)

	worktreePath := filepath.Join(rigPath, "polecats", "toast", "testrig")
	if err := os.MkdirAll(filepath.Dir(worktreePath), 0755); err != nil {
		t.Fatalf("mkdir polecat dir: %v", err)
	}
	mayorRigPath := filepath.Join(rigPath, "mayor", "rig")
	runGitOrFatal(t, mayorRigPath, "worktree", "add", "-q", "-b", "polecat/toast/live", worktreePath, "main")

	sessionName := NewSessionManager(tm, r).SessionName("toast")
	startLiveSession(t, sessionName)

	reconciled, err := mgr.ReconcileStaleClosedWorktree("toast")
	if err != nil {
		t.Fatalf("ReconcileStaleClosedWorktree: %v", err)
	}
	if reconciled {
		t.Fatalf("ReconcileStaleClosedWorktree reconciled = true, want false (live session must block reconciliation)")
	}
	if !mgr.exists("toast") {
		t.Fatalf("polecat dir for toast was removed despite a live tmux session")
	}
}
