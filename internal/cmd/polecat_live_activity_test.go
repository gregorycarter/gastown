package cmd

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/polecat"
)

func TestLiveActivityBlockers(t *testing.T) {
	cases := []struct {
		known, running bool
		activity       string
		want           []string
	}{
		{true, false, "", nil},
		{true, true, "", []string{"session_state=running"}},
		{false, false, "", []string{"session_state=unknown"}},
		{true, false, "async_preflight=running source=kasb-x", []string{"async_preflight=running source=kasb-x"}},
	}
	for _, tc := range cases {
		if got := liveActivityBlockers(tc.known, tc.running, tc.activity); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("liveActivityBlockers(%v,%v,%q) = %v, want %v", tc.known, tc.running, tc.activity, got, tc.want)
		}
	}
}

// kasb-84hoc: check-recovery said SAFE_TO_NUKE for raider while its session
// ran and a preflight ran in its worktree.
func TestApplyLiveActivityWithdrawsSafeToNuke(t *testing.T) {
	safe := func() RecoveryStatus {
		return RecoveryStatus{Verdict: polecat.WorkstateVerdictSafeToNuke, Reason: "reusable", SafeToNuke: true, Reusable: true}
	}

	status := safe()
	applyLiveActivityToRecoveryStatus(&status, []string{"session_state=running"})
	if status.Verdict != polecat.WorkstateVerdictWorking || status.SafeToNuke || status.Reason != recoveryReasonLiveSession {
		t.Fatalf("live session: %+v", status)
	}

	status = safe()
	applyLiveActivityToRecoveryStatus(&status, []string{"session_state=running", "live_process_in_worktree pid=23063(python3)"})
	if status.Verdict != polecat.WorkstateVerdictWorking || status.SafeToNuke || status.Reusable || status.Reason != recoveryReasonLiveActivity || len(status.Blockers) != 2 {
		t.Fatalf("live preflight: %+v", status)
	}

	status = safe()
	applyLiveActivityToRecoveryStatus(&status, nil)
	if status.Verdict != polecat.WorkstateVerdictSafeToNuke || !status.SafeToNuke {
		t.Fatalf("idle polecat lost SAFE_TO_NUKE: %+v", status)
	}

	pending := RecoveryStatus{Verdict: polecat.WorkstateVerdictPendingMR, Reason: "active-mr-open", Blockers: []string{"pending_mr=kasb-wisp-xb2pc status=blocked"}}
	applyLiveActivityToRecoveryStatus(&pending, []string{"session_state=running"})
	if pending.Verdict != polecat.WorkstateVerdictPendingMR || len(pending.Blockers) != 1 || len(pending.Diagnostics) != 1 {
		t.Fatalf("an unsafe verdict must keep its meaning: %+v", pending)
	}
}

// kasb-84hoc: a BLOCKED MR found only through the source bead keeps the
// verdict at PENDING_MR instead of SAFE_TO_NUKE.
func TestBlockedMRWithoutActiveMRPointerIsPendingMR(t *testing.T) {
	in := polecat.WorkstateInput{State: polecat.StateDone, CleanupStatus: polecat.CleanupClean, Branch: "polecat/raider/kasb-0osbb.1+x", SessionKnown: true}
	if d := polecat.DecideWorkstate(in); d.Verdict != polecat.WorkstateVerdictSafeToNuke {
		t.Fatalf("precondition: %+v", d)
	}
	in.ActiveMRBlocker = polecat.PendingMRBlocker(stubPendingMRFinder{status: "blocked"}, "raider", in.Branch, "kasb-0osbb.1")
	if d := polecat.DecideWorkstate(in); d.Verdict != polecat.WorkstateVerdictPendingMR || d.SafeToNuke {
		t.Fatalf("blocked MR not preserved: %+v", d)
	}
}

type stubPendingMRFinder struct{ status string }

func (s stubPendingMRFinder) FindMRForBranch(string) (*beads.Issue, error) { return nil, nil }

func (s stubPendingMRFinder) FindPendingMRsForIssue(string) ([]*beads.Issue, error) {
	return []*beads.Issue{{ID: "kasb-wisp-xb2pc", Status: s.status, Description: "branch: polecat/raider/kasb-0osbb.1+x\nsource_issue: kasb-0osbb.1\n"}}, nil
}

// kasb-84hoc: the witness's canonical home holds only runtime state, so the
// git reads of gt prime --check failed there and blocked its writes.
func TestPrimeCheckWitnessHomeWithoutWorktreeSkipsGit(t *testing.T) {
	town := makeTestGitRepo(t)
	home := filepath.Join(town, "kasb_core", "witness")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CEILING_DIRECTORIES", town)
	t.Setenv("TMUX", "")
	result, err := runTownPrimeCheck(t, RoleInfo{Role: RoleWitness, Rig: "kasb_core", Home: home, WorkDir: home, TownRoot: town})
	if err != nil {
		t.Fatalf("witness check failed: %v", err)
	}
	if result["ok"] != true || !strings.Contains(result["git_skipped"].(string), "not a git worktree") || result["head"] != nil {
		t.Fatalf("result = %v", result)
	}
}

func TestRoleHomeWithoutWorktreeOnlyForWitness(t *testing.T) {
	home := t.TempDir()
	for _, role := range []Role{RolePolecat, RoleCrew, RoleRefinery, RoleMayor, RoleDeacon} {
		if reason := roleHomeWithoutWorktree(RoleInfo{Role: role, Home: home, WorkDir: home}); reason != "" {
			t.Errorf("%s skipped git checks: %q", role, reason)
		}
	}
	if reason := roleHomeWithoutWorktree(RoleInfo{Role: RoleWitness, Home: home, WorkDir: home}); reason == "" {
		t.Error("witness home without .git must skip git checks")
	}
	if err := os.WriteFile(filepath.Join(home, ".git"), []byte("gitdir: /x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if reason := roleHomeWithoutWorktree(RoleInfo{Role: RoleWitness, Home: home, WorkDir: home}); reason != "" {
		t.Errorf("witness with its own worktree skipped git checks: %q", reason)
	}
}
