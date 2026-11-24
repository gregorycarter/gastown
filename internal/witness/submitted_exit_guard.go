package witness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/git"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/tmux"
)

// submittedExitNudgeVeto returns why the patrol must not nudge a
// submitted-still-running polecat to exit, or "" when the nudge may go.
//
// Submission evidence on the agent bead (active_mr / mr_id, cleanup=clean)
// outlives the submission: a worker whose MR was closed or superseded is
// re-slung, or resumes the same bead, in the same session, and the agent bead
// still carries the old MR. On 24 Nov 2026 the scan nudged nuka to exit
// while kasb-qyar.121.11 was hooked to it, tests ran in its sandbox and its
// worktree held uncommitted source work (kasb-2ifwk). The worker is busy
// when any of these holds:
//   - a hooked or assigned source bead is open and assigned to it;
//   - its worktree has uncommitted tracked changes;
//   - an async preflight runs for its sandbox, or any process other than
//     the session's own agent stack (pane, shell, agent binary and its
//     helpers) uses the sandbox.
//
// Every check fails closed: a lookup error is itself a veto.
func submittedExitNudgeVeto(bd *BdCli, workDir, townRoot, rigName, polecatName, sessionName, snapHook string, snap *agentBeadSnapshot, t *tmux.Tmux) string {
	if reason, err := assignedOpenWorkReason(bd, workDir, townRoot, rigName, polecatName, snapHook, snap); err != nil {
		return "assigned_work=unknown: " + err.Error()
	} else if reason != "" {
		return reason
	}
	rigPath := filepath.Join(townRoot, rigName)
	sandboxDir := filepath.Join(rigPath, "polecats", polecatName)
	if reason, err := submittedExitWorktreeDirty(polecatClonePath(sandboxDir, rigName)); err != nil {
		return "worktree_dirty=unknown: " + err.Error()
	} else if reason != "" {
		return reason
	}
	return submittedExitSandboxActivity(t, sessionName, rigPath, sandboxDir)
}

// assignedOpenWorkReason reports a non-terminal bead assigned to the polecat:
// its hook (from the hook column or the description hook_bead, looked up in
// the bead's own database) or any hooked, in_progress, open or blocked bead
// assigned to it in its rig database. gt done unassigns the source it holds
// for merge, so an assigned open bead is live work, not a finished one.
func assignedOpenWorkReason(bd *BdCli, workDir, townRoot, rigName, polecatName, snapHook string, snap *agentBeadSnapshot) (string, error) {
	agent := fmt.Sprintf("%s/polecats/%s", rigName, polecatName)
	hooks := []string{}
	if h := strings.TrimSpace(snapHook); h != "" {
		hooks = append(hooks, h)
	}
	if snap != nil && snap.Fields != nil {
		if h := strings.TrimSpace(snap.Fields.HookBead); h != "" && h != strings.TrimSpace(snapHook) {
			hooks = append(hooks, h)
		}
	}
	for _, hook := range hooks {
		output, err := bd.Exec(beads.ResolveHookDir(townRoot, hook, workDir), "show", hook, "--json")
		if err != nil {
			return "", fmt.Errorf("bd show %s: %w", hook, err)
		}
		if strings.TrimSpace(output) == "" {
			return "", fmt.Errorf("bd show %s: empty output", hook)
		}
		var issues []beads.Issue
		if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &issues); err != nil {
			return "", fmt.Errorf("bd show %s: %w", hook, err)
		}
		if len(issues) == 0 {
			continue // reaped: not assigned work
		}
		issue := issues[0]
		if !beads.IssueStatus(issue.Status).IsTerminal() && strings.TrimSpace(issue.Assignee) == agent {
			return fmt.Sprintf("assigned_work=%s status=%s", hook, issue.Status), nil
		}
	}

	listDir := workDir
	if rigDir := beads.GetRigDirForName(townRoot, rigName); rigDir != "" {
		listDir = rigDir
	}
	output, err := bd.Exec(listDir, "list", "--assignee="+agent, "--status=hooked,in_progress,open,blocked", "--json", "--limit=1")
	if err != nil {
		return "", fmt.Errorf("bd list --assignee=%s: %w", agent, err)
	}
	var assigned []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(output)), &assigned); err != nil {
		return "", fmt.Errorf("bd list --assignee=%s: %w", agent, err)
	}
	if len(assigned) > 0 {
		return fmt.Sprintf("assigned_work=%s status=%s", assigned[0].ID, assigned[0].Status), nil
	}
	return "", nil
}

// polecatClonePath mirrors polecat.Manager.clonePath: polecats/<name>/<rig>/,
// or the legacy polecats/<name>/ when that is the worktree.
func polecatClonePath(sandboxDir, rigName string) string {
	newPath := filepath.Join(sandboxDir, rigName)
	if info, err := os.Stat(newPath); err == nil && info.IsDir() {
		return newPath
	}
	if _, err := os.Stat(filepath.Join(sandboxDir, ".git")); err == nil {
		return sandboxDir
	}
	return newPath
}

// submittedExitWorktreeDirty reports uncommitted tracked changes (modified,
// added, deleted or unmerged files) in the polecat's worktree; untracked
// files alone do not count. Tests replace it.
var submittedExitWorktreeDirty = func(clonePath string) (string, error) {
	status, err := git.NewGit(clonePath).Status()
	if err != nil {
		return "", err
	}
	n := len(status.Modified) + len(status.Added) + len(status.Deleted) + len(status.Unmerged)
	if n == 0 {
		return "", nil
	}
	return fmt.Sprintf("worktree_dirty=%d uncommitted tracked change(s)", n), nil
}

// submittedExitSandboxActivity reports an async preflight for the sandbox or
// a live process in it other than the session's own agent stack (an idle
// agent's pane, shell and binary always sit in the worktree). When the stack
// cannot be resolved nothing is excluded, so the agent itself counts as
// activity and the nudge is withheld (fail closed). Tests replace it.
var submittedExitSandboxActivity = func(t *tmux.Tmux, sessionName, rigPath, sandboxDir string) string {
	exclude := map[int]bool{}
	if t != nil {
		if pids, err := t.AgentStackPIDs(sessionName); err == nil {
			for _, pid := range pids {
				exclude[pid] = true
			}
		}
	}
	return polecat.WorktreeActivityBlockerExcluding(rigPath, sandboxDir, time.Now(), exclude)
}
