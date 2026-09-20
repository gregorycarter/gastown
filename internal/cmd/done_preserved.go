package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/checkpoint"
)

func shouldCheckpointBlockedDone(rig, exitType, issue string) bool {
	return rig == "hisn" && (exitType == ExitDeferred || exitType == ExitEscalated) && strings.HasPrefix(issue, "hisn-") && !strings.Contains(issue, "-wfs-") && !strings.Contains(issue, "-wisp-")
}

// An explicit deferred exit releases execution, not ownership or disk contents.
// Persist the original branch/head and source before allowing session shutdown.
func checkpointBlockedDone(town, cwd, actor, issue, branch, exitType string) error {
	b := mqResumeBeads(town)
	source, err := b.Show(issue)
	if err != nil {
		return err
	}
	if source.Assignee != actor || (source.Status != "hooked" && source.Status != "in_progress" && source.Status != "blocked" && source.Status != "open") {
		return fmt.Errorf("blocked exit source ownership/status changed; session retained")
	}
	head, err := mqResumeGit(cwd, "rev-parse", "HEAD")
	if err != nil || !mqResumeSHA.MatchString(head) {
		return fmt.Errorf("blocked exit HEAD unavailable")
	}
	actualBranch, err := mqResumeGit(cwd, "branch", "--show-current")
	if err != nil || actualBranch != branch {
		return fmt.Errorf("blocked exit branch changed")
	}
	if _, err := mqResumeGit(cwd, "status", "--porcelain"); err != nil {
		return err
	}
	cp, err := checkpoint.Capture(cwd)
	if err != nil {
		return err
	}
	if cp.LastCommit != head || cp.Branch != branch {
		return fmt.Errorf("blocked exit git snapshot changed")
	}
	cp.WithHookedBead(issue).WithNotes(exitType + ": worktree and unpublished changes preserved; resume original work when recorded prerequisites close")
	if err := checkpoint.Write(cwd, cp); err != nil {
		return err
	}
	if source.Status != "blocked" {
		if err := BdCmd("update", issue, "--if-status="+source.Status, "--status=blocked").Dir(cwd).WithBeadsDir(beads.ResolveBeadsDir(filepath.Join(town, "hisn"))).WithAutoCommit().Run(); err != nil {
			return err
		}
	}
	after, err := b.Show(issue)
	if err != nil {
		return err
	}
	if after.Status != "blocked" || after.Assignee != source.Assignee || after.Description != source.Description {
		return fmt.Errorf("blocked exit source update unconfirmed")
	}
	return nil
}

// Escalation preserves the source and its molecule just as deferral does.
func shouldFinishDoneSource(exitType, issue string) bool {
	return exitType == ExitCompleted || (exitType == ExitDeferred && strings.Contains(issue, "-wfs-"))
}
