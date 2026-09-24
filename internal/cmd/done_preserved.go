package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/checkpoint"
)

func shouldCheckpointBlockedDone(rig, exitType, issue string) bool {
	return rig == "hisn" && (exitType == ExitDeferred || exitType == ExitEscalated) && strings.HasPrefix(issue, "hisn-") && !strings.Contains(issue, "-wfs-") && !strings.Contains(issue, "-wisp-")
}

type blockedDoneHandoff struct {
	Branch           string `json:"branch"`
	Head             string `json:"head"`
	LastStep         string `json:"lastStep"`
	FailingSignature string `json:"failingSignature"`
	NextAction       string `json:"nextAction"`
	HoldUntil        string `json:"holdUntil"`
}

// A deferred exit preserves a remotely resumable branch and a bounded handoff
// before releasing the worker. Failed pushes leave the original session intact.
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
	status, err := mqResumeGit(cwd, "status", "--porcelain")
	if err != nil {
		return err
	}
	if status != "" {
		return fmt.Errorf("blocked exit has unpublished worktree changes; session retained")
	}
	cp, err := checkpoint.Capture(cwd)
	if err != nil {
		return err
	}
	if cp.LastCommit != head || cp.Branch != branch {
		return fmt.Errorf("blocked exit git snapshot changed")
	}
	if !mqResumeBranch.MatchString(branch) {
		return fmt.Errorf("blocked exit branch cannot be resumed by another worker")
	}
	if _, err := mqResumeGit(cwd, "push", "origin", "HEAD:refs/heads/"+branch); err != nil {
		return fmt.Errorf("blocked exit branch push failed; session retained: %w", err)
	}
	remote, err := mqResumeGit(cwd, "ls-remote", "origin", "refs/heads/"+branch)
	if err != nil || !strings.HasPrefix(remote, head+"\t") {
		return fmt.Errorf("blocked exit pushed head could not be verified; session retained")
	}
	handoff := blockedDoneHandoff{Branch: branch, Head: head, LastStep: exitType,
		NextAction: "Resume on the pushed branch, satisfy the recorded prerequisite, then rerun preflight and gt done",
		HoldUntil:  time.Now().UTC().Add(time.Hour).Format(time.RFC3339)}
	raw, err := json.Marshal(handoff)
	if err != nil {
		return err
	}
	cp.WithHookedBead(issue).WithNotes(exitType + ": pushed branch and structured Beads handoff; any worker may resume after the prerequisite or timeout")
	if err := checkpoint.Write(cwd, cp); err != nil {
		return err
	}
	if err := BdCmd("update", issue, "--if-status="+source.Status, "--if-assignee="+actor,
		"--status=blocked", "--assignee=", "--set-metadata=handoff="+string(raw),
		"--set-metadata=holdUntil="+handoff.HoldUntil).Dir(cwd).
		WithBeadsDir(beads.ResolveBeadsDir(filepath.Join(town, "hisn"))).WithAutoCommit().Run(); err != nil {
		return err
	}
	after, err := b.Show(issue)
	if err != nil {
		return err
	}
	if after.Status != "blocked" || after.Assignee != "" || after.Description != source.Description {
		return fmt.Errorf("blocked exit source update unconfirmed")
	}
	return nil
}

// Escalation preserves the source and its molecule just as deferral does.
func shouldFinishDoneSource(exitType, issue string) bool {
	return exitType == ExitCompleted || (exitType == ExitDeferred && strings.Contains(issue, "-wfs-"))
}
