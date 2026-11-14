package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// rebaseGit is the subset of *git.Git that autoRebaseOnTarget needs. Defined as
// an interface so tests can drive the decision logic without standing up a full
// git repo for every gating case.
type rebaseGit interface {
	Rebase(onto string) error
	AbortRebase() error
}

// autoRebaseOnTarget rebases the current branch onto base when the branch is
// behind the target. It is a no-op when there is nothing to rebase, when the
// polecat ran the formula's pre-verify step (rebasing again would invalidate
// the gate results that --pre-verified attests to), or when a prior push
// checkpoint exists (rebasing after pushing would require a force-push).
//
// Returns:
//   - rebased: true if a rebase actually ran successfully.
//   - skipReason: non-empty when behind > 0 but the rebase was intentionally
//     skipped. Empty when behind == 0 (no rebase needed) or when rebased == true.
//   - err: rebase failure, after AbortRebase has been attempted to clean up.
//
// gh#3400.
func autoRebaseOnTarget(g rebaseGit, base string, behind int, preVerified, alreadyPushed bool) (rebased bool, skipReason string, err error) {
	if behind <= 0 {
		return false, "", nil
	}
	switch {
	case preVerified:
		return false, "--pre-verified is set", nil
	case alreadyPushed:
		return false, "prior push checkpoint exists", nil
	}

	fmt.Printf("→ Auto-rebasing onto %s (%d commits behind)\n", base, behind)
	if rebaseErr := g.Rebase(base); rebaseErr != nil {
		_ = g.AbortRebase()
		return false, "", fmt.Errorf("auto-rebase onto %s failed: %w\n"+
			"Resolve conflicts manually (git fetch origin && git rebase %s), commit the resolution, then rerun gt done.",
			base, rebaseErr, base)
	}
	return true, "", nil
}

// passingPreflightReceiptForHead returns the path of a passing preflight
// receipt that covers head for sourceID, or "" when there is none.
//
// hq-i2hc3: a receipted head must not be rebased by gt done. The receipt is
// bound to the head the polecat proved; a local auto-rebase makes a new head
// that needs another 60-90 minute preflight, while the Refinery already rebases
// at landing (and returns real conflicts to the same polecat as needs-rebase).
// Receipts live in <worktree>/.delivery/preflight-receipts/*.json (kasb schema:
// head_sha, source_bead_id, status). Per-stage partial receipts
// (*-<stage>-stage.json) never count.
func passingPreflightReceiptForHead(root, head, sourceID string) string {
	if root == "" || head == "" || sourceID == "" {
		return ""
	}
	files, _ := filepath.Glob(filepath.Join(root, ".delivery", "preflight-receipts", "*.json"))
	sort.Strings(files)
	for _, p := range files {
		if strings.HasSuffix(p, "-stage.json") {
			continue
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var r struct {
			HeadSHA    string `json:"head_sha"`
			SourceBead string `json:"source_bead_id"`
			Status     string `json:"status"`
		}
		if json.Unmarshal(raw, &r) != nil {
			continue
		}
		if r.HeadSHA != head || r.SourceBead != sourceID {
			continue
		}
		if r.Status == "passed" || r.Status == "passed_non_behavioral" {
			return p
		}
	}
	return ""
}
