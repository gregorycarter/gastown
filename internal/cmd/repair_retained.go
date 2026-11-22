package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/session"
)

// hq-3zchm: the same-MR recovery sweep runs on every daemon tick. When a
// blocked needs-repair MR cannot be queued (worker gone, reason missing,
// head moved) it used to be skipped silently, so a repair could sit for
// hours with no worker and nothing in any log. Report each retained MR
// once per reason per hour; the daemon log carries the scheduler's stderr.

const retainedRepairReportInterval = time.Hour

type retainedRepairRecord struct {
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

func retainedRepairStatePath(town string) string {
	return filepath.Join(town, ".runtime", "repair-retained.json")
}

// shouldReportRetainedRepair records the report and returns true when this
// MR's reason changed or was last reported more than an hour ago.
func shouldReportRetainedRepair(town, mrID, reason string, now time.Time) bool {
	path := retainedRepairStatePath(town)
	state := map[string]retainedRepairRecord{}
	if data, err := os.ReadFile(path); err == nil { //nolint:gosec // G304: trusted town path
		_ = json.Unmarshal(data, &state)
	}
	prev, ok := state[mrID]
	if ok && prev.Reason == reason && now.Sub(prev.At) < retainedRepairReportInterval {
		return false
	}
	state[mrID] = retainedRepairRecord{Reason: reason, At: now.UTC()}
	// Forget MRs not seen for a day so the file stays small.
	for id, rec := range state {
		if now.Sub(rec.At) > 24*time.Hour {
			delete(state, id)
		}
	}
	if data, err := json.Marshal(state); err == nil {
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		tmp := path + ".tmp"
		if os.WriteFile(tmp, data, 0o600) == nil {
			_ = os.Rename(tmp, path)
		}
	}
	return true
}

func reportRetainedRepair(town, mrID string, err error, dry bool) {
	reason := err.Error()
	if !dry && !shouldReportRetainedRepair(town, mrID, reason, time.Now()) {
		return
	}
	fmt.Fprintf(os.Stderr, "MR recovery %s retained, no worker queued: %s\n", mrID, reason)
}

// parkedBehindOwnMR reports whether an assignment is only waiting for its own
// merge request (gt done left it hooked with awaiting-merge:<mr>). Such work
// is not active: before hq-3zchm a worker holding two MRs could never take a
// repair for either, because each source counted as the other's active work.
func parkedBehindOwnMR(issue *beads.Issue) bool {
	if issue == nil {
		return false
	}
	for _, label := range issue.Labels {
		if strings.HasPrefix(label, "awaiting-merge:") {
			return true
		}
	}
	return false
}

// restoreRepairWorkerBranch puts an idle original worker's worktree back on
// the branch of the MR it must repair. A polecat that finished the MR and was
// then reused for other work keeps that other branch checked out, so the
// same-MR recovery check ("branch --show-current") failed on every tick and
// the repair never got a session (kasb RCA 22 Nov, H7).
//
// It only acts when the switch cannot lose anything: no live session, a clean
// worktree, the MR's pushed head still equal to its recorded commit, and that
// commit present locally after a fetch. The previous branch stays as a ref.
func restoreRepairWorkerBranch(town, rigName string, mr *beads.Issue, ops mqResumeWorkerOps) error {
	f := beads.ParseMRFields(mr)
	if f == nil || f.Worker == "" || f.Branch == "" || !mqResumeSHA.MatchString(f.CommitSHA) {
		return fmt.Errorf("invalid MR mapping")
	}
	spec, err := recoveryRig(rigName)
	if err != nil {
		return err
	}
	if !workerNamePattern.MatchString(f.Worker) {
		return fmt.Errorf("invalid worker name")
	}
	root := filepath.Join(town, rigName, "polecats", f.Worker)
	if stat, err := os.Stat(filepath.Join(root, rigName)); err == nil && stat.IsDir() {
		root = filepath.Join(root, rigName)
	}
	if real, err := filepath.EvalSymlinks(root); err != nil || real != root {
		return fmt.Errorf("original worker worktree missing or redirected")
	}
	running, err := ops.running(session.PolecatSessionName(spec.Prefix, f.Worker))
	if err != nil || running {
		return fmt.Errorf("worker session is live or unknown")
	}
	if status, err := ops.git(root, "status", "--porcelain"); err != nil || status != "" {
		return fmt.Errorf("worker worktree is not clean")
	}
	if current, err := ops.git(root, "branch", "--show-current"); err != nil || current == f.Branch {
		return fmt.Errorf("worker already on the MR branch")
	}
	remote, err := ops.git(root, "ls-remote", "--refs", "origin", "refs/heads/"+f.Branch)
	if err != nil || remote != f.CommitSHA+"\trefs/heads/"+f.Branch {
		return fmt.Errorf("MR pushed head changed or unavailable")
	}
	// Never move a local MR branch that holds commits the remote lacks.
	if local, err := ops.git(root, "rev-parse", "--verify", "--quiet", "refs/heads/"+f.Branch); err == nil && local != "" && local != f.CommitSHA {
		return fmt.Errorf("local MR branch differs from its pushed head")
	}
	if _, err := ops.git(root, "fetch", "origin", f.Branch); err != nil {
		return fmt.Errorf("fetching MR branch: %w", err)
	}
	if _, err := ops.git(root, "checkout", "-B", f.Branch, f.CommitSHA); err != nil {
		return fmt.Errorf("switching worker to MR branch: %w", err)
	}
	fmt.Fprintf(os.Stderr, "MR recovery %s: restored worker %s to %s at %s for its repair\n", mr.ID, f.Worker, f.Branch, f.CommitSHA[:12])
	return nil
}

func restorableWorkerBranchError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "original worker Git state changed (branch --show-current)")
}
