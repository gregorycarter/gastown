package polecat

import (
	"fmt"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/git"
)

// A Witness restart must not turn a parked source back into live occupancy or
// resurrect a worker after operator takeover. Dependency recovery first hooks
// the validated original source through the pressure-admitted scheduler.
func (m *SessionManager) validateHisnStart(name, workDir string, opts SessionStartOptions) error {
	if m.rig.Name != "hisn" {
		return nil
	}
	id := opts.Issue
	if id == "" {
		branch, err := git.NewGit(workDir).CurrentBranch()
		if err != nil {
			return err
		}
		meta, ok := ParseBranchName(branch)
		if !ok {
			return nil
		}
		if meta.Polecat != name {
			return fmt.Errorf("preserved branch belongs to another worker")
		}
		id = meta.Issue
	}
	if id == "" {
		return nil
	}
	source, err := beads.New(m.rig.Path).Show(id)
	if err != nil {
		return fmt.Errorf("source startup state unavailable: %w", err)
	}
	return validateHisnStartSource(source, "hisn/polecats/"+name, opts)
}

func validateHisnStartSource(source *beads.Issue, actor string, opts SessionStartOptions) error {
	if source == nil {
		return fmt.Errorf("source startup state unavailable")
	}
	if source.Status == "blocked" || beads.IssueStatus(source.Status).IsTerminal() {
		return fmt.Errorf("source %s is %s; preserve work and wait for scheduler recovery", source.ID, source.Status)
	}
	if source.Assignee != "" && source.Assignee != actor {
		return fmt.Errorf("source %s belongs to %s; do not restart previous worker", source.ID, source.Assignee)
	}
	// prepareSessionLaunch has already run the admission callback and checked
	// preserved Git state. Ordinary restarts and dependency resumes have no MR
	// exception; a matching label alone is never permission to launch.
	branch, validBranch := ParseBranchName(opts.PreserveBranch)
	admittedMR := opts.RecoveryMR != "" && opts.BeforeLaunch != nil && opts.PreserveHead != "" &&
		validBranch && branch.Issue == source.ID && "hisn/polecats/"+branch.Polecat == actor &&
		opts.Issue == source.ID && source.Assignee == actor
	for _, label := range source.Labels {
		if admittedMR && label == "awaiting-merge:"+opts.RecoveryMR {
			continue
		}
		if label == "needs-operator" || label == "needs-operator-rollout" || label == "needs-review" || strings.HasPrefix(label, "awaiting-merge:") {
			return fmt.Errorf("source %s held by %s; preserve worker", source.ID, label)
		}
	}
	return nil
}
