package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
	"github.com/steveyegge/gastown/internal/workspace"
)

var workerNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func init() {
	var dry bool
	c := &cobra.Command{Use: "recover-dependencies hisn", Short: "Queue preserved workers whose recorded dependencies have closed", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "hisn" {
			return fmt.Errorf("dependency recovery is opt-in for hisn")
		}
		if err := validateMQResumeCaller(os.Getenv("GT_ROLE"), os.Getenv("GT_RIG"), os.Getenv("BD_ACTOR")); err != nil {
			return err
		}
		town, err := workspace.FindFromCwdOrError()
		if err != nil {
			return err
		}
		result, err := recoverDependencies(town, dry)
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
	}}
	c.Flags().BoolVar(&dry, "dry-run", false, "Read-only recovery assessment")
	mqCmd.AddCommand(c)
}

func dependencyResumeSource(source *beads.Issue, show func(string) (*beads.Issue, error)) (string, error) {
	if source == nil || (source.Status != "open" && source.Status != "blocked") || mqResumeHeld(source) || capacity.IsMessagingBead(source.Labels) || source.Type == "epic" {
		return "", fmt.Errorf("not preserved dependency-blocked work")
	}
	worker := strings.TrimPrefix(source.Assignee, "hisn/polecats/")
	if worker == source.Assignee || !workerNamePattern.MatchString(worker) {
		return "", fmt.Errorf("source has no original Hisn worker")
	}
	for _, label := range source.Labels {
		if strings.HasPrefix(label, "awaiting-merge:") || label == "needs-review" || label == "needs-rebase" {
			return "", fmt.Errorf("source requires MR disposition")
		}
	}
	hasBlocker := false
	for _, dep := range source.Dependencies {
		if dep.DependencyType == "blocks" && !strings.Contains(dep.ID, "-wisp-") {
			hasBlocker = true
		}
	}
	if !hasBlocker {
		return "", fmt.Errorf("no explicit work prerequisite; do not reinterpret a manual hold")
	}
	if err := validateMQResumeDependencies(source, show, map[string]bool{}, true); err != nil {
		return "", err
	}
	return worker, nil
}

// No resets, fresh molecules, new MRs or remote-head assumptions: unpublished
// commits stay on the original branch. Dirty worktrees remain preserved for review.
func loadDependencyResume(town, id string) (*mqResumeState, error) {
	if blocked, reason := IsRigParkedOrDocked(town, "hisn"); blocked {
		return nil, fmt.Errorf("rig %s", reason)
	}
	b := mqResumeBeads(town)
	source, err := b.Show(id)
	if err != nil {
		return nil, err
	}
	worker, err := dependencyResumeSource(source, b.Show)
	if err != nil {
		return nil, err
	}
	mrs, err := b.ListMergeRequests(beads.ListOptions{Label: "gt:merge-request", Status: "all", Limit: 0, Priority: -1})
	if err != nil {
		return nil, err
	}
	for _, mr := range mrs {
		if mr.Status == "closed" || mr.Status == "tombstone" {
			continue
		}
		f := beads.ParseMRFields(mr)
		if f == nil {
			return nil, fmt.Errorf("unknown MR mapping")
		}
		if f.SourceIssue == id || f.Worker == worker {
			return nil, fmt.Errorf("existing MR %s requires same-MR recovery", mr.ID)
		}
	}
	root := filepath.Join(town, "hisn", "polecats", worker, "hisn")
	branch, err := mqResumeGit(root, "branch", "--show-current")
	if err != nil {
		return nil, err
	}
	match := mqResumeBranch.FindStringSubmatch(branch)
	if match == nil || match[1] != worker || match[2] != id {
		return nil, fmt.Errorf("original worker branch does not match source")
	}
	head, err := mqResumeGit(root, "rev-parse", "HEAD")
	if err != nil || !mqResumeSHA.MatchString(head) {
		return nil, fmt.Errorf("original head unavailable")
	}
	signature, _ := json.Marshal(struct {
		ID, Status, Assignee, Description string
		Labels                            []string
	}{source.ID, source.Status, source.Assignee, source.Description, source.Labels})
	r := mqResumeRecord{SchemaVersion: 1, Kind: "dependency-resume", Rig: "hisn", Source: id, Worker: worker, Branch: branch, Submitted: head, DescriptionSHA256: mqResumeHash(string(signature))}
	r.Instructions = fmt.Sprintf("Resume preserved source %s on this original worker and branch. Its recorded prerequisites are closed. Preserve unpublished commits. Fetch main, integrate the prerequisite fixes, finish the original work and run required preflight. Reuse any original MR; never create replacement work. If blocked again, record the actual prerequisite and stop cleanly.", id)
	raw, _ := json.Marshal(r)
	state := &mqResumeState{Source: source, Record: r, ReceiptHash: mqResumeHash(string(raw))}
	if err := validateMQResumeWorker(town, state, b); err != nil {
		return nil, err
	}
	return state, nil
}

func recoverDependencies(town string, dry bool) ([]mqResumeResult, error) {
	result := []mqResumeResult{}
	max, err := configuredSchedulerMaxPolecats(town)
	if err != nil {
		return nil, err
	}
	if max <= 0 {
		return result, nil
	}
	scheduler, err := capacity.LoadState(town)
	if err != nil {
		return nil, err
	}
	if scheduler.Paused {
		return result, nil
	}
	if blocked, _ := IsRigParkedOrDocked(town, "hisn"); blocked {
		return result, nil
	}
	b := mqResumeBeads(town)
	for _, status := range []string{"blocked", "open"} {
		sources, err := b.List(beads.ListOptions{Status: status, Limit: 0, Priority: -1})
		if err != nil {
			return nil, err
		}
		for _, source := range sources {
			if !strings.HasPrefix(source.Assignee, "hisn/polecats/") {
				continue
			}
			state, err := loadDependencyResume(town, source.ID)
			if err != nil {
				continue
			}
			row := mqResumeOutput(state, "ready-to-queue", "")
			if !dry {
				row, err = queueMQResume(town, state, func() (*mqResumeState, error) { return loadDependencyResume(town, source.ID) }, b.ListOpenSlingContexts, b.CreateSlingContext, b.UpdateSlingContextFields)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Dependency recovery %s retained: %v\n", source.ID, err)
					continue
				}
			}
			result = append(result, row)
		}
	}
	// Existing recovery receipts retain their own exact MR and hold checks.
	mrs, err := b.ListMergeRequests(beads.ListOptions{Label: "gt:merge-request", Status: "blocked", Limit: 0, Priority: -1})
	if err != nil {
		return result, err
	}
	for _, mr := range mrs {
		state, err := loadMQResumeState(town, "hisn", mr.ID)
		if err != nil {
			continue
		}
		row := mqResumeOutput(state, "ready-to-queue", "")
		if !dry {
			row, err = queueMQResume(town, state, func() (*mqResumeState, error) { return loadMQResumeState(town, "hisn", mr.ID) }, b.ListOpenSlingContexts, b.CreateSlingContext, b.UpdateSlingContextFields)
			if err != nil {
				fmt.Fprintf(os.Stderr, "MR recovery %s retained: %v\n", mr.ID, err)
				continue
			}
		}
		result = append(result, row)
	}
	return result, nil
}
