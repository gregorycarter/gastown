package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
)

const conflictRemediationTitlePrefix = "Resolve merge conflicts:"

// conflictRemediationTask identifies the original merge request a refinery
// conflict task is meant to unblock.
type conflictRemediationTask struct {
	TaskID          string
	OriginalMRID    string
	Branch          string
	OriginalIssueID string
}

// parseConflictRemediationTask recognizes tasks produced by the refinery. A
// title match with incomplete metadata is an error so gt done cannot silently
// fall back to creating a second MR for the task.
func parseConflictRemediationTask(issue *beads.Issue) (*conflictRemediationTask, bool, error) {
	if issue == nil || !strings.HasPrefix(strings.TrimSpace(issue.Title), conflictRemediationTitlePrefix) {
		return nil, false, nil
	}

	if strings.TrimSpace(issue.ID) == "" {
		return nil, true, fmt.Errorf("conflict task is missing its issue ID")
	}
	if !beads.HasLabel(issue, "gt:task") {
		return nil, true, fmt.Errorf("conflict task %s is missing the gt:task label", issue.ID)
	}

	metadata := parseConflictTaskMetadata(issue.Description)
	info := &conflictRemediationTask{
		TaskID:          strings.TrimSpace(issue.ID),
		OriginalMRID:    metadata["Original MR"],
		Branch:          metadata["Branch"],
		OriginalIssueID: metadata["Original issue"],
	}
	if info.OriginalMRID == "" || info.Branch == "" || info.OriginalIssueID == "" {
		return nil, true, fmt.Errorf("conflict task %s must include Original MR, Branch, and Original issue metadata", issue.ID)
	}
	return info, true, nil
}

func parseConflictTaskMetadata(description string) map[string]string {
	metadata := make(map[string]string)
	inMetadata := false
	for _, line := range strings.Split(description, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			inMetadata = trimmed == "## Metadata"
			continue
		}
		if !inMetadata {
			continue
		}
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
		key, value, ok := strings.Cut(trimmed, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			metadata[key] = value
		}
	}
	return metadata
}

func validateConflictRemediationMR(task *conflictRemediationTask, mr *beads.Issue, target string) (*beads.MRFields, error) {
	if task == nil {
		return nil, fmt.Errorf("conflict task metadata is missing")
	}
	if mr == nil {
		return nil, fmt.Errorf("original merge request %s is missing", task.OriginalMRID)
	}
	if strings.TrimSpace(mr.ID) != task.OriginalMRID {
		return nil, fmt.Errorf("conflict task %s references MR %s, got %s", task.TaskID, task.OriginalMRID, mr.ID)
	}
	if !beads.HasLabel(mr, "gt:merge-request") {
		return nil, fmt.Errorf("original merge request %s is not labeled gt:merge-request", mr.ID)
	}
	if beads.IssueStatus(mr.Status).IsTerminal() {
		return nil, fmt.Errorf("original merge request %s is already terminal (%s)", mr.ID, mr.Status)
	}

	fields := beads.ParseMRFields(mr)
	if fields == nil {
		return nil, fmt.Errorf("original merge request %s has no merge request metadata", mr.ID)
	}
	if strings.TrimSpace(fields.SourceIssue) != task.OriginalIssueID {
		return nil, fmt.Errorf("original merge request %s source_issue %s does not match conflict task source %s", mr.ID, fields.SourceIssue, task.OriginalIssueID)
	}
	if strings.TrimSpace(fields.Branch) != task.Branch {
		return nil, fmt.Errorf("original merge request %s branch %s does not match conflict task branch %s", mr.ID, fields.Branch, task.Branch)
	}
	if strings.TrimSpace(fields.Target) == "" || strings.TrimSpace(fields.Target) != strings.TrimSpace(target) {
		return nil, fmt.Errorf("original merge request %s target %s does not match submission target %s", mr.ID, fields.Target, target)
	}
	if fields.ConflictTaskID != "" && fields.ConflictTaskID != task.TaskID {
		return nil, fmt.Errorf("original merge request %s points to conflict task %s, not %s", mr.ID, fields.ConflictTaskID, task.TaskID)
	}
	if fields.ConflictTaskID == "" {
		linked := false
		for _, dependency := range mr.Dependencies {
			if strings.TrimSpace(dependency.ID) == task.TaskID {
				linked = true
				break
			}
		}
		if !linked {
			return nil, fmt.Errorf("original merge request %s does not reference conflict task %s", mr.ID, task.TaskID)
		}
	}
	return fields, nil
}

func conflictRemediationMRDescription(mr *beads.Issue, fields *beads.MRFields, taskID, commitSHA string, preVerified bool, preVerifiedAt, preVerifiedBase string) (string, error) {
	if mr == nil || fields == nil {
		return "", fmt.Errorf("original merge request metadata is missing")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", fmt.Errorf("conflict remediation task ID is missing")
	}
	commitSHA = strings.TrimSpace(commitSHA)
	if commitSHA == "" {
		return "", fmt.Errorf("conflict remediation commit SHA is missing")
	}

	updated := *fields
	if updated.CommitSHA != commitSHA {
		// Verification on the previous head does not apply after the conflict
		// merge changes the submitted commit.
		updated.PreVerified = false
		updated.PreVerifiedAt = ""
		updated.PreVerifiedBase = ""
	}
	updated.CommitSHA = commitSHA
	updated.ConflictTaskID = taskID
	if preVerified {
		updated.PreVerified = true
		updated.PreVerifiedAt = strings.TrimSpace(preVerifiedAt)
		updated.PreVerifiedBase = strings.TrimSpace(preVerifiedBase)
	}
	return beads.SetMRFields(mr, &updated), nil
}

func closeConflictRemediationTask(bd *beads.Beads, taskID, mrID, commitSHA string) error {
	task, err := bd.Show(taskID)
	if err != nil || task == nil {
		if err == nil {
			err = fmt.Errorf("conflict task %s is missing", taskID)
		}
		return fmt.Errorf("reading conflict task %s before close: %w", taskID, err)
	}
	if beads.IssueStatus(task.Status).IsTerminal() && task.Status != string(beads.StatusClosed) {
		return fmt.Errorf("conflict task %s is terminal with status %s", taskID, task.Status)
	}
	if task.Status != string(beads.StatusClosed) {
		if skipReason, _ := doneSourceCloseSkipReason(bd, taskID, task); skipReason != "" {
			return fmt.Errorf("cannot close conflict task %s: %s", taskID, skipReason)
		}
	}

	if attachment := beads.ParseAttachmentFields(task); attachment != nil && attachment.AttachedMolecule != "" {
		molecule, showErr := bd.Show(attachment.AttachedMolecule)
		if showErr != nil && !errors.Is(showErr, beads.ErrNotFound) {
			return fmt.Errorf("reading attached molecule %s: %w", attachment.AttachedMolecule, showErr)
		}
		if molecule != nil && !beads.IssueStatus(molecule.Status).IsTerminal() {
			if n := closeDescendants(bd, attachment.AttachedMolecule); n > 0 {
				fmt.Fprintf(os.Stderr, "Closed %d molecule step(s) for %s\n", n, attachment.AttachedMolecule)
			}
			if closeErr := bd.ForceCloseWithReason("done", attachment.AttachedMolecule); closeErr != nil && !errors.Is(closeErr, beads.ErrNotFound) {
				return fmt.Errorf("closing attached molecule %s: %w", attachment.AttachedMolecule, closeErr)
			}
		}
	}
	if task.Status == string(beads.StatusClosed) {
		return nil
	}

	reason := fmt.Sprintf("conflict remediation submitted to original MR %s at %s", mrID, commitSHA)
	if err := bd.CloseWithReason(reason, taskID); err != nil {
		return fmt.Errorf("closing conflict task %s: %w", taskID, err)
	}
	return nil
}
