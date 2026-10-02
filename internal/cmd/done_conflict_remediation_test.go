package cmd

import (
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

func TestParseConflictRemediationTask(t *testing.T) {
	task := &beads.Issue{
		ID:     "gt-conflict",
		Title:  "Resolve merge conflicts: original work",
		Labels: []string{"gt:task"},
		Description: `Resolve merge conflicts for branch polecat/original/gt-work

## Metadata
- Original MR: gt-mr1
- Branch: polecat/original/gt-work
- Conflict with: bridge-town-next@abc123
- Original issue: gt-work
- Retry count: 1

## Instructions
Close the task after pushing.`,
	}

	got, recognized, err := parseConflictRemediationTask(task)
	if err != nil {
		t.Fatalf("parseConflictRemediationTask returned error: %v", err)
	}
	if !recognized {
		t.Fatal("conflict task was not recognized")
	}
	if got.TaskID != "gt-conflict" || got.OriginalMRID != "gt-mr1" || got.Branch != "polecat/original/gt-work" || got.OriginalIssueID != "gt-work" {
		t.Fatalf("parsed conflict task = %#v", got)
	}
}

func TestParseConflictRemediationTaskFailsClosedOnMissingMetadata(t *testing.T) {
	task := &beads.Issue{
		ID:          "gt-conflict",
		Title:       "Resolve merge conflicts: original work",
		Labels:      []string{"gt:task"},
		Description: "## Metadata\n- Branch: polecat/original/gt-work\n",
	}

	got, recognized, err := parseConflictRemediationTask(task)
	if !recognized || err == nil || got != nil {
		t.Fatalf("parse result = (%#v, %v, %v), want recognized malformed task", got, recognized, err)
	}
}

func TestParseConflictRemediationTaskIgnoresOrdinaryIssue(t *testing.T) {
	task := &beads.Issue{ID: "gt-work", Title: "Fix ordinary issue", Description: "Original MR: gt-mr1"}
	got, recognized, err := parseConflictRemediationTask(task)
	if err != nil || recognized || got != nil {
		t.Fatalf("parse result = (%#v, %v, %v), want ordinary issue", got, recognized, err)
	}
}

func TestValidateConflictRemediationMR(t *testing.T) {
	task := &conflictRemediationTask{
		TaskID:          "gt-conflict",
		OriginalMRID:    "gt-mr1",
		Branch:          "polecat/original/gt-work",
		OriginalIssueID: "gt-work",
	}
	validMR := &beads.Issue{
		ID:     "gt-mr1",
		Status: "open",
		Labels: []string{"gt:merge-request"},
		Description: "branch: polecat/original/gt-work\n" +
			"target: bridge-town-next\nsource_issue: gt-work\nconflict_task_id: gt-conflict\n",
	}
	dependencyLinkedMR := &beads.Issue{
		ID:           "gt-mr1",
		Status:       "open",
		Labels:       []string{"gt:merge-request"},
		Description:  "branch: polecat/original/gt-work\ntarget: bridge-town-next\nsource_issue: gt-work\n",
		Dependencies: []beads.IssueDep{{ID: "gt-conflict"}},
	}

	fields, err := validateConflictRemediationMR(task, validMR, "bridge-town-next")
	if err != nil {
		t.Fatalf("valid original MR rejected: %v", err)
	}
	if fields.Branch != task.Branch || fields.SourceIssue != task.OriginalIssueID {
		t.Fatalf("validated fields = %#v", fields)
	}
	if _, err := validateConflictRemediationMR(task, dependencyLinkedMR, "bridge-town-next"); err != nil {
		t.Fatalf("MR dependency link rejected: %v", err)
	}

	tests := []struct {
		name   string
		mr     *beads.Issue
		target string
		want   string
	}{
		{name: "wrong source", mr: &beads.Issue{ID: "gt-mr1", Status: "open", Labels: []string{"gt:merge-request"}, Description: "branch: polecat/original/gt-work\ntarget: bridge-town-next\nsource_issue: gt-other\n"}, target: "bridge-town-next", want: "source_issue"},
		{name: "wrong branch", mr: &beads.Issue{ID: "gt-mr1", Status: "open", Labels: []string{"gt:merge-request"}, Description: "branch: polecat/other/gt-work\ntarget: bridge-town-next\nsource_issue: gt-work\n"}, target: "bridge-town-next", want: "branch"},
		{name: "wrong target", mr: validMR, target: "main", want: "target"},
		{name: "different conflict task", mr: &beads.Issue{ID: "gt-mr1", Status: "open", Labels: []string{"gt:merge-request"}, Description: "branch: polecat/original/gt-work\ntarget: bridge-town-next\nsource_issue: gt-work\nconflict_task_id: gt-other\n"}, target: "bridge-town-next", want: "conflict task"},
		{name: "missing conflict task link", mr: &beads.Issue{ID: "gt-mr1", Status: "open", Labels: []string{"gt:merge-request"}, Description: dependencyLinkedMR.Description}, target: "bridge-town-next", want: "does not reference"},
		{name: "closed original", mr: &beads.Issue{ID: "gt-mr1", Status: "closed", Labels: []string{"gt:merge-request"}, Description: validMR.Description}, target: "bridge-town-next", want: "terminal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := validateConflictRemediationMR(task, tt.mr, tt.target); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validation error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestConflictRemediationMRDescriptionUpdatesSHAAndVerification(t *testing.T) {
	mr := &beads.Issue{
		ID:          "gt-mr1",
		Description: "Merge: gt-work\nbranch: polecat/original/gt-work\ntarget: bridge-town-next\nsource_issue: gt-work\nconflict_task_id: gt-conflict\ncommit_sha: old-sha\npre_verified: true\npre_verified_at: old-time\npre_verified_base: old-base\n",
	}
	fields := beads.ParseMRFields(mr)

	updatedDescription, err := conflictRemediationMRDescription(mr, fields, "gt-conflict", "new-sha", false, "", "")
	if err != nil {
		t.Fatalf("conflictRemediationMRDescription: %v", err)
	}
	updatedMR := &beads.Issue{ID: mr.ID, Description: updatedDescription}
	updated := beads.ParseMRFields(updatedMR)
	if updated.CommitSHA != "new-sha" {
		t.Errorf("commit SHA = %q, want new-sha", updated.CommitSHA)
	}
	if updated.PreVerified || updated.PreVerifiedAt != "" || updated.PreVerifiedBase != "" {
		t.Errorf("stale verification metadata remains: %#v", updated)
	}
	if updated.ConflictTaskID != "gt-conflict" || updated.SourceIssue != "gt-work" || updated.Branch != "polecat/original/gt-work" {
		t.Errorf("original MR identity metadata changed: %#v", updated)
	}

	verifiedDescription, err := conflictRemediationMRDescription(mr, fields, "gt-conflict", "new-sha", true, "now", "base-sha")
	if err != nil {
		t.Fatalf("pre-verified conflictRemediationMRDescription: %v", err)
	}
	verified := beads.ParseMRFields(&beads.Issue{ID: mr.ID, Description: verifiedDescription})
	if !verified.PreVerified || verified.PreVerifiedAt != "now" || verified.PreVerifiedBase != "base-sha" {
		t.Errorf("new verification metadata = %#v", verified)
	}
}
