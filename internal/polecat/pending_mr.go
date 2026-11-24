package polecat

import (
	"fmt"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
)

// PendingMRFinder is the merge-queue lookup PendingMRBlocker needs.
type PendingMRFinder interface {
	FindMRForBranch(branch string) (*beads.Issue, error)
	FindPendingMRsForIssue(issueID string) ([]*beads.Issue, error)
}

// PendingMRBlocker returns a non-empty reason when an OPEN, IN_PROGRESS or
// BLOCKED merge request still belongs to this polecat, found by its branch
// or by its source bead, independent of the agent bead's active_mr pointer.
//
// kasb-84hoc: check-recovery reported SAFE_TO_NUKE for raider while its MR
// was BLOCKED needs-repair, because only active_mr was consulted. A blocked
// MR (needs-repair, needs-rebase, a hold) goes back to the same worker, so
// its sandbox is still needed. MRs found by source bead count only when
// their branch belongs to this polecat: a re-dispatch to another worker does
// not pin this sandbox. Lookup errors fail closed.
func PendingMRBlocker(f PendingMRFinder, polecatName, branch, sourceIssue string) string {
	if f == nil {
		return "pending_mr=unverified"
	}
	if branch = strings.TrimSpace(branch); branch != "" {
		mr, err := f.FindMRForBranch(branch)
		if err != nil {
			return fmt.Sprintf("pending_mr=unknown branch=%s lookup_error: %v", branch, err)
		}
		if mr != nil && !beads.IssueStatus(mr.Status).IsTerminal() {
			return fmt.Sprintf("pending_mr=%s status=%s branch=%s", mr.ID, mr.Status, branch)
		}
	}
	if sourceIssue = strings.TrimSpace(sourceIssue); sourceIssue != "" {
		mrs, err := f.FindPendingMRsForIssue(sourceIssue)
		if err != nil {
			return fmt.Sprintf("pending_mr=unknown source_issue=%s lookup_error: %v", sourceIssue, err)
		}
		for _, mr := range mrs {
			if mr == nil || beads.IssueStatus(mr.Status).IsTerminal() {
				continue
			}
			if !mrBelongsToPolecat(mr, polecatName, branch) {
				continue
			}
			return fmt.Sprintf("pending_mr=%s status=%s source_issue=%s", mr.ID, mr.Status, sourceIssue)
		}
	}
	return ""
}

func mrBelongsToPolecat(mr *beads.Issue, polecatName, branch string) bool {
	fields := beads.ParseMRFields(mr)
	if fields == nil || strings.TrimSpace(fields.Branch) == "" {
		// No branch to attribute it: it may be ours, so keep the sandbox.
		return true
	}
	mrBranch := strings.TrimSpace(fields.Branch)
	if branch != "" && mrBranch == branch {
		return true
	}
	if meta, ok := ParseBranchName(mrBranch); ok {
		return meta.Polecat == polecatName
	}
	return false
}
