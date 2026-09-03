package cmd

import (
	"strings"
	"sync"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/workspace"
)

// issueHasOpenMRFn is the seam tests replace. It answers "does this work bead
// already have a merge request sitting in the queue?".
var issueHasOpenMRFn = issueHasOpenMR

var (
	openMROnce    sync.Once
	openMRSources map[string]bool
)

// issueHasOpenMR reports whether an open merge-request bead names issueID as its
// source. The answer is computed once per process: gt convoy stranded is a
// one-shot CLI invocation, and re-listing the queue per tracked issue would turn
// a single scan into O(convoy size) queue queries.
func issueHasOpenMR(issueID string) bool {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return false
	}
	openMROnce.Do(func() {
		openMRSources = map[string]bool{}
		townRoot, err := workspace.FindFromCwd()
		if err != nil {
			return
		}
		mrs, err := beads.New(townRoot).ListMergeRequests(beads.ListOptions{
			Status:   "open",
			Label:    "gt:merge-request",
			Priority: -1,
		})
		if err != nil {
			return
		}
		for _, mr := range mrs {
			if fields := beads.ParseMRFields(mr); fields != nil {
				if src := strings.TrimSpace(fields.SourceIssue); src != "" {
					openMRSources[src] = true
				}
			}
		}
	})
	return openMRSources[issueID]
}

// resetOpenMRCache exists for tests.
func resetOpenMRCache() {
	openMROnce = sync.Once{}
	openMRSources = nil
}

// awaitingMergeBlocksDispatch reports whether a tracked issue must be withheld
// from the convoy feeder because its work is already in the merge queue.
//
// Without this, workflow.close_on_merge would hand the feeder a gift: a source
// bead held as in_progress with no assignee reads as "orphaned molecule, worker
// dead" to isReadyIssue, and `gt convoy stranded` would re-sling a bead whose MR
// is sitting in front of the Refinery.
func awaitingMergeBlocksDispatch(t trackedIssueInfo) bool {
	if !config.CloseOnMergeEnabledForTown(townRootForAwaitingMerge()) {
		return false
	}
	if beads.LabelsContainAwaitingMerge(t.Labels) {
		return true
	}
	// Beads submitted before the label existed, or whose hold update failed,
	// are still recoverable from the queue itself.
	switch strings.TrimSpace(t.Status) {
	case string(beads.StatusInProgress), string(beads.IssueStatusHooked):
		return issueHasOpenMRFn(t.ID)
	}
	return false
}

func townRootForAwaitingMerge() string {
	townRoot, err := workspace.FindFromCwd()
	if err != nil {
		return ""
	}
	return townRoot
}
