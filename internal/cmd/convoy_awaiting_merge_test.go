package cmd

import (
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

func stubOpenMR(t *testing.T, ids ...string) {
	t.Helper()
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	old := issueHasOpenMRFn
	issueHasOpenMRFn = func(id string) bool { return set[id] }
	t.Cleanup(func() { issueHasOpenMRFn = old; resetOpenMRCache() })
}

// hq-19pxc: bt-j6uu.12 is held in_progress with no assignee while
// bt-wisp-tgbaa sits in the queue. The convoy feeder must not re-sling it.
func TestIsReadyIssueSkipsAwaitingMergeLabel(t *testing.T) {
	stubOpenMR(t)
	held := trackedIssueInfo{
		ID:     "bt-j6uu.12",
		Status: "in_progress",
		Labels: []string{"gastown", beads.AwaitingMergeLabel("bt-wisp-tgbaa")},
	}
	if isReadyIssue(held, nil) {
		t.Fatal("a bead held for a pending MR must not be reported as stranded/ready")
	}
}

func TestIsReadyIssueSkipsInProgressWithOpenMR(t *testing.T) {
	// No label (submitted by an older gt, or the hold update failed) — the
	// queue itself is still authoritative.
	stubOpenMR(t, "bt-j6uu.12")
	held := trackedIssueInfo{ID: "bt-j6uu.12", Status: "in_progress"}
	if isReadyIssue(held, nil) {
		t.Fatal("in_progress bead with an open MR must not be re-slung")
	}
}

func TestIsReadyIssueStillRecoversInProgressWithoutMR(t *testing.T) {
	stubOpenMR(t)
	orphan := trackedIssueInfo{ID: "bt-j6uu.12", Status: "in_progress"}
	if !isReadyIssue(orphan, nil) {
		t.Fatal("an in_progress bead with no MR is still an orphaned molecule and stays recoverable")
	}
}

func TestIsReadyIssueOpenUnassignedUnaffected(t *testing.T) {
	stubOpenMR(t, "bt-j6uu.12")
	// An open, unassigned bead is trivially ready. The awaiting-merge guard
	// only looks at in_progress/hooked, so an open bead is unaffected even if
	// a stale MR still names it.
	if !isReadyIssue(trackedIssueInfo{ID: "bt-j6uu.12", Status: "open"}, nil) {
		t.Fatal("open unassigned bead must stay ready")
	}
}

func TestAwaitingMergeBlocksDispatchIgnoresClosed(t *testing.T) {
	stubOpenMR(t, "bt-j6uu.12")
	if awaitingMergeBlocksDispatch(trackedIssueInfo{ID: "bt-j6uu.12", Status: "closed"}) {
		t.Fatal("closed beads are filtered earlier; the guard must not claim them")
	}
}
