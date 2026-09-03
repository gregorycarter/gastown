package refinery

import (
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

// labelAwareWorkBeadStore is a fakeWorkBeadStore that can also edit labels,
// i.e. the shape *beads.Beads has in production.
type labelAwareWorkBeadStore struct {
	*fakeWorkBeadStore
	updateErr   error
	updateCalls []beads.UpdateOptions
}

func newLabelAwareWorkBeadStore() *labelAwareWorkBeadStore {
	return &labelAwareWorkBeadStore{fakeWorkBeadStore: newFakeWorkBeadStore()}
}

func (f *labelAwareWorkBeadStore) Update(id string, opts beads.UpdateOptions) error {
	f.updateCalls = append(f.updateCalls, opts)
	if f.updateErr != nil {
		return f.updateErr
	}
	issue, ok := f.issues[id]
	if !ok {
		return beads.ErrNotFound
	}
	var kept []string
	for _, l := range issue.Labels {
		drop := false
		for _, r := range opts.RemoveLabels {
			if l == r {
				drop = true
			}
		}
		if !drop {
			kept = append(kept, l)
		}
	}
	issue.Labels = append(kept, opts.AddLabels...)
	return nil
}

// Topology from hq-19pxc/hq-tx4md: bt-j6uu.12 held for MR bt-wisp-xq1yy on
// branch polecat/warboy/bt-j6uu.12+mtip9i05, landing as e1e30e2.
func heldSourceIssue() *beads.Issue {
	issue := workIssue("bt-j6uu.12", string(beads.StatusInProgress))
	issue.Labels = []string{"gastown", beads.AwaitingMergeLabel("bt-wisp-xq1yy")}
	return issue
}

func TestPostMergeClosesHeldSourceAndClearsAwaitingMergeLabel(t *testing.T) {
	work := newLabelAwareWorkBeadStore()
	work.add(heldSourceIssue())

	result := closeMergedWorkBead(work, nil, nil, mergedWorkBeadCloseRequest{
		MRID:        "bt-wisp-xq1yy",
		Branch:      "polecat/warboy/bt-j6uu.12+mtip9i05",
		Target:      "main",
		SourceIssue: "bt-j6uu.12",
		MergeCommit: "e1e30e2",
	})

	if !result.Closed {
		t.Fatalf("held in_progress source was not closed on merge receipt: %+v", result)
	}
	if got := work.issues["bt-j6uu.12"].Status; got != string(beads.StatusClosed) {
		t.Fatalf("source status = %q, want closed", got)
	}
	if beads.IsAwaitingMerge(work.issues["bt-j6uu.12"]) {
		t.Fatalf("awaiting-merge label survived the merge: %v", work.issues["bt-j6uu.12"].Labels)
	}
	if len(work.updateCalls) != 1 {
		t.Fatalf("expected exactly one label update, got %d", len(work.updateCalls))
	}
	if !strings.HasPrefix(work.lastCloseReason, "Merged in bt-wisp-xq1yy") {
		t.Fatalf("close reason must keep the 'Merged in ' prefix (convoy merge-blocks contract): %q", work.lastCloseReason)
	}
	if !strings.Contains(work.lastCloseReason, "merged e1e30e2 via bt-wisp-xq1yy") {
		t.Fatalf("close reason missing landed sha attribution: %q", work.lastCloseReason)
	}
}

func TestPostMergeLabelClearFailureStillCloses(t *testing.T) {
	work := newLabelAwareWorkBeadStore()
	work.add(heldSourceIssue())
	work.updateErr = beads.ErrNotFound

	result := closeMergedWorkBead(work, nil, nil, mergedWorkBeadCloseRequest{
		MRID:        "bt-wisp-xq1yy",
		SourceIssue: "bt-j6uu.12",
		MergeCommit: "e1e30e2",
	})
	if !result.Closed {
		t.Fatal("a failed label clear must not block the close: the merge already happened")
	}
}

func TestPostMergeSkipsLabelUpdateWhenNoHold(t *testing.T) {
	work := newLabelAwareWorkBeadStore()
	work.add(workIssue("bt-j6uu.12", string(beads.StatusOpen)))

	closeMergedWorkBead(work, nil, nil, mergedWorkBeadCloseRequest{
		MRID:        "bt-wisp-xq1yy",
		SourceIssue: "bt-j6uu.12",
		MergeCommit: "e1e30e2",
	})
	if len(work.updateCalls) != 0 {
		t.Fatalf("no awaiting-merge label present; expected no label update, got %v", work.updateCalls)
	}
}

// Flag-off parity: without a hold the close reason is byte-identical to
// upstream apart from the added landed-sha line, which only appears when a
// merge commit is known.
func TestPostMergeCloseReasonWithoutMergeCommitUnchanged(t *testing.T) {
	work := newFakeWorkBeadStore()
	work.add(workIssue("bt-j6uu.12", string(beads.StatusOpen)))

	closeMergedWorkBead(work, nil, nil, mergedWorkBeadCloseRequest{
		MRID:        "bt-wisp-xq1yy",
		SourceIssue: "bt-j6uu.12",
	})
	if work.lastCloseReason != "Merged in bt-wisp-xq1yy" {
		t.Fatalf("close reason = %q, want the upstream single-line form", work.lastCloseReason)
	}
}
