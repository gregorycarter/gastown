package refinery

import (
	"errors"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

type fakeRejectionBeads struct {
	issues     map[string]*beads.Issue
	updates    map[string]beads.UpdateOptions
	comments   map[string][]string
	updateErr  error
	commentErr error
	showErr    error
}

func newFakeRejectionBeads() *fakeRejectionBeads {
	return &fakeRejectionBeads{
		issues:   map[string]*beads.Issue{},
		updates:  map[string]beads.UpdateOptions{},
		comments: map[string][]string{},
	}
}

func (f *fakeRejectionBeads) Show(id string) (*beads.Issue, error) {
	if f.showErr != nil {
		return nil, f.showErr
	}
	issue, ok := f.issues[id]
	if !ok {
		return nil, beads.ErrNotFound
	}
	return issue, nil
}

func (f *fakeRejectionBeads) Update(id string, opts beads.UpdateOptions) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updates[id] = opts
	return nil
}

func (f *fakeRejectionBeads) AddComment(id, comment string) error {
	if f.commentErr != nil {
		return f.commentErr
	}
	f.comments[id] = append(f.comments[id], comment)
	return nil
}

// The rejected candidate from hq-19pxc / hq-tx4md.
func rejectedMR() *MergeRequest {
	return &MergeRequest{
		ID:        "bt-wisp-tgbaa",
		IssueID:   "bt-j6uu.12",
		Branch:    "polecat/warboy/bt-j6uu.12+mtip9i05",
		CommitSHA: "9a2bf2d",
		Worker:    "polecats/warboy",
	}
}

const gateReason = "native:chatgpt_dashboard_share_indirect expected manage_dashboard_share first, got list_dashboards first"

func TestApplyRejectionReopensClosedSource(t *testing.T) {
	// The exact hq-19pxc shape: the source was closed four seconds after the MR
	// bead was created, so the rejection had nothing to act on.
	bd := newFakeRejectionBeads()
	bd.issues["bt-j6uu.12"] = &beads.Issue{
		ID:     "bt-j6uu.12",
		Title:  "MCP dashboard share indirect",
		Type:   "bug",
		Status: string(beads.StatusClosed),
	}

	outcome := applyRejectionToSource(bd, rejectedMR(), gateReason)

	if !outcome.Recorded || !outcome.Reopened {
		t.Fatalf("outcome = %+v, want recorded+reopened", outcome)
	}
	opts := bd.updates["bt-j6uu.12"]
	if opts.Status == nil || *opts.Status != string(beads.StatusOpen) {
		t.Fatalf("status = %v, want open", opts.Status)
	}
	if len(opts.AddLabels) != 1 || opts.AddLabels[0] != beads.LabelMRRejected {
		t.Fatalf("AddLabels = %v, want [%s]", opts.AddLabels, beads.LabelMRRejected)
	}
	fields := beads.ParseRejectionFields(&beads.Issue{Description: *opts.Description})
	if fields == nil || fields.RejectedSHA != "9a2bf2d" || fields.RejectedBranch != "polecat/warboy/bt-j6uu.12+mtip9i05" {
		t.Fatalf("rejection fields = %+v", fields)
	}
	if !strings.Contains(fields.RejectedReason, "chatgpt_dashboard_share_indirect") {
		t.Fatalf("reason not recorded: %q", fields.RejectedReason)
	}
	// The polecat work formula keys on this note plus a branch.
	comments := bd.comments["bt-j6uu.12"]
	if len(comments) != 1 || !strings.HasPrefix(comments[0], "MERGE REJECTION: ") ||
		!strings.Contains(comments[0], "branch=polecat/warboy/bt-j6uu.12+mtip9i05") ||
		!strings.Contains(comments[0], "sha=9a2bf2d") {
		t.Fatalf("comment = %v", comments)
	}
}

func TestApplyRejectionReleasesAwaitingMergeHold(t *testing.T) {
	bd := newFakeRejectionBeads()
	bd.issues["bt-j6uu.12"] = &beads.Issue{
		ID:     "bt-j6uu.12",
		Type:   "bug",
		Status: string(beads.StatusInProgress),
		Labels: []string{"gastown", beads.AwaitingMergeLabel("bt-wisp-tgbaa")},
	}

	outcome := applyRejectionToSource(bd, rejectedMR(), gateReason)

	if !outcome.HoldCleared || !outcome.Reopened {
		t.Fatalf("outcome = %+v, want the hold released and the bead reopened", outcome)
	}
	opts := bd.updates["bt-j6uu.12"]
	if opts.Status == nil || *opts.Status != string(beads.StatusOpen) {
		t.Fatalf("status = %v, want open", opts.Status)
	}
	if len(opts.RemoveLabels) != 1 || opts.RemoveLabels[0] != "awaiting-merge:bt-wisp-tgbaa" {
		t.Fatalf("RemoveLabels = %v", opts.RemoveLabels)
	}
}

func TestApplyRejectionLeavesOpenSourceStatusAlone(t *testing.T) {
	bd := newFakeRejectionBeads()
	bd.issues["bt-j6uu.12"] = &beads.Issue{ID: "bt-j6uu.12", Type: "bug", Status: string(beads.StatusOpen)}

	outcome := applyRejectionToSource(bd, rejectedMR(), gateReason)

	if !outcome.Recorded || outcome.Reopened {
		t.Fatalf("outcome = %+v; an already-open source needs no status change", outcome)
	}
	if bd.updates["bt-j6uu.12"].Status != nil {
		t.Fatal("status must not be rewritten when the bead is already dispatchable")
	}
}

func TestApplyRejectionSecondRejectionReplacesFields(t *testing.T) {
	// hq-tx4md: bt-j6uu.12 was rejected twice. The second record must replace
	// the first, not accumulate.
	bd := newFakeRejectionBeads()
	bd.issues["bt-j6uu.12"] = &beads.Issue{
		ID:          "bt-j6uu.12",
		Type:        "bug",
		Status:      string(beads.StatusOpen),
		Description: "context line\nrejected_sha: 0000000\nrejected_branch: old\nrejected_reason: first\n",
	}

	applyRejectionToSource(bd, rejectedMR(), gateReason)

	desc := *bd.updates["bt-j6uu.12"].Description
	if strings.Count(desc, "rejected_sha:") != 1 {
		t.Fatalf("duplicate rejection records:\n%s", desc)
	}
	if !strings.Contains(desc, "rejected_sha: 9a2bf2d") || strings.Contains(desc, "0000000") {
		t.Fatalf("stale rejection survived:\n%s", desc)
	}
	if !strings.Contains(desc, "context line") {
		t.Fatalf("unrelated description content was dropped:\n%s", desc)
	}
}

func TestApplyRejectionUpdateFailureIsReported(t *testing.T) {
	bd := newFakeRejectionBeads()
	bd.issues["bt-j6uu.12"] = &beads.Issue{ID: "bt-j6uu.12", Type: "bug", Status: string(beads.StatusClosed)}
	bd.updateErr = errors.New("dolt write failed")

	outcome := applyRejectionToSource(bd, rejectedMR(), gateReason)
	if outcome.Recorded || outcome.Reopened {
		t.Fatalf("a failed write must not be reported as a successful reopen: %+v", outcome)
	}
	if len(outcome.Notes) == 0 {
		t.Fatal("failure must be noted for the log")
	}
}

func TestApplyRejectionSkipsMissingAndNonConcreteSources(t *testing.T) {
	bd := newFakeRejectionBeads()
	if got := applyRejectionToSource(bd, rejectedMR(), gateReason); got.Recorded {
		t.Fatal("missing source must not be recorded")
	}
	bd.issues["bt-j6uu.12"] = &beads.Issue{
		ID:     "bt-j6uu.12",
		Type:   "bug",
		Status: string(beads.StatusOpen),
		Labels: []string{"gt:rig"},
	}
	if got := applyRejectionToSource(bd, rejectedMR(), gateReason); got.Recorded {
		t.Fatal("a rig identity bead is not a concrete work bead and must never be reopened as work")
	}

	noSource := rejectedMR()
	noSource.IssueID = ""
	if got := applyRejectionToSource(bd, noSource, gateReason); got.Recorded {
		t.Fatal("no source issue means nothing to record")
	}
}

func TestMergeRejectionCommentIsSingleLine(t *testing.T) {
	mr := rejectedMR()
	got := mergeRejectionComment(mr, "line one\nline two")
	if strings.Contains(got, "\n") {
		t.Fatalf("comment must stay single-line for formula matching: %q", got)
	}
}

func TestWorkerSessionName(t *testing.T) {
	tests := []struct{ rig, worker, want string }{
		{"bridge_town_core", "polecats/warboy", "bt-warboy"},
		{"bridge_town_core", "warboy", "bt-warboy"},
		{"bridge_town_core", "bridge_town_core/polecats/warboy", "bt-warboy"},
		{"bridge_town_core", "", ""},
		{"", "warboy", ""},
	}
	for _, tt := range tests {
		got := workerSessionName(tt.rig, tt.worker)
		if tt.want == "" {
			if got != "" {
				t.Fatalf("workerSessionName(%q,%q) = %q, want empty", tt.rig, tt.worker, got)
			}
			continue
		}
		// The rig prefix comes from the town registry, which is absent in tests;
		// only the polecat half is stable here.
		if !strings.HasSuffix(got, "-warboy") {
			t.Fatalf("workerSessionName(%q,%q) = %q, want a *-warboy session", tt.rig, tt.worker, got)
		}
	}
}
