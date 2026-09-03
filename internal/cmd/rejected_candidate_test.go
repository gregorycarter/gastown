package cmd

import (
	"errors"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

type fakeRejectionMemory struct {
	issues    map[string]*beads.Issue
	updates   map[string]beads.UpdateOptions
	shows     int
	updateErr error
}

func newFakeRejectionMemory() *fakeRejectionMemory {
	return &fakeRejectionMemory{issues: map[string]*beads.Issue{}, updates: map[string]beads.UpdateOptions{}}
}

func (f *fakeRejectionMemory) Show(id string) (*beads.Issue, error) {
	f.shows++
	issue, ok := f.issues[id]
	if !ok {
		return nil, beads.ErrNotFound
	}
	return issue, nil
}

func (f *fakeRejectionMemory) Update(id string, opts beads.UpdateOptions) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updates[id] = opts
	return nil
}

// hq-tx4md: rejected 9a2bf2d, remediated as e1e30e2 on the same branch.
func rejectedSource() *beads.Issue {
	return &beads.Issue{
		ID:     "bt-j6uu.12",
		Type:   "bug",
		Status: string(beads.StatusOpen),
		Labels: []string{beads.LabelMRRejected},
		Description: "rejected_sha: 9a2bf2d\n" +
			"rejected_branch: polecat/warboy/bt-j6uu.12+mtip9i05\n" +
			"rejected_reason: gate failed\n",
	}
}

func TestGuardRefusesTheRejectedSHA(t *testing.T) {
	bd := newFakeRejectionMemory()
	bd.issues["bt-j6uu.12"] = rejectedSource()

	err := guardRejectedCandidate(bd, true, "bt-j6uu.12", "9a2bf2d")
	var rejected *ErrRejectedCandidate
	if !errors.As(err, &rejected) {
		t.Fatalf("err = %v, want ErrRejectedCandidate", err)
	}
	if !strings.Contains(err.Error(), "gate failed") {
		t.Fatalf("message should carry the reason: %v", err)
	}
	if len(bd.updates) != 0 {
		t.Fatal("a refused resubmission must not clear the memory")
	}
}

func TestGuardMatchesAbbreviatedSHA(t *testing.T) {
	bd := newFakeRejectionMemory()
	bd.issues["bt-j6uu.12"] = rejectedSource()

	// The record is 7 chars; git hands gt done the full 40.
	if err := guardRejectedCandidate(bd, true, "bt-j6uu.12", "9a2bf2d1c0ffee0000000000000000000000abcd"); err == nil {
		t.Fatal("an abbreviated record must still match the full SHA")
	}
}

func TestGuardClearsMemoryOnNewSHA(t *testing.T) {
	bd := newFakeRejectionMemory()
	bd.issues["bt-j6uu.12"] = rejectedSource()

	if err := guardRejectedCandidate(bd, true, "bt-j6uu.12", "e1e30e2"); err != nil {
		t.Fatalf("a new SHA is a new candidate, got %v", err)
	}
	opts, ok := bd.updates["bt-j6uu.12"]
	if !ok {
		t.Fatal("memory was not cleared")
	}
	if opts.Description == nil || beads.ParseRejectionFields(&beads.Issue{Description: *opts.Description}) != nil {
		t.Fatalf("rejection fields survived: %v", opts.Description)
	}
	if len(opts.RemoveLabels) != 1 || opts.RemoveLabels[0] != beads.LabelMRRejected {
		t.Fatalf("RemoveLabels = %v", opts.RemoveLabels)
	}
}

func TestGuardIsInertWhenDisabledOrUnknown(t *testing.T) {
	bd := newFakeRejectionMemory()
	bd.issues["bt-j6uu.12"] = rejectedSource()

	if err := guardRejectedCandidate(bd, false, "bt-j6uu.12", "9a2bf2d"); err != nil {
		t.Fatalf("flag off must be a no-op, got %v", err)
	}
	if err := guardRejectedCandidate(bd, true, "bt-missing", "9a2bf2d"); err != nil {
		t.Fatalf("an unreadable source must never block a submission, got %v", err)
	}
	if err := guardRejectedCandidate(bd, true, "bt-j6uu.12", ""); err != nil {
		t.Fatalf("no SHA means no comparison, got %v", err)
	}
	if err := guardRejectedCandidate(nil, true, "bt-j6uu.12", "9a2bf2d"); err != nil {
		t.Fatalf("no client means no comparison, got %v", err)
	}
}

func TestGuardSurvivesFailedClear(t *testing.T) {
	bd := newFakeRejectionMemory()
	bd.issues["bt-j6uu.12"] = rejectedSource()
	bd.updateErr = errors.New("dolt write failed")

	if err := guardRejectedCandidate(bd, true, "bt-j6uu.12", "e1e30e2"); err != nil {
		t.Fatalf("a failed clear must not block the submission, got %v", err)
	}
}

func TestSameCommit(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"9a2bf2d", "9a2bf2d1c0ffee", true},
		{"9A2BF2D", "9a2bf2d1c0ffee", true},
		{"9a2bf2d", "e1e30e2", false},
		{"", "9a2bf2d", false},
		{"9a2bf", "9a2bf2d", false}, // too short to be meaningful
		{"9a2bf2d", "9a2bf2d", true},
	}
	for _, tt := range tests {
		if got := sameCommit(tt.a, tt.b); got != tt.want {
			t.Fatalf("sameCommit(%q,%q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestRejectionMemoLabelsQueueEntry(t *testing.T) {
	bd := newFakeRejectionMemory()
	bd.issues["bt-j6uu.12"] = rejectedSource()
	memo := newRejectionMemo(bd, true)

	stale := &beads.MRFields{SourceIssue: "bt-j6uu.12", CommitSHA: "9a2bf2d"}
	fresh := &beads.MRFields{SourceIssue: "bt-j6uu.12", CommitSHA: "e1e30e2"}

	if !memo.isRejectedCandidate(stale) {
		t.Fatal("the rejected SHA must be marked")
	}
	if memo.isRejectedCandidate(fresh) {
		t.Fatal("a new SHA on the same branch is a NEW candidate (hq-tx4md)")
	}
	if memo.isRejectedCandidate(nil) || memo.isRejectedCandidate(&beads.MRFields{SourceIssue: "bt-j6uu.12"}) {
		t.Fatal("no SHA, no verdict")
	}
	if bd.shows != 1 {
		t.Fatalf("source bead read %d times, want 1 (memoized)", bd.shows)
	}
}

func TestRejectionMemoDisabled(t *testing.T) {
	bd := newFakeRejectionMemory()
	bd.issues["bt-j6uu.12"] = rejectedSource()
	memo := newRejectionMemo(bd, false)

	if memo.isRejectedCandidate(&beads.MRFields{SourceIssue: "bt-j6uu.12", CommitSHA: "9a2bf2d"}) {
		t.Fatal("flag off must not change gt mq list output")
	}
	if bd.shows != 0 {
		t.Fatal("flag off must not cost a lookup")
	}
}
