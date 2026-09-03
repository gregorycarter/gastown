package cmd

import (
	"errors"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

type fakeAwaitingMergeBeads struct {
	openMRs     []*beads.Issue
	findErr     error
	updates     map[string]beads.UpdateOptions
	updateErr   error
	updateCalls int
}

func (f *fakeAwaitingMergeBeads) FindOpenMRsForIssue(string) ([]*beads.Issue, error) {
	return f.openMRs, f.findErr
}

func (f *fakeAwaitingMergeBeads) Update(id string, opts beads.UpdateOptions) error {
	f.updateCalls++
	if f.updates == nil {
		f.updates = map[string]beads.UpdateOptions{}
	}
	f.updates[id] = opts
	return f.updateErr
}

// The hq-19pxc topology: MR bt-wisp-tgbaa created at 14:14:42Z for source
// bt-j6uu.12, which upstream closed at 14:14:46Z — four seconds later.
func hq19pxcSource() *beads.Issue {
	return &beads.Issue{
		ID:          "bt-j6uu.12",
		Title:       "MCP dashboard share indirect",
		Status:      string(beads.IssueStatusHooked),
		Assignee:    "bridge_town_core/polecats/warboy",
		Description: "attached_molecule: bt-wisp-mol1\nattached_formula: mol-polecat-work\n",
	}
}

func openMR(id string) *beads.Issue {
	return &beads.Issue{ID: id, Labels: []string{"gt:merge-request"}, Status: string(beads.StatusOpen)}
}

func TestDecideMRHold(t *testing.T) {
	tests := []struct {
		name         string
		closeOnMerge bool
		exitType     string
		beadID       string
		issue        *beads.Issue
		bd           *fakeAwaitingMergeBeads
		wantHold     bool
		wantMR       string
	}{
		{
			name:         "completed with open MR is held",
			closeOnMerge: true,
			exitType:     ExitCompleted,
			beadID:       "bt-j6uu.12",
			issue:        hq19pxcSource(),
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
			wantHold:     true,
			wantMR:       "bt-wisp-tgbaa",
		},
		{
			name:         "flag off keeps upstream close",
			closeOnMerge: false,
			exitType:     ExitCompleted,
			beadID:       "bt-j6uu.12",
			issue:        hq19pxcSource(),
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
		},
		{
			name:         "no MR means no merge is pending",
			closeOnMerge: true,
			exitType:     ExitCompleted,
			beadID:       "bt-j6uu.12",
			issue:        hq19pxcSource(),
			bd:           &fakeAwaitingMergeBeads{},
		},
		{
			name:         "deferred exit keeps upstream behaviour",
			closeOnMerge: true,
			exitType:     ExitDeferred,
			beadID:       "bt-j6uu.12",
			issue:        hq19pxcSource(),
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
		},
		{
			name:         "escalated exit keeps upstream behaviour",
			closeOnMerge: true,
			exitType:     ExitEscalated,
			beadID:       "bt-j6uu.12",
			issue:        hq19pxcSource(),
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
		},
		{
			name:         "no_merge bead closes as today",
			closeOnMerge: true,
			exitType:     ExitCompleted,
			beadID:       "bt-j6uu.12",
			issue:        &beads.Issue{ID: "bt-j6uu.12", Description: "no_merge: true\n"},
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
		},
		{
			name:         "review_only bead closes as today",
			closeOnMerge: true,
			exitType:     ExitCompleted,
			beadID:       "bt-j6uu.12",
			issue:        &beads.Issue{ID: "bt-j6uu.12", Description: "review_only: true\n"},
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
		},
		{
			name:         "local merge strategy closes as today",
			closeOnMerge: true,
			exitType:     ExitCompleted,
			beadID:       "bt-j6uu.12",
			issue:        &beads.Issue{ID: "bt-j6uu.12", Description: "merge_strategy: local\n"},
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
		},
		{
			name:         "workflow step bead always closes",
			closeOnMerge: true,
			exitType:     ExitCompleted,
			beadID:       "bt-wfs-step1",
			issue:        &beads.Issue{ID: "bt-wfs-step1"},
			bd:           &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa")}},
		},
		{
			name:         "queue lookup failure fails closed to upstream",
			closeOnMerge: true,
			exitType:     ExitCompleted,
			beadID:       "bt-j6uu.12",
			issue:        hq19pxcSource(),
			bd:           &fakeAwaitingMergeBeads{findErr: errors.New("dolt timeout")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideMRHold(tt.bd, tt.closeOnMerge, tt.exitType, tt.beadID, tt.issue)
			if got.Hold != tt.wantHold {
				t.Fatalf("Hold = %v (%s), want %v", got.Hold, got.Reason, tt.wantHold)
			}
			if got.MRID != tt.wantMR {
				t.Fatalf("MRID = %q, want %q", got.MRID, tt.wantMR)
			}
		})
	}
}

func TestDecideMRHoldPicksNewestOpenMR(t *testing.T) {
	bd := &fakeAwaitingMergeBeads{openMRs: []*beads.Issue{openMR("bt-wisp-tgbaa"), openMR("bt-wisp-xq1yy")}}
	got := decideMRHold(bd, true, ExitCompleted, "bt-j6uu.12", hq19pxcSource())
	if !got.Hold || got.MRID != "bt-wisp-xq1yy" {
		t.Fatalf("got %+v, want hold on the newest MR bt-wisp-xq1yy", got)
	}
}

func TestHoldSourceForMergeParksBead(t *testing.T) {
	bd := &fakeAwaitingMergeBeads{}
	issue := hq19pxcSource()
	issue.Labels = []string{"gastown", beads.AwaitingMergeLabel("bt-wisp-mhr53")}

	if err := holdSourceForMerge(bd, "bt-j6uu.12", issue, "bt-wisp-tgbaa"); err != nil {
		t.Fatal(err)
	}
	opts := bd.updates["bt-j6uu.12"]
	if opts.Status == nil || *opts.Status != string(beads.StatusInProgress) {
		t.Fatalf("status = %v, want in_progress", opts.Status)
	}
	if opts.Assignee == nil || *opts.Assignee != "" {
		t.Fatalf("assignee = %v, want cleared", opts.Assignee)
	}
	if len(opts.AddLabels) != 1 || opts.AddLabels[0] != "awaiting-merge:bt-wisp-tgbaa" {
		t.Fatalf("AddLabels = %v", opts.AddLabels)
	}
	// hq-19pxc's bead had a prior rejected MR (bt-wisp-mhr53); its hold label
	// must not linger next to the new one.
	if len(opts.RemoveLabels) != 1 || opts.RemoveLabels[0] != "awaiting-merge:bt-wisp-mhr53" {
		t.Fatalf("RemoveLabels = %v, want the superseded hold", opts.RemoveLabels)
	}
}

func TestHoldSourceForMergeIsIdempotent(t *testing.T) {
	bd := &fakeAwaitingMergeBeads{}
	issue := hq19pxcSource()
	issue.Labels = []string{beads.AwaitingMergeLabel("bt-wisp-tgbaa")}

	if err := holdSourceForMerge(bd, "bt-j6uu.12", issue, "bt-wisp-tgbaa"); err != nil {
		t.Fatal(err)
	}
	if got := bd.updates["bt-j6uu.12"].RemoveLabels; len(got) != 0 {
		t.Fatalf("re-holding the same MR should remove nothing, got %v", got)
	}
}

func TestApplyMRHoldOrCloseReturnsFalseWhenHoldFails(t *testing.T) {
	// A failed hold must not fall through to the close — that would resurrect
	// hq-19pxc — but it also must not claim the bead was closed.
	bd := &fakeAwaitingMergeBeads{
		openMRs:   []*beads.Issue{openMR("bt-wisp-tgbaa")},
		updateErr: errors.New("dolt write failed"),
	}
	if held := applyMRHoldOrClose(bd, t.TempDir(), ExitCompleted, "bt-j6uu.12", hq19pxcSource()); !held {
		t.Fatal("a failed hold must still suppress the close")
	}
}

func TestApplyMRHoldOrCloseClosesWithoutMR(t *testing.T) {
	bd := &fakeAwaitingMergeBeads{}
	if held := applyMRHoldOrClose(bd, t.TempDir(), ExitCompleted, "bt-j6uu.12", hq19pxcSource()); held {
		t.Fatal("without an open MR gt done must still close the source bead")
	}
	if bd.updateCalls != 0 {
		t.Fatal("no hold expected")
	}
}
