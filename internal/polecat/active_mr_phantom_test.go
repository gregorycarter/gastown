package polecat

import (
	"errors"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

type phantomReader struct {
	issues map[string]*beads.Issue
	err    error
}

func (r phantomReader) Show(id string) (*beads.Issue, error) {
	if r.err != nil {
		return nil, r.err
	}
	issue, ok := r.issues[id]
	if !ok {
		return nil, beads.ErrNotFound
	}
	return issue, nil
}

// bt-w25y: four polecats held active_mr pointers to MR wisps absent from both
// databases, each blocking sandbox reuse and non-force nuke.
func phantomInput(completedAgo time.Duration, town IssueReader) ActiveMRInput {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	return ActiveMRInput{
		ActiveMR:            "bt-wisp-gone",
		SourceIssueHint:     "bt-j6uu.12",
		TownReader:          town,
		CompletionTime:      now.Add(-completedAgo).Format(time.RFC3339),
		PhantomReleaseAfter: DefaultPhantomMRAge,
		Now:                 now,
	}
}

// The source is still open — upstream would hold forever, because the release
// requires a proven-terminal source and the MR that would prove it is gone.
func openSourceReader() phantomReader {
	return phantomReader{issues: map[string]*beads.Issue{
		"bt-j6uu.12": {ID: "bt-j6uu.12", Status: string(beads.StatusOpen)},
	}}
}

func TestPhantomActiveMRReleasedAfter24h(t *testing.T) {
	town := phantomReader{issues: map[string]*beads.Issue{}}
	got := AssessActiveMR(openSourceReader(), phantomInput(48*time.Hour, town))

	if got.Pending {
		t.Fatalf("hold not released: %+v", got)
	}
	if !got.PhantomReleased {
		t.Fatal("release must be attributed to the phantom rule, not a terminal source")
	}
	if got.Reason != "" {
		t.Fatalf("Reason = %q, want empty for a released hold", got.Reason)
	}
}

func TestPhantomActiveMRHeldWhenRecent(t *testing.T) {
	town := phantomReader{issues: map[string]*beads.Issue{}}
	// gt polecat reconcile owns anything younger than the threshold.
	got := AssessActiveMR(openSourceReader(), phantomInput(2*time.Hour, town))
	if !got.Pending || got.PhantomReleased {
		t.Fatalf("a 2h-old absence must stay pending for reconcile to repair: %+v", got)
	}
}

func TestPhantomActiveMRHeldWhenResolvableTownSide(t *testing.T) {
	town := phantomReader{issues: map[string]*beads.Issue{
		"bt-wisp-gone": {ID: "bt-wisp-gone", Status: string(beads.StatusOpen)},
	}}
	got := AssessActiveMR(openSourceReader(), phantomInput(48*time.Hour, town))
	if !got.Pending {
		t.Fatal("an MR that still resolves town-side is not a phantom")
	}
}

func TestPhantomActiveMRHeldWithoutTownReader(t *testing.T) {
	got := AssessActiveMR(openSourceReader(), phantomInput(48*time.Hour, nil))
	if !got.Pending {
		t.Fatal("absence from one database proves nothing")
	}
}

func TestPhantomActiveMRHeldOnTownLookupError(t *testing.T) {
	town := phantomReader{err: errors.New("dolt timeout")}
	got := AssessActiveMR(openSourceReader(), phantomInput(48*time.Hour, town))
	if !got.Pending {
		t.Fatal("a lookup error is not proof of absence")
	}
}

func TestPhantomActiveMRHeldWithoutCompletionTime(t *testing.T) {
	in := phantomInput(48*time.Hour, phantomReader{issues: map[string]*beads.Issue{}})
	in.CompletionTime = ""
	if got := AssessActiveMR(openSourceReader(), in); !got.Pending {
		t.Fatal("no timestamp means no age evidence")
	}

	in.CompletionTime = "not-a-timestamp"
	if got := AssessActiveMR(openSourceReader(), in); !got.Pending {
		t.Fatal("an unparseable timestamp must not release the hold")
	}
}

func TestPhantomActiveMRDisabledByDefault(t *testing.T) {
	// PhantomReleaseAfter == 0 is what workflow.close_on_merge=false produces:
	// byte-for-byte upstream behaviour.
	in := phantomInput(48*time.Hour, phantomReader{issues: map[string]*beads.Issue{}})
	in.PhantomReleaseAfter = 0
	if got := AssessActiveMR(openSourceReader(), in); !got.Pending || got.PhantomReleased {
		t.Fatalf("flag off must keep the hold: %+v", got)
	}
}

func TestPhantomRuleDoesNotApplyToTerminalMR(t *testing.T) {
	// A CLOSED (not missing) MR takes the existing terminal-source path; the
	// phantom rule is only for beads that resolve nowhere.
	rig := phantomReader{issues: map[string]*beads.Issue{
		"bt-wisp-gone": {ID: "bt-wisp-gone", Status: string(beads.StatusClosed)},
		"bt-j6uu.12":   {ID: "bt-j6uu.12", Status: string(beads.StatusOpen)},
	}}
	got := AssessActiveMR(rig, phantomInput(48*time.Hour, phantomReader{issues: map[string]*beads.Issue{}}))
	if !got.Pending || got.PhantomReleased {
		t.Fatalf("a closed MR with an open source stays pending: %+v", got)
	}
}
