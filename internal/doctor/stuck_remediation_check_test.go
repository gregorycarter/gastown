package doctor

import (
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
)

type fakeSourceReader struct{ issues map[string]*beads.Issue }

func (f fakeSourceReader) Show(id string) (*beads.Issue, error) {
	issue, ok := f.issues[id]
	if !ok {
		return nil, beads.ErrNotFound
	}
	return issue, nil
}

func stubWorker(t *testing.T, alive bool, mailSubject string) {
	t.Helper()
	oldAlive, oldMail := workerSessionAlive, unreadCorrectionMail
	workerSessionAlive = func(string, string) bool { return alive }
	unreadCorrectionMail = func(string, string, string) string { return mailSubject }
	t.Cleanup(func() { workerSessionAlive, unreadCorrectionMail = oldAlive, oldMail })
}

// The hq-19pxc MR: created 14:14:42Z, source closed 14:14:46Z, gate failed,
// worker session dead, FIX_NEEDED delivered and never read.
func hq19pxcMRFields() *beads.MRFields {
	return &beads.MRFields{
		SourceIssue: "bt-j6uu.12",
		Branch:      "polecat/warboy/bt-j6uu.12+mtip9i05",
		CommitSHA:   "9a2bf2d",
		Worker:      "polecats/warboy",
		Rig:         "bridge_town_core",
	}
}

func TestClassifyStuckMRTerminalSourceAndUnreadMail(t *testing.T) {
	stubWorker(t, false, "FIX_NEEDED bt-j6uu.12")
	reader := fakeSourceReader{issues: map[string]*beads.Issue{
		"bt-j6uu.12": {ID: "bt-j6uu.12", Status: string(beads.StatusClosed)},
	}}

	reasons := classifyStuckMR(reader, "/town", "bridge_town_core", hq19pxcMRFields())
	if len(reasons) != 2 {
		t.Fatalf("reasons = %v, want the terminal source and the undeliverable FIX_NEEDED", reasons)
	}
	if !strings.Contains(reasons[0], "source is closed") {
		t.Fatalf("reasons[0] = %q", reasons[0])
	}
	if !strings.Contains(reasons[1], "no session") || !strings.Contains(reasons[1], "FIX_NEEDED") {
		t.Fatalf("reasons[1] = %q", reasons[1])
	}
}

func TestClassifyStuckMRRejectedCandidate(t *testing.T) {
	stubWorker(t, true, "")
	reader := fakeSourceReader{issues: map[string]*beads.Issue{
		"bt-j6uu.12": {
			ID:          "bt-j6uu.12",
			Status:      string(beads.StatusOpen),
			Description: "rejected_sha: 9a2bf2d\n",
		},
	}}

	reasons := classifyStuckMR(reader, "/town", "bridge_town_core", hq19pxcMRFields())
	if len(reasons) != 1 || !strings.Contains(reasons[0], "already-rejected candidate") {
		t.Fatalf("reasons = %v", reasons)
	}
}

func TestClassifyStuckMRHealthyQueueEntry(t *testing.T) {
	stubWorker(t, true, "")
	reader := fakeSourceReader{issues: map[string]*beads.Issue{
		"bt-j6uu.12": {
			ID:     "bt-j6uu.12",
			Status: string(beads.StatusInProgress),
			Labels: []string{beads.AwaitingMergeLabel("bt-wisp-tgbaa")},
		},
	}}

	// The normal case under workflow.close_on_merge: source held open, worker
	// alive, no rejection. Nothing to report.
	if reasons := classifyStuckMR(reader, "/town", "bridge_town_core", hq19pxcMRFields()); len(reasons) != 0 {
		t.Fatalf("healthy pending work flagged: %v", reasons)
	}
}

func TestClassifyStuckMRDeadWorkerWithoutEvidence(t *testing.T) {
	// A dead session alone is not a stall — gt done kills the session on every
	// clean completion, so most queued MRs have no live worker.
	stubWorker(t, false, "")
	reader := fakeSourceReader{issues: map[string]*beads.Issue{
		"bt-j6uu.12": {ID: "bt-j6uu.12", Status: string(beads.StatusInProgress)},
	}}
	if reasons := classifyStuckMR(reader, "/town", "bridge_town_core", hq19pxcMRFields()); len(reasons) != 0 {
		t.Fatalf("a dead session with no pending correction is normal: %v", reasons)
	}
}

func TestClassifyStuckMRDeadWorkerWithRejectedSource(t *testing.T) {
	stubWorker(t, false, "")
	reader := fakeSourceReader{issues: map[string]*beads.Issue{
		"bt-j6uu.12": {
			ID:     "bt-j6uu.12",
			Status: string(beads.StatusOpen),
			Labels: []string{beads.LabelMRRejected},
		},
	}}
	reasons := classifyStuckMR(reader, "/town", "bridge_town_core", hq19pxcMRFields())
	if len(reasons) != 1 || !strings.Contains(reasons[0], beads.LabelMRRejected) {
		t.Fatalf("reasons = %v", reasons)
	}
}

func TestClassifyStuckMRMissingSource(t *testing.T) {
	stubWorker(t, true, "")
	reader := fakeSourceReader{issues: map[string]*beads.Issue{}}

	if reasons := classifyStuckMR(reader, "/town", "bridge_town_core", hq19pxcMRFields()); len(reasons) != 1 ||
		!strings.Contains(reasons[0], "not readable") {
		t.Fatalf("reasons = %v", reasons)
	}

	noSource := hq19pxcMRFields()
	noSource.SourceIssue = ""
	if reasons := classifyStuckMR(reader, "/town", "bridge_town_core", noSource); len(reasons) != 1 ||
		!strings.Contains(reasons[0], "no source issue") {
		t.Fatalf("reasons = %v", reasons)
	}
}

func TestStuckRemediationSkippedWhenFlagOff(t *testing.T) {
	town := t.TempDir()
	off := false
	settings := config.NewTownSettings()
	settings.Workflow = &config.TownWorkflowConfig{CloseOnMerge: &off}
	writeTownSettings(t, town, settings)
	config.ResetCloseOnMergeCache()
	t.Cleanup(config.ResetCloseOnMergeCache)

	result := NewStuckRemediationCheck().Run(&CheckContext{TownRoot: town})
	if result.Status != StatusOK || !strings.Contains(result.Message, "skipped") {
		t.Fatalf("result = %+v, want a skip when the lifecycle is off", result)
	}
}
