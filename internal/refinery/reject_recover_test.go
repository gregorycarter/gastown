package refinery

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/rig"
)

type reslingCall struct {
	workDir, issueID, rigName, branch string
}

func stubRecovery(t *testing.T, alive bool, reslingErr error) *[]reslingCall {
	t.Helper()
	oldAlive := isWorkerSessionAlive
	oldResling := reslingRejectedWork
	calls := &[]reslingCall{}
	isWorkerSessionAlive = func(string, string) bool { return alive }
	reslingRejectedWork = func(workDir, issueID, rigName, branch string) error {
		*calls = append(*calls, reslingCall{workDir, issueID, rigName, branch})
		return reslingErr
	}
	t.Cleanup(func() {
		isWorkerSessionAlive = oldAlive
		reslingRejectedWork = oldResling
	})
	return calls
}

func testManager(out *bytes.Buffer) *Manager {
	return &Manager{
		rig:     &rig.Rig{Name: "bridge_town_core", Path: "/town/bridge_town_core"},
		workDir: "/town/bridge_town_core",
		output:  out,
	}
}

// hq-19pxc: FIX_NEEDED was delivered to warboy and never read, because warboy
// had no session. gt done kills the session on every clean completion, so this
// is the normal case, not the exception.
func TestRecoverRejectedWorkReslingsDeadWorker(t *testing.T) {
	calls := stubRecovery(t, false, nil)
	out := &bytes.Buffer{}
	outcome := rejectionOutcome{SourceIssue: "bt-j6uu.12"}

	testManager(out).recoverRejectedWork(rejectedMR(), gateReason, RejectOptions{CloseOnMerge: true}, &outcome)

	if len(*calls) != 1 {
		t.Fatalf("expected one re-sling, got %v", *calls)
	}
	call := (*calls)[0]
	if call.issueID != "bt-j6uu.12" || call.rigName != "bridge_town_core" ||
		call.branch != "polecat/warboy/bt-j6uu.12+mtip9i05" {
		t.Fatalf("re-sling called with %+v", call)
	}
	if call.workDir != "/town" {
		t.Fatalf("re-sling must run from the town root, got %q", call.workDir)
	}
	if !strings.Contains(out.String(), "re-slung onto") {
		t.Fatalf("decision not logged: %q", out.String())
	}
}

func TestRecoverRejectedWorkNudgesLiveWorker(t *testing.T) {
	calls := stubRecovery(t, true, nil)
	out := &bytes.Buffer{}
	outcome := rejectionOutcome{SourceIssue: "bt-j6uu.12"}

	// Notify=false: no nudge is sent, and crucially no re-sling either — a live
	// session already holds the worktree.
	testManager(out).recoverRejectedWork(rejectedMR(), gateReason, RejectOptions{CloseOnMerge: true}, &outcome)

	if len(*calls) != 0 {
		t.Fatalf("a live worker must not be re-slung: %v", *calls)
	}
	if !strings.Contains(out.String(), "alive") {
		t.Fatalf("decision not logged: %q", out.String())
	}
}

func TestRecoverRejectedWorkHonoursNoResling(t *testing.T) {
	calls := stubRecovery(t, false, nil)
	out := &bytes.Buffer{}
	outcome := rejectionOutcome{SourceIssue: "bt-j6uu.12"}

	testManager(out).recoverRejectedWork(rejectedMR(), gateReason, RejectOptions{CloseOnMerge: true, NoResling: true}, &outcome)

	if len(*calls) != 0 {
		t.Fatalf("--no-resling must suppress re-dispatch: %v", *calls)
	}
	if !strings.Contains(out.String(), "--no-resling") {
		t.Fatalf("suppression not logged: %q", out.String())
	}
}

func TestRecoverRejectedWorkLogsReslingFailureWithRepairCommand(t *testing.T) {
	stubRecovery(t, false, errors.New("bead not slingable"))
	out := &bytes.Buffer{}
	outcome := rejectionOutcome{SourceIssue: "bt-j6uu.12"}

	testManager(out).recoverRejectedWork(rejectedMR(), gateReason, RejectOptions{CloseOnMerge: true}, &outcome)

	logged := out.String()
	if !strings.Contains(logged, "could not re-sling") || !strings.Contains(logged, "gt sling bt-j6uu.12 bridge_town_core --branch") {
		t.Fatalf("a failed re-sling must print the manual repair: %q", logged)
	}
}

func TestRecoverRejectedWorkWithoutSourceIssue(t *testing.T) {
	calls := stubRecovery(t, false, nil)
	out := &bytes.Buffer{}
	outcome := rejectionOutcome{}
	mr := rejectedMR()
	mr.IssueID = ""

	testManager(out).recoverRejectedWork(mr, gateReason, RejectOptions{CloseOnMerge: true}, &outcome)

	if len(*calls) != 0 {
		t.Fatalf("nothing to re-sling without a source issue: %v", *calls)
	}
}
