package refinery

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

// hq-mw7902 review: the reject re-sling uses --force, which never overrides a
// hold. A held source is a skip: no failure note, no repair instruction.
func TestRecoverRejectedWorkHeldSourceIsSkip(t *testing.T) {
	held := exec.Command("sh", "-c", "exit 75").Run()
	if !capacity.IsDispatchHeldExit(held) {
		t.Fatalf("precondition: %v is not a held exit", held)
	}
	calls := stubRecovery(t, false, held)
	out := &bytes.Buffer{}
	outcome := rejectionOutcome{SourceIssue: "bt-j6uu.12"}

	testManager(out).recoverRejectedWork(rejectedMR(), gateReason, RejectOptions{CloseOnMerge: true}, &outcome)

	if len(*calls) != 1 {
		t.Fatalf("expected one re-sling attempt, got %v", *calls)
	}
	notes := strings.Join(outcome.Notes, "; ")
	if !strings.Contains(notes, "re-sling skipped: source held") || strings.Contains(notes, "re-sling failed") {
		t.Fatalf("notes = %q, want a held skip", notes)
	}
	if strings.Contains(out.String(), "Warning") || strings.Contains(out.String(), "repair with") {
		t.Fatalf("held skip reported as a failure: %s", out.String())
	}
}
