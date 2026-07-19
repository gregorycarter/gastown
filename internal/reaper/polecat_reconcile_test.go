package reaper

import (
	"strings"
	"testing"
)

func TestReconcilePolecatAgentClosures_IgnoresNonPolecatEntries(t *testing.T) {
	entries := []ClosedEntry{
		{ID: "gt-1234", Title: "some regular stale issue", Database: "gastown_rig"},
		{ID: "gt-gastown-witness", Title: "witness singleton agent bead", Database: "gastown_rig"},
	}

	// townRoot deliberately has no mayor/rigs.json — if the function tried to
	// reconcile these entries it would error; it must not even try, since
	// neither entry is a polecat agent bead.
	result := ReconcilePolecatAgentClosures(t.TempDir(), entries, false)

	if len(result.Reconciled) != 0 || len(result.Skipped) != 0 || len(result.Errors) != 0 {
		t.Fatalf("expected no-op for non-polecat entries, got %+v", result)
	}
}

func TestReconcilePolecatAgentClosures_EmptyInput(t *testing.T) {
	result := ReconcilePolecatAgentClosures(t.TempDir(), nil, false)
	if len(result.Reconciled) != 0 || len(result.Skipped) != 0 || len(result.Errors) != 0 {
		t.Fatalf("expected empty result for no closed entries, got %+v", result)
	}
}

func TestReconcilePolecatAgentClosures_DryRunSkipsFilesystem(t *testing.T) {
	entries := []ClosedEntry{
		{ID: "gt-gastown-polecat-toast", Title: "toast agent bead", Database: "gastown"},
	}

	// dryRun=true must report the candidate without ever touching rig config
	// or the filesystem — townRoot has no mayor/rigs.json, so any attempt to
	// actually load the rig would surface as an error here.
	result := ReconcilePolecatAgentClosures(t.TempDir(), entries, true)

	if len(result.Errors) != 0 {
		t.Fatalf("dry-run reconcile produced errors: %v", result.Errors)
	}
	if len(result.Reconciled) != 1 || !strings.HasPrefix(result.Reconciled[0], "gastown/toast") {
		t.Fatalf("Reconciled = %v, want a single dry-run entry for gastown/toast", result.Reconciled)
	}
}

func TestReconcilePolecatAgentClosures_MissingRigConfigReportsError(t *testing.T) {
	entries := []ClosedEntry{
		{ID: "gt-gastown-polecat-toast", Title: "toast agent bead", Database: "gastown"},
	}

	// No mayor/rigs.json under this townRoot — reconciliation for a real (non
	// dry-run) pass must surface the failure per-entry rather than panicking
	// or silently dropping the closed polecat.
	result := ReconcilePolecatAgentClosures(t.TempDir(), entries, false)

	if len(result.Reconciled) != 0 || len(result.Skipped) != 0 {
		t.Fatalf("expected no reconciliation without a loadable rig, got %+v", result)
	}
	if len(result.Errors) != 1 || !strings.HasPrefix(result.Errors[0], "gastown/toast:") {
		t.Fatalf("Errors = %v, want a single gastown/toast error", result.Errors)
	}
}
