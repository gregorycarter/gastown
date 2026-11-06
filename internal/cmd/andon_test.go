package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

func writeAndonFixture(t *testing.T, townRoot, value string) string {
	t.Helper()
	path := filepath.Join(townRoot, ".runtime", "andon", "kasb_core.json")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

const activeAndonFixture = `{"schema_version":1,"active":true,"incident_id":"kasb-incident","allowed_beads":["kasb-fix"]}`

func TestAndonStateAndExplicitSourceAdmission(t *testing.T) {
	town := t.TempDir()
	if err := checkAndonDispatch(town, "kasb_core", "kasb-feature"); err != nil {
		t.Fatalf("absent state changed ordinary dispatch: %v", err)
	}
	writeAndonFixture(t, town, activeAndonFixture)
	for _, tc := range []struct {
		rig, bead string
		held      bool
	}{
		{"kasb_core", "kasb-feature", true},
		{"kasb_core", "kasb-fix", false},
		{"kasb_core", "kasb-fix.1", true},
		{"kasb_core", "", true},
		{"hisn", "hisn-feature", false},
	} {
		err := checkAndonDispatch(town, tc.rig, tc.bead)
		var hold *andonHoldError
		if errors.As(err, &hold) != tc.held {
			t.Errorf("%s/%s: held=%v, err=%v", tc.rig, tc.bead, tc.held, err)
		}
	}
	writeAndonFixture(t, town, `{"schema_version":1,"active":false,"allowed_beads":[]}`)
	if err := checkAndonDispatch(town, "kasb_core", "kasb-feature"); err != nil {
		t.Fatalf("released state remains held: %v", err)
	}
}

func TestAndonMalformedStatePreservesOnlyAffectedRigHold(t *testing.T) {
	for _, value := range []string{
		`{`, `null`, `{}`, `[]`,
		`{"schema_version":2,"active":false,"allowed_beads":[]}`,
		`{"schema_version":1,"active":null,"allowed_beads":[]}`,
		`{"schema_version":1,"active":"false","allowed_beads":[]}`,
		`{"schema_version":1,"active":false,"allowed_beads":null}`,
		`{"schema_version":1,"active":true,"allowed_beads":["kasb-fix",null]}`,
		`{"schema_version":1,"active":true,"allowed_beads":["kasb-fix",7]}`,
	} {
		t.Run(value, func(t *testing.T) {
			town := t.TempDir()
			writeAndonFixture(t, town, value)
			if err := checkAndonDispatch(town, "kasb_core", "kasb-fix"); err == nil {
				t.Fatal("malformed state permitted dispatch")
			}
			if err := checkAndonDispatch(town, "hisn", "hisn-feature"); err != nil {
				t.Fatalf("another rig was held: %v", err)
			}
		})
	}
}

func TestAndonUnreadableStatePreservesHold(t *testing.T) {
	town := t.TempDir()
	path := filepath.Join(town, ".runtime", "andon", "kasb_core.json")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	if err := checkAndonDispatch(town, "kasb_core", "kasb-fix"); err == nil {
		t.Fatal("unreadable state permitted dispatch")
	}
}

func TestAndonPlansRemediationAndOtherRigsBeforeCapacity(t *testing.T) {
	town := t.TempDir()
	writeAndonFixture(t, town, activeAndonFixture)
	ready := []capacity.PendingBead{
		{ID: "ctx-p0", WorkBeadID: "kasb-feature", TargetRig: "kasb_core", Priority: 0, HasPriority: true, Labels: []string{"ci-train-failure", "landing-unblock"}},
		{ID: "kasb-fix", WorkBeadID: "kasb-other", TargetRig: "kasb_core"},
		{ID: "ctx-fix", WorkBeadID: "kasb-fix", TargetRig: "kasb_core"},
		{ID: "ctx-other-rig", WorkBeadID: "hisn-feature", TargetRig: "hisn"},
	}
	plan, held := planDispatchWithAndon(town, 2, 2, ready, capacity.WIPCap{}, nil)
	if len(plan.ToDispatch) != 2 || plan.ToDispatch[0].WorkBeadID != "kasb-fix" || plan.ToDispatch[1].WorkBeadID != "hisn-feature" {
		t.Fatalf("held queue head consumed slots: %+v", plan)
	}
	if len(held) != 2 || plan.Skipped != 2 || !strings.Contains(plan.Reason, "andon") {
		t.Fatalf("missing hold disposition: plan=%+v held=%+v", plan, held)
	}
}

func TestAndonRechecksAtExecutionAndDoesNotConsumeRetry(t *testing.T) {
	town := t.TempDir()
	b := capacity.PendingBead{ID: "ctx-feature", WorkBeadID: "kasb-feature", TargetRig: "kasb_core"}
	plan, _ := planDispatchWithAndon(town, 1, 1, []capacity.PendingBead{b}, capacity.WIPCap{}, nil)
	if len(plan.ToDispatch) != 1 {
		t.Fatal("ordinary planning failed")
	}
	writeAndonFixture(t, town, activeAndonFixture)
	_, err := dispatchSingleBead(b, town, "test") // Stops before even requiring a context or DB.
	if err == nil || !isDispatchDeferral(fmt.Errorf("dispatch: %w", err)) || classifyDispatchFailure(err).countsTowardCircuitBreaker() {
		t.Fatalf("late hold not a retry-free deferral: %v", err)
	}
	if err := validatePendingBeadForDispatch(town, b, false); !isDispatchDeferral(err) {
		t.Fatalf("validation ignored hold: %v", err)
	}
}

func TestAndonSameWorkerRecoveryStillRequiresAllowedSource(t *testing.T) {
	town := t.TempDir()
	writeAndonFixture(t, town, activeAndonFixture)
	b := capacity.PendingBead{ID: "kasb-fix", WorkBeadID: "kasb-feature", TargetRig: "kasb_core",
		Context: &capacity.SlingContextFields{ResumeMR: "kasb-mr", WorkBeadID: "kasb-feature", TargetRig: "kasb_core"}}
	_, err := dispatchSingleBead(b, town, "test")
	if !isDispatchDeferral(err) {
		t.Fatalf("same-worker recovery bypassed source whitelist: %v", err)
	}
}

func TestAndonQueueDisplayExplainsHeldWorkAndRelease(t *testing.T) {
	town := t.TempDir()
	path := writeAndonFixture(t, town, activeAndonFixture)
	rows := []scheduledContextAssessment{{ready: true, found: true,
		context: slingContextRecord{issue: &beads.Issue{ID: "ctx-feature"}},
		fields:  &capacity.SlingContextFields{WorkBeadID: "kasb-feature", TargetRig: "kasb_core"},
		info:    beadStatusInfo{Status: "open", Title: "Feature"}}}
	annotateAndon(rows, town)
	items := scheduledBeadInfosFromAssessments(rows)
	if len(items) != 1 || !items[0].Blocked || !strings.Contains(items[0].Reason, "kasb-incident") {
		t.Fatalf("queue does not explain hold: %+v", items)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	annotateAndon(rows, town)
	items = scheduledBeadInfosFromAssessments(rows)
	if items[0].Blocked || items[0].Reason != "" {
		t.Fatalf("queue kept stale hold: %+v", items)
	}
}

func TestAndonDirectAndFormulaTargetCannotBypassWithForce(t *testing.T) {
	town := t.TempDir()
	writeAndonFixture(t, town, activeAndonFixture)
	for _, target := range []string{"kasb_core", "kasb_core/polecats/alice", "kasb_core/crew/demo"} {
		for _, bead := range []string{"kasb-feature", ""} {
			_, err := resolveTarget(target, ResolveTargetOptions{TownRoot: town, BeadID: bead, Force: true, DryRun: true})
			if !isDispatchDeferral(err) {
				t.Errorf("target %s bead %q bypassed hold: %v", target, bead, err)
			}
		}
	}
	_, err := executeSling(SlingParams{TownRoot: town, RigName: "kasb_core", BeadID: "kasb-feature", Force: true})
	if !isDispatchDeferral(err) {
		t.Fatalf("batch/direct sling bypassed hold: %v", err)
	}
}

func TestAndonResolvedAgentAliasCannotBypassHold(t *testing.T) {
	town := t.TempDir()
	writeAndonFixture(t, town, activeAndonFixture)
	previous := resolveTargetAgentFn
	t.Cleanup(func() { resolveTargetAgentFn = previous })
	resolveTargetAgentFn = func(string) (string, string, string, error) {
		return "kasb_core/crew/demo", "%1", town, nil
	}
	_, err := resolveTarget("demo-alias", ResolveTargetOptions{TownRoot: town, BeadID: "kasb-feature", Force: true})
	if !isDispatchDeferral(err) {
		t.Fatalf("resolved alias bypassed hold: %v", err)
	}
}

func TestAndonLateHoldBackfillsWithoutFailure(t *testing.T) {
	town := t.TempDir()
	ready := []capacity.PendingBead{
		{ID: "ctx-feature", WorkBeadID: "kasb-feature", TargetRig: "kasb_core"},
		{ID: "ctx-fix", WorkBeadID: "kasb-fix", TargetRig: "kasb_core"},
	}
	plan, _ := planDispatchWithAndon(town, 1, 1, ready, capacity.WIPCap{}, nil)
	writeAndonFixture(t, town, activeAndonFixture)
	var executed []string
	cycle := capacity.DispatchCycle{
		Validate:   func(b capacity.PendingBead) error { return checkAndonDispatch(town, b.TargetRig, b.WorkBeadID) },
		Execute:    func(b capacity.PendingBead) error { executed = append(executed, b.WorkBeadID); return nil },
		OnSuccess:  func(capacity.PendingBead) error { return nil },
		IsDeferral: isDispatchDeferral,
	}
	report, err := cycle.RunPlan(plan)
	if err != nil || report.Failed != 0 || report.Deferred != 1 || report.Dispatched != 1 || len(executed) != 1 || executed[0] != "kasb-fix" {
		t.Fatalf("late hold wasted slot or counted failure: report=%+v executed=%v err=%v", report, executed, err)
	}
}
