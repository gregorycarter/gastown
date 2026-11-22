package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

// hq-mw7902: a context queued before its bead was labelled must not dispatch
// once the label lands. Labels are re-read every tick.
func TestApplyDispatchHold_LabelAddedAfterContextCreated(t *testing.T) {
	fields := &capacity.SlingContextFields{WorkBeadID: "kasb-mfyhh", TargetRig: "kasb_core"}
	row := scheduledContextAssessment{
		fields: fields,
		found:  true,
		ready:  true,
		info:   beadStatusInfo{Status: "open", Labels: []string{"demo"}},
	}

	unlabelled := row
	applyDispatchHold(&unlabelled, nil)
	if !unlabelled.ready || unlabelled.held != "" {
		t.Fatalf("unlabelled bead held: %+v", unlabelled)
	}

	// Label added after the context was created.
	row.info.Labels = []string{"demo", "dispatch:hold", "live-validation"}
	applyDispatchHold(&row, nil)
	if row.ready {
		t.Fatal("held bead is still ready to dispatch")
	}
	if got := row.pauseReason(); got != "held: dispatch:hold" {
		t.Fatalf("pauseReason = %q, want %q", got, "held: dispatch:hold")
	}
	if row.fields.DispatchFailures != 0 {
		t.Fatal("a hold must not count as a dispatch failure")
	}
}

// An explicit --force records the labels it overrode; a hold added later
// still stops the context.
func TestApplyDispatchHold_OverrideOnlyCoversRecordedLabels(t *testing.T) {
	row := scheduledContextAssessment{
		fields: &capacity.SlingContextFields{WorkBeadID: "kasb-x", TargetRig: "kasb_core", HoldOverride: "live-validation"},
		found:  true,
		ready:  true,
		info:   beadStatusInfo{Status: "open", Labels: []string{"live-validation"}},
	}
	overridden := row
	applyDispatchHold(&overridden, nil)
	if !overridden.ready {
		t.Fatal("explicitly overridden hold blocked dispatch")
	}

	row.info.Labels = append(row.info.Labels, "dispatch:hold")
	applyDispatchHold(&row, nil)
	if row.ready || row.held != "dispatch:hold" {
		t.Fatalf("later dispatch:hold did not stop the context: %+v", row)
	}
}

// Repair contexts keep their own hold rule (mqRepairHeld); the shared
// predicate does not apply to them.
func TestApplyDispatchHold_RecoveryContextUnchanged(t *testing.T) {
	row := scheduledContextAssessment{
		context: slingContextRecord{issue: &beads.Issue{ID: "kasb-wisp-ctx"}},
		fields:  &capacity.SlingContextFields{WorkBeadID: "kasb-x", TargetRig: "kasb_core", ResumeMR: "kasb-wisp-abc"},
		found:   true,
		ready:   true,
		info:    beadStatusInfo{Status: "in_progress", Labels: []string{"gt-fork"}},
	}
	applyDispatchHold(&row, nil)
	if !row.ready || row.held != "" {
		t.Fatalf("recovery context changed by the shared hold predicate: %+v", row)
	}
	if got := readySlingContextsFromAssessments([]scheduledContextAssessment{row}); len(got) != 1 {
		t.Fatalf("recovery context not dispatchable: %v", got)
	}
}

func TestReadySlingContexts_ExcludesHeldRows(t *testing.T) {
	held := scheduledContextAssessment{
		context: slingContextRecord{issue: &beads.Issue{ID: "kasb-wisp-ctx"}},
		fields:  &capacity.SlingContextFields{WorkBeadID: "kasb-h", TargetRig: "kasb_core"},
		found:   true,
		ready:   true,
		info:    beadStatusInfo{Status: "open", Labels: []string{"needs-operator"}},
	}
	applyDispatchHold(&held, nil)
	if got := readySlingContextsFromAssessments([]scheduledContextAssessment{held}); len(got) != 0 {
		t.Fatalf("held context dispatched: %v", got)
	}
}

// setupHeldScheduleTest is a town with one rig (gastown, prefix zz-) whose bd
// stub serves zz-held with the given labels and refuses any write.
func setupHeldScheduleTest(t *testing.T, labelsJSON string) (logPath string) {
	logPath, _ = setupHeldScheduleTown(t, labelsJSON)
	return logPath
}

func setupHeldScheduleTown(t *testing.T, labelsJSON string) (logPath, townRoot string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell bd stub")
	}
	townRoot = t.TempDir()
	for _, dir := range []string{"mayor/rig", "gastown/mayor/rig/.beads", ".beads", "bin"} {
		if err := os.MkdirAll(filepath.Join(townRoot, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	rigs := &config.RigsConfig{Version: 1, Rigs: map[string]config.RigEntry{
		"gastown": {GitURL: "git@github.com:test/gastown.git", AddedAt: time.Now().Truncate(time.Second),
			BeadsConfig: &config.BeadsConfig{Repo: "local", Prefix: "zz-"}},
	}}
	if err := config.SaveRigsConfig(filepath.Join(townRoot, "mayor", "rigs.json"), rigs); err != nil {
		t.Fatal(err)
	}
	routes := `{"prefix":"zz-","path":"gastown/mayor/rig"}` + "\n"
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"), []byte(routes), 0644); err != nil {
		t.Fatal(err)
	}
	logPath = filepath.Join(townRoot, "bd.log")
	script := `#!/bin/sh
echo "$*" >> "${BD_LOG}"
cmd="$1"; shift || true
if [ "$cmd" = "--allow-stale" ]; then cmd="$1"; shift || true; fi
case "$cmd" in
  show) echo '[{"id":"zz-held","title":"Held bead","status":"open","assignee":"","labels":` + labelsJSON + `}]' ;;
  list|query) if [ -n "${STUB_CTX_JSON:-}" ]; then printf '%s\n' "$STUB_CTX_JSON"; else echo '[]'; fi ;;
  update) [ "${STUB_ALLOW_UPDATE:-}" = "1" ] && exit 0; echo "unexpected side effect: $cmd" >&2; exit 2 ;;
  create|cook|mol|close|dep) echo "unexpected side effect: $cmd" >&2; exit 2 ;;
esac
exit 0
`
	_ = writeBDStub(t, filepath.Join(townRoot, "bin"), script, "")
	t.Setenv("BD_LOG", logPath)
	t.Setenv("PATH", filepath.Join(townRoot, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(EnvGTRole, "mayor")
	t.Setenv("GT_TEST_NO_NUDGE", "1")
	cwd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	if err := os.Chdir(filepath.Join(townRoot, "mayor", "rig")); err != nil {
		t.Fatal(err)
	}
	return logPath, townRoot
}

// Deferred `gt sling` (and so the convoy feed's sling) refuses a held bead
// before creating any sling context (hq-mw7902).
func TestScheduleBeadRefusesHeldBead(t *testing.T) {
	logPath := setupHeldScheduleTest(t, `["demo","dispatch:hold","live-validation"]`)

	err := scheduleBead("zz-held", "gastown", ScheduleOptions{})
	if err == nil || !strings.Contains(err.Error(), "is held: dispatch:hold,live-validation") {
		t.Fatalf("scheduleBead error = %v, want held refusal", err)
	}
	log, _ := os.ReadFile(logPath)
	if strings.Contains(string(log), "create") {
		t.Fatalf("a sling context was created for a held bead; bd log:\n%s", log)
	}
}

func contextListJSON(t *testing.T, fields *capacity.SlingContextFields) string {
	t.Helper()
	desc, err := json.Marshal(beads.FormatSlingContextDescription(fields))
	if err != nil {
		t.Fatal(err)
	}
	return `[{"id":"zz-wisp-ctx","title":"sling-context: Held bead","status":"open","labels":["gt:sling-context"],"description":` + string(desc) + `}]`
}

// Review BLOCKER: --force (Refinery reject, Deacon redispatch, the Mayor's
// recovery sling) and even --override-hold must never write into an existing
// recovery context, whose identity is compared every tick.
func TestScheduleBeadNeverWritesIntoRecoveryContext(t *testing.T) {
	for _, recovery := range []*capacity.SlingContextFields{
		{Version: 1, WorkBeadID: "zz-held", TargetRig: "gastown", ResumeMR: "kasb-wisp-mr1", ResumeWorker: "nux"},
		{Version: 1, WorkBeadID: "zz-held", TargetRig: "gastown", ResumeDependency: true},
	} {
		for _, opts := range []ScheduleOptions{{Force: true}, {Force: true, OverrideHold: true}} {
			logPath := setupHeldScheduleTest(t, `["dispatch:hold"]`)
			t.Setenv("STUB_CTX_JSON", contextListJSON(t, recovery))
			if err := scheduleBead("zz-held", "gastown", opts); err != nil {
				t.Fatalf("opts %+v: unexpected error %v", opts, err)
			}
			log, _ := os.ReadFile(logPath)
			if strings.Contains(string(log), "update") {
				t.Fatalf("opts %+v wrote into a recovery context; bd log:\n%s", opts, log)
			}
		}
	}
}

// A recorded override never changes recovery identity.
func TestSameMQResumeContextIgnoresHoldOverride(t *testing.T) {
	a := &capacity.SlingContextFields{Version: 1, WorkBeadID: "kasb-x", TargetRig: "kasb_core", ResumeMR: "kasb-wisp-mr1"}
	b := *a
	b.HoldOverride = "dispatch:hold"
	if !sameMQResumeContext(a, &b) {
		t.Fatal("hold_override broke recovery identity")
	}
}

// --force keeps its 5ff67b32 meaning; only --override-hold queues a held bead.
func TestScheduleBeadForceDoesNotOverrideHold(t *testing.T) {
	setupHeldScheduleTest(t, `["dispatch:hold"]`)
	err := scheduleBead("zz-held", "gastown", ScheduleOptions{Force: true})
	var held *dispatchHeldError
	if !errors.As(err, &held) {
		t.Fatalf("--force alone queued a held bead (err=%v)", err)
	}
}

// --override-hold on an existing ordinary context records the override.
func TestScheduleBeadOverrideHoldRecordsOnExistingContext(t *testing.T) {
	logPath := setupHeldScheduleTest(t, `["dispatch:hold"]`)
	t.Setenv("STUB_CTX_JSON", contextListJSON(t, &capacity.SlingContextFields{Version: 1, WorkBeadID: "zz-held", TargetRig: "gastown"}))
	t.Setenv("STUB_ALLOW_UPDATE", "1")
	if err := scheduleBead("zz-held", "gastown", ScheduleOptions{OverrideHold: true}); err != nil {
		t.Fatal(err)
	}
	log, _ := os.ReadFile(logPath)
	if !strings.Contains(string(log), "update zz-wisp-ctx") {
		t.Fatalf("override not recorded; bd log:\n%s", log)
	}
}

// runBatchSchedule never overrides holds, even with --override-hold set.
func TestRunBatchScheduleNeverOverridesHold(t *testing.T) {
	logPath, townRoot := setupHeldScheduleTown(t, `["dispatch:hold"]`)
	prev := slingOverrideHold
	slingOverrideHold = true
	t.Cleanup(func() { slingOverrideHold = prev })
	if err := runBatchSchedule([]string{"zz-held"}, "gastown", townRoot); err == nil {
		t.Fatal("batch scheduled a held bead")
	}
	log, _ := os.ReadFile(logPath)
	if strings.Contains(string(log), "create") {
		t.Fatalf("batch created a context for a held bead; bd log:\n%s", log)
	}
}

// gt sling exits with ExitCodeDispatchHeld for a held refusal, which the
// automatic re-sling callers recognise as a skip.
func TestRunSlingHeldExitCode(t *testing.T) {
	setupHeldScheduleTown(t, `["dispatch:hold"]`)
	settings := `{"type":"town-settings","version":1,"scheduler":{"max_polecats":7}}`
	townRoot, _ := os.Getwd()
	townRoot = filepath.Dir(filepath.Dir(townRoot))
	if err := os.MkdirAll(filepath.Join(townRoot, "settings"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(townRoot, "settings", "config.json"), []byte(settings), 0644); err != nil {
		t.Fatal(err)
	}
	if err := config.SaveTownConfig(filepath.Join(townRoot, "mayor", "town.json"), &config.TownConfig{
		Type: "town", Version: config.CurrentTownVersion, Name: "held-test", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	err := runSling(nil, []string{"zz-held", "gastown"})
	code, ok := IsSilentExit(err)
	if !ok || code != capacity.ExitCodeDispatchHeld {
		t.Fatalf("runSling = %v, want silent exit %d", err, capacity.ExitCodeDispatchHeld)
	}
}

// Review item 3: a dependency-resume context applies the shared hold; only
// same-MR repair contexts are exempt (they use mqRepairHeld).
func TestApplyDispatchHold_DependencyResumeIsHeld(t *testing.T) {
	row := scheduledContextAssessment{
		context: slingContextRecord{issue: &beads.Issue{ID: "kasb-wisp-ctx"}},
		fields:  &capacity.SlingContextFields{WorkBeadID: "kasb-xfle.9", TargetRig: "kasb_core", ResumeDependency: true},
		found:   true,
		ready:   true,
		info:    beadStatusInfo{Status: "open", Labels: []string{"dispatch:hold"}},
	}
	applyDispatchHold(&row, nil)
	if row.ready || row.held != "dispatch:hold" {
		t.Fatalf("dependency resume of a held bead stays ready: %+v", row)
	}
}

// Review item 2: once the overridden label is gone, the override is pruned,
// so re-adding the hold stops the context again.
func TestHoldOverridePrunedWhenLabelRemoved(t *testing.T) {
	if kept, changed := prunedHoldOverride([]string{"dispatch:hold", "live-validation"}, []string{"live-validation"}); !changed || kept != "live-validation" {
		t.Fatalf("prunedHoldOverride = (%q, %v)", kept, changed)
	}
	if _, changed := prunedHoldOverride([]string{"dispatch:hold"}, []string{"dispatch:hold", "demo"}); changed {
		t.Fatal("override pruned while its label is still present")
	}

	logPath := setupHeldScheduleTest(t, `[]`)
	t.Setenv("STUB_ALLOW_UPDATE", "1")
	rigDir, _ := os.Getwd()
	fields := &capacity.SlingContextFields{WorkBeadID: "zz-held", TargetRig: "gastown", HoldOverride: "dispatch:hold"}
	rows := []scheduledContextAssessment{{
		context: slingContextRecord{issue: &beads.Issue{ID: "zz-wisp-ctx"}, workDir: rigDir, beadsDir: filepath.Join(rigDir, ".beads")},
		fields:  fields,
		found:   true,
		ready:   true,
		info:    beadStatusInfo{Status: "open"}, // hold label removed
	}}
	pruneStaleHoldOverrides(rows)
	if fields.HoldOverride != "" {
		t.Fatalf("override kept after its label was removed: %q", fields.HoldOverride)
	}
	if log, _ := os.ReadFile(logPath); !strings.Contains(string(log), "update zz-wisp-ctx") {
		t.Fatalf("pruned override not persisted; bd log:\n%s", log)
	}

	// The hold is re-added: the context is held again.
	rows[0].info.Labels = []string{"dispatch:hold"}
	applyDispatchHold(&rows[0], nil)
	if rows[0].ready {
		t.Fatal("re-added hold did not stop the context")
	}
}
