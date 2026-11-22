package cmd

import (
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
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell bd stub")
	}
	townRoot := t.TempDir()
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
  list) echo '[]' ;;
  create|update|cook|mol|close|dep) echo "unexpected side effect: $cmd" >&2; exit 2 ;;
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
	return logPath
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
