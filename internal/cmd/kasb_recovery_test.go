package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
)

func kasbRecoveryFixture() (*beads.Issue, *beads.Issue, func(string) (*beads.Issue, error)) {
	source := &beads.Issue{ID: "kasb-test.1", Status: "in_progress", Assignee: "kasb_core/polecats/chrome"}
	mr := &beads.Issue{ID: "kasb-wisp-test", Status: "blocked", Labels: []string{"gt:merge-request", "needs-rebase"}, Description: beads.FormatMRFields(&beads.MRFields{Rig: "kasb_core", Worker: "chrome", SourceIssue: source.ID, Branch: "polecat/chrome/kasb-test.1+abc", CommitSHA: strings.Repeat("a", 40), Target: "main"})}
	return mr, source, func(id string) (*beads.Issue, error) {
		if id == source.ID {
			return source, nil
		}
		if id == mr.ID {
			return mr, nil
		}
		return nil, nil
	}
}
func TestKasbRecoveryPreservesNativeMRAndRejectsChangedCustody(t *testing.T) {
	for _, scenario := range []string{"ready", "closed", "held", "reassigned", "foreign-rig", "existing-receipt", "duplicate-field"} {
		t.Run(scenario, func(t *testing.T) {
			mr, source, show := kasbRecoveryFixture()
			switch scenario {
			case "closed":
				source.Status = "closed"
			case "held":
				mr.Labels = append(mr.Labels, "needs-operator")
			case "reassigned":
				source.Assignee = "kasb_core/polecats/other"
			case "foreign-rig":
				mr.Description = strings.ReplaceAll(mr.Description, "kasb_core", "hisn")
			case "existing-receipt":
				mr.Metadata = json.RawMessage(`{"delivery_recovery":{}}`)
			case "duplicate-field":
				mr.Description += "\nworker: chrome\n"
			}
			state, err := kasbRecoveryCandidate(mr, strings.Repeat("b", 40), show)
			if (err == nil) != (scenario == "ready") {
				t.Fatalf("state=%v err=%v", state, err)
			}
			if err == nil && (state.Record.MR != mr.ID || state.Record.Source != source.ID || strings.Contains(state.Record.Instructions, "resubmit.mjs")) {
				t.Fatal("wrong Kasb recovery contract")
			}
		})
	}
}
func TestKasbRecoveryWorkerUsesKasbOriginAndSession(t *testing.T) {
	mr, _, show := kasbRecoveryFixture()
	state, err := kasbRecoveryCandidate(mr, strings.Repeat("b", 40), show)
	if err != nil {
		t.Fatal(err)
	}
	town, _ := filepath.EvalSymlinks(t.TempDir())
	root := filepath.Join(town, "kasb_core", "polecats", "chrome", "kasb_core")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	ops := mqResumeWorkerOps{git: func(_ string, args ...string) (string, error) {
		switch strings.Join(args, " ") {
		case "rev-parse --show-toplevel":
			return root, nil
		case "branch --show-current":
			return state.Record.Branch, nil
		case "rev-parse HEAD":
			return state.Record.Submitted, nil
		case "status --porcelain":
			return "", nil
		case "remote get-url origin":
			return "https://github.com/gregorycarter/kasb-core.git", nil
		case "ls-remote --refs origin refs/heads/" + state.Record.Branch:
			return state.Record.Submitted + "\trefs/heads/" + state.Record.Branch, nil
		}
		t.Fatalf("unexpected git %v", args)
		return "", nil
	}, assignments: func(actor string) ([]*beads.Issue, error) {
		if actor != "kasb_core/polecats/chrome" {
			t.Fatal(actor)
		}
		return nil, nil
	}, running: func(name string) (bool, error) {
		if name != "kasb-chrome" {
			t.Fatal(name)
		}
		return false, nil
	}}
	if err := validateMQResumeWorkerWith(town, state, ops); err != nil {
		t.Fatal(err)
	}
	ops.assignments = func(string) ([]*beads.Issue, error) { return []*beads.Issue{{ID: "kasb-other", Status: "hooked"}}, nil }
	if validateMQResumeWorkerWith(town, state, ops) == nil {
		t.Fatal("reassigned worker admitted")
	}
}
func TestKasbDependencyAndCallerRemainRigBound(t *testing.T) {
	source := &beads.Issue{ID: "kasb-test", Status: "blocked", Assignee: "kasb_core/polecats/chrome", Dependencies: []beads.IssueDep{{ID: "kasb-prereq", DependencyType: "blocks"}}}
	worker, err := dependencyResumeSource(source, func(string) (*beads.Issue, error) { return &beads.Issue{ID: "kasb-prereq", Status: "closed"}, nil })
	if err != nil || worker != "chrome" {
		t.Fatal(worker, err)
	}
	source.Assignee = "hisn/polecats/chrome"
	if _, err := dependencyResumeSource(source, nil); err == nil {
		t.Fatal("cross-rig worker admitted")
	}
	if validateMQResumeCaller("kasb_core/refinery", "kasb_core", "kasb_core/refinery") != nil {
		t.Fatal("Kasb refinery refused")
	}
	if validateMQResumeCaller("hisn/refinery", "kasb_core", "hisn/refinery") == nil {
		t.Fatal("foreign refinery accepted")
	}
	if !shouldCheckpointBlockedDone("kasb_core", ExitDeferred, "kasb-test") {
		t.Fatal("Kasb deferred branch not preserved")
	}
}

func TestKasbRecoveryBeadsPinsKasbDatabase(t *testing.T) {
	town, _ := filepath.EvalSymlinks(t.TempDir())
	root := filepath.Join(town, "kasb_core", "mayor", "rig")
	db := filepath.Join(town, "kasb_core", ".beads")
	bin := filepath.Join(town, "bin")
	for _, dir := range []string{root, db, bin} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(db, "metadata.json"), []byte(`{"dolt_database":"kasb_fixture","dolt_server_host":"127.0.0.1","dolt_server_port":45454}`), 0600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
[ "$PWD" = "$KASB_RECOVERY_TEST_ROOT" ] || exit 90
[ "$BEADS_DIR" = "$KASB_RECOVERY_TEST_DB" ] || exit 91
[ "$BEADS_DOLT_SERVER_DATABASE" = kasb_fixture ] || exit 92
case "$1" in
 --allow-stale) echo 'unknown flag: --allow-stale'; exit 1 ;;
 list|query) echo '[]';;
 *) exit 93;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "bd"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("KASB_RECOVERY_TEST_ROOT", root)
	t.Setenv("KASB_RECOVERY_TEST_DB", db)
	t.Setenv("BEADS_DIR", filepath.Join(town, "foreign"))
	t.Setenv("GT_DOLT_PORT", "45454")
	beads.ResetBdAllowStaleCacheForTest()
	t.Cleanup(beads.ResetBdAllowStaleCacheForTest)
	if _, err := mqResumeBeads(town, "kasb_core").List(beads.ListOptions{Status: "blocked", Priority: -1}); err != nil {
		t.Fatal(err)
	}
}

func TestKasbResubmissionChangesOnlyOriginalMRHead(t *testing.T) {
	mr, _, _ := kasbRecoveryFixture()
	before := beads.ParseMRFields(mr)
	mr.Description += "\noperator_note: preserve this\n"
	updated, err := kasbResubmittedDescription(mr, strings.Repeat("c", 40))
	if err != nil {
		t.Fatal(err)
	}
	copy := *mr
	copy.Description = updated
	after := beads.ParseMRFields(&copy)
	if after.CommitSHA != strings.Repeat("c", 40) || before.Branch != after.Branch || before.SourceIssue != after.SourceIssue || !strings.Contains(updated, "operator_note: preserve this") {
		t.Fatal(updated)
	}
	mr.Description += "commit-sha: " + strings.Repeat("b", 40)
	if _, err := kasbResubmittedDescription(mr, strings.Repeat("c", 40)); err == nil {
		t.Fatal("ambiguous head accepted")
	}
}
