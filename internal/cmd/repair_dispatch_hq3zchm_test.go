package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

// hq-3zchm: host back-pressure must defer, never trip the context breaker.
func TestClassifyDispatchFailure_DiskAndBusyAdmissionAreCapacity(t *testing.T) {
	for _, msg := range []string{
		"sling failed: failed to spawn polecat: allocating and creating polecat: insufficient disk space: CRITICAL: only 17.7 GB free (96.2% used)",
		"sling failed: failed to spawn polecat: polecat admission is busy; retry shortly",
	} {
		class := classifyDispatchFailure(errors.New(msg))
		if class != dispatchFailureCapacity {
			t.Errorf("%q classified %s, want capacity", msg, class)
		}
		if class.countsTowardCircuitBreaker() {
			t.Errorf("%q counts toward the circuit breaker", msg)
		}
	}
	if classifyDispatchFailure(errors.New("formula mol-x not found")) != dispatchFailureWork {
		t.Error("a genuine bead failure must still count")
	}
}

func TestKasbRecoveryCandidate_MissingCorrectionReasonStillQueuesRepair(t *testing.T) {
	mr, _, show := kasbRecoveryFixture()
	mr.Labels = []string{"gt:merge-request", "needs-repair"}
	state, err := kasbRecoveryCandidate(mr, strings.Repeat("b", 40), show)
	if err != nil {
		t.Fatalf("needs-repair without a correction reason was dropped: %v", err)
	}
	if state.Record.Kind != "same-mr-repair" || state.Record.RepairReason != kasbMissingCorrectionReason {
		t.Fatalf("kind=%q reason=%q", state.Record.Kind, state.Record.RepairReason)
	}
	if !strings.Contains(state.Record.Instructions, kasbMissingCorrectionReason) {
		t.Fatal("the worker's instructions do not carry the fallback reason")
	}

	// A recorded reason is kept verbatim.
	mr2, _, show2 := kasbRecoveryFixture()
	mr2.Labels = []string{"gt:merge-request", "needs-repair"}
	mr2.Metadata = json.RawMessage(`{"correction_reason":"async preflight failed: web-e2e"}`)
	state2, err := kasbRecoveryCandidate(mr2, strings.Repeat("b", 40), show2)
	if err != nil || state2.Record.RepairReason != "async preflight failed: web-e2e" {
		t.Fatalf("recorded reason not kept: %v %+v", err, state2)
	}
}

func TestValidateMQResumeWorker_IgnoresSourcesParkedBehindTheirOwnMR(t *testing.T) {
	mr, _, show := kasbRecoveryFixture()
	state, err := kasbRecoveryCandidate(mr, strings.Repeat("b", 40), show)
	if err != nil {
		t.Fatal(err)
	}
	town, _ := filepath.EvalSymlinks(t.TempDir())
	root := filepath.Join(town, "kasb_core", "polecats", "chrome", "kasb_core")
	if err := os.MkdirAll(root, 0o700); err != nil {
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
		return "", errors.New("unexpected git")
	}, running: func(string) (bool, error) { return false, nil }}

	// A second source waiting on its own MR is parked, not active work.
	ops.assignments = func(string) ([]*beads.Issue, error) {
		return []*beads.Issue{{ID: "kasb-other", Status: "hooked", Labels: []string{"awaiting-merge:kasb-wisp-other"}}}, nil
	}
	if err := validateMQResumeWorkerWith(town, state, ops); err != nil {
		t.Fatalf("worker with a second pending MR refused its repair: %v", err)
	}
	// Genuinely active other work still refuses.
	ops.assignments = func(string) ([]*beads.Issue, error) {
		return []*beads.Issue{{ID: "kasb-other", Status: "in_progress"}}, nil
	}
	if validateMQResumeWorkerWith(town, state, ops) == nil {
		t.Fatal("worker with active other work admitted")
	}
}

func restoreFixture(t *testing.T) (string, *beads.Issue, string, map[string]string, *[]string) {
	t.Helper()
	town, _ := filepath.EvalSymlinks(t.TempDir())
	root := filepath.Join(town, "kasb_core", "polecats", "dust", "kasb_core")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("c", 40)
	branch := "polecat/dust/kasb-qyar.121.11+mwoxjxo5"
	mr := &beads.Issue{ID: "kasb-wisp-tnyd9", Status: "blocked", Labels: []string{"gt:merge-request", "needs-repair"},
		Description: beads.FormatMRFields(&beads.MRFields{Rig: "kasb_core", Worker: "dust", SourceIssue: "kasb-qyar.121.11", Branch: branch, CommitSHA: sha, Target: "main"})}
	answers := map[string]string{
		"status --porcelain":                              "",
		"branch --show-current":                           "polecat/dust/kasb-4wnlx.1+mwq46e2f",
		"ls-remote --refs origin refs/heads/" + branch:    sha + "\trefs/heads/" + branch,
		"rev-parse --verify --quiet refs/heads/" + branch: "",
		"fetch origin " + branch:                          "",
		"checkout -B " + branch + " " + sha:               "",
	}
	var calls []string
	return town, mr, root, answers, &calls
}

func restoreOps(answers map[string]string, calls *[]string, running bool) mqResumeWorkerOps {
	return mqResumeWorkerOps{git: func(_ string, args ...string) (string, error) {
		key := strings.Join(args, " ")
		*calls = append(*calls, key)
		if v, ok := answers[key]; ok {
			return v, nil
		}
		return "", errors.New("unexpected git " + key)
	}, running: func(string) (bool, error) { return running, nil }}
}

func TestRestoreRepairWorkerBranch_SwitchesIdleCleanWorker(t *testing.T) {
	town, mr, _, answers, calls := restoreFixture(t)
	if err := restoreRepairWorkerBranch(town, "kasb_core", mr, restoreOps(answers, calls, false)); err != nil {
		t.Fatal(err)
	}
	last := (*calls)[len(*calls)-1]
	if !strings.HasPrefix(last, "checkout -B polecat/dust/kasb-qyar.121.11+mwoxjxo5 ") {
		t.Fatalf("worker not switched to the MR branch; last git call %q", last)
	}
}

func TestRestoreRepairWorkerBranch_RefusesWhenAnythingCouldBeLost(t *testing.T) {
	cases := map[string]func(answers map[string]string, mr *beads.Issue) bool{
		"live session":   func(map[string]string, *beads.Issue) bool { return true },
		"dirty worktree": func(a map[string]string, _ *beads.Issue) bool { a["status --porcelain"] = " M apps/x.py"; return false },
		"pushed head moved": func(a map[string]string, mr *beads.Issue) bool {
			a["ls-remote --refs origin refs/heads/polecat/dust/kasb-qyar.121.11+mwoxjxo5"] = strings.Repeat("d", 40) + "\trefs/heads/polecat/dust/kasb-qyar.121.11+mwoxjxo5"
			return false
		},
		"local branch ahead": func(a map[string]string, _ *beads.Issue) bool {
			a["rev-parse --verify --quiet refs/heads/polecat/dust/kasb-qyar.121.11+mwoxjxo5"] = strings.Repeat("e", 40)
			return false
		},
		"already on MR branch": func(a map[string]string, _ *beads.Issue) bool {
			a["branch --show-current"] = "polecat/dust/kasb-qyar.121.11+mwoxjxo5"
			return false
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			town, mr, _, answers, calls := restoreFixture(t)
			running := mutate(answers, mr)
			if err := restoreRepairWorkerBranch(town, "kasb_core", mr, restoreOps(answers, calls, running)); err == nil {
				t.Fatal("restore went ahead")
			}
			for _, c := range *calls {
				if strings.HasPrefix(c, "checkout") {
					t.Fatalf("checkout ran: %q", c)
				}
			}
		})
	}
}

func TestShouldReportRetainedRepair_OncePerReasonPerHour(t *testing.T) {
	town := t.TempDir()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	if !shouldReportRetainedRepair(town, "kasb-wisp-a", "reason one", now) {
		t.Fatal("first report suppressed")
	}
	if shouldReportRetainedRepair(town, "kasb-wisp-a", "reason one", now.Add(10*time.Minute)) {
		t.Fatal("repeat inside the hour not suppressed")
	}
	if !shouldReportRetainedRepair(town, "kasb-wisp-a", "reason two", now.Add(11*time.Minute)) {
		t.Fatal("a changed reason was suppressed")
	}
	if !shouldReportRetainedRepair(town, "kasb-wisp-a", "reason two", now.Add(72*time.Minute)) {
		t.Fatal("report after an hour suppressed")
	}
}
