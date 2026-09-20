package cmd

import (
	"errors"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDependencyResumeReadiness(t *testing.T) {
	for _, scenario := range []string{"ready", "open-source", "still-blocked", "held", "parent-held", "manual-hold", "reassigned", "awaiting-mr", "unknown-relation", "unknown-dependency"} {
		t.Run(scenario, func(t *testing.T) {
			source := &beads.Issue{ID: "hisn-test", Status: "blocked", Assignee: "hisn/polecats/flint", Dependencies: []beads.IssueDep{{ID: "hisn-prereq", Status: "closed", DependencyType: "blocks"}}}
			deps := map[string]*beads.Issue{"hisn-prereq": {ID: "hisn-prereq", Status: "closed"}, "hisn-parent": {ID: "hisn-parent", Status: "open", Labels: []string{"needs-operator"}}}
			switch scenario {
			case "open-source":
				source.Status = "open"
			case "still-blocked":
				deps["hisn-prereq"].Status = "open"
			case "held":
				source.Labels = []string{"needs-operator"}
			case "parent-held":
				source.Dependencies = append(source.Dependencies, beads.IssueDep{ID: "hisn-parent", DependencyType: "parent-child"})
			case "manual-hold":
				source.Dependencies = nil
			case "reassigned":
				source.Assignee = "mayor/"
			case "awaiting-mr":
				source.Labels = []string{"awaiting-merge:hisn-wisp-mr"}
			case "unknown-relation":
				source.Dependencies = append(source.Dependencies, beads.IssueDep{ID: "hisn-other", DependencyType: ""})
			case "unknown-dependency":
				delete(deps, "hisn-prereq")
			}
			worker, err := dependencyResumeSource(source, func(id string) (*beads.Issue, error) { return deps[id], nil })
			good := scenario == "ready" || scenario == "open-source"
			if (err == nil) != good || good && worker != "flint" {
				t.Fatal(worker, err)
			}
		})
	}
}
func TestDependencyResumePreservesUnpublishedHead(t *testing.T) {
	town, _ := filepath.EvalSymlinks(t.TempDir())
	root := filepath.Join(town, "hisn", "polecats", "flint", "hisn")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	state := &mqResumeState{Record: mqResumeRecord{Kind: "dependency-resume", Rig: "hisn", Worker: "flint", Source: "hisn-test", Branch: "polecat/flint/hisn-test+x", Submitted: strings.Repeat("a", 40)}}
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
			return "https://github.com/gregorycarter/hisn-core.git", nil
		}
		t.Fatalf("must not reset or require pushed recovery HEAD: %v", args)
		return "", nil
	}, assignments: func(string) ([]*beads.Issue, error) { return nil, nil }, running: func(string) (bool, error) { return false, nil }}
	if err := validateMQResumeWorkerWith(town, state, ops); err != nil {
		t.Fatal(err)
	}
	ops.running = func(string) (bool, error) { return false, errors.New("tmux unknown") }
	if validateMQResumeWorkerWith(town, state, ops) == nil {
		t.Fatal("unknown session admitted")
	}
}
func TestDependencyResumeDispatchCannotExceedFour(t *testing.T) {
	t.Setenv("GT_DAEMON", "1")
	town := mqResumeCapacityTown(t, 12, "hisn-a\nhisn-b\nhisn-c\nhisn-d\n")
	fields := &capacity.SlingContextFields{TargetRig: "hisn", WorkBeadID: "hisn-test", ResumeDependency: true, ResumeWorker: "flint"}
	started := false
	_, err := dispatchMQResumeWith(town, fields, mqResumeDispatchOps{load: func(string, *capacity.SlingContextFields) (*mqResumeState, error) { return &mqResumeState{}, nil }, admit: acquirePolecatAdmission, start: func(string, polecat.SessionStartOptions) error { started = true; return nil }})
	var denied *polecatCapacityAdmissionError
	if !errors.As(err, &denied) || started {
		t.Fatalf("capacity boundary: %v started=%v", err, started)
	}
}
