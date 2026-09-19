package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

func mqResumeFixture(t *testing.T) (map[string]*beads.Issue, func(string) (*beads.Issue, error)) {
	t.Helper()
	r := mqResumeRecord{SchemaVersion: 1, Kind: "same-mr-rebase", Rig: "hisn", MR: "hisn-wisp-test", Source: "hisn-test.1", Worker: "quartz", Branch: "polecat/quartz/hisn-test.1+test", Submitted: strings.Repeat("a", 40), Base: strings.Repeat("b", 40), CreatedAt: "2026-09-19T04:00:00.000Z"}
	mr := &beads.Issue{ID: r.MR, Status: "blocked", Labels: []string{"gt:merge-request", "needs-rebase"}, Description: beads.FormatMRFields(&beads.MRFields{Rig: r.Rig, Worker: r.Worker, SourceIssue: r.Source, Branch: r.Branch, CommitSHA: r.Submitted, Target: "main"})}
	r.DescriptionSHA256 = mqResumeHash(mr.Description)
	r.Instructions = fmt.Sprintf("Original worker hisn/%s: recover source %s on %s; resolve/rebase onto %s, commit as Greg Carter <email@gregorycarter.net>, run and publish renewed exact-HEAD preflight, push only this worker branch with an explicit expected-old-head lease, run node tools/delivery/resubmit.mjs %s --apply, then gt done --pre-verified. Preserve this MR and source; do not create conflict/verifier tasks or close the source.", r.Worker, r.Source, r.Branch, r.Base, r.MR)
	mr.Metadata, _ = json.Marshal(map[string]any{"delivery_recovery": r})
	source := &beads.Issue{ID: r.Source, Status: "in_progress", Assignee: "hisn/polecats/quartz", Labels: []string{"awaiting-merge:" + r.MR}, Description: "attached_molecule: hisn-wisp-molecule\n", Dependencies: []beads.IssueDep{{ID: "hisn-wisp-molecule", DependencyType: "blocks"}, {ID: "hisn-parent", DependencyType: "parent-child"}}}
	issues := map[string]*beads.Issue{mr.ID: mr, source.ID: source, "hisn-parent": {ID: "hisn-parent", Status: "in_progress", Dependencies: []beads.IssueDep{{ID: "hisn-gate", DependencyType: "blocks"}}}, "hisn-gate": {ID: "hisn-gate", Status: "closed"}, "hisn-wisp-molecule": {ID: "hisn-wisp-molecule", Status: "open"}}
	return issues, func(id string) (*beads.Issue, error) {
		issue := issues[id]
		if issue == nil {
			return nil, errors.New("not found")
		}
		return issue, nil
	}
}

func TestMQResumeRecordsPreserveExistingWork(t *testing.T) {
	issues, show := mqResumeFixture(t)
	before, _ := json.Marshal(issues)
	state, err := validateMQResumeRecords("hisn", "hisn-wisp-test", show)
	if err != nil {
		t.Fatal(err)
	}
	if state.Record.Worker != "quartz" || state.Record.Source != "hisn-test.1" {
		t.Fatalf("wrong original work: %+v", state)
	}
	after, _ := json.Marshal(issues)
	if string(before) != string(after) {
		t.Fatal("validation mutated existing work")
	}
}

func TestMQResumeRecordsRefuseUnsafeIdentity(t *testing.T) {
	cases := map[string]func(map[string]*beads.Issue){
		"closed MR": func(m map[string]*beads.Issue) { m["hisn-wisp-test"].Status = "closed" },
		"open MR":   func(m map[string]*beads.Issue) { m["hisn-wisp-test"].Status = "open" },
		"held MR": func(m map[string]*beads.Issue) {
			m["hisn-wisp-test"].Labels = append(m["hisn-wisp-test"].Labels, "needs-operator")
		},
		"missing receipt":   func(m map[string]*beads.Issue) { m["hisn-wisp-test"].Metadata = nil },
		"stale description": func(m map[string]*beads.Issue) { m["hisn-wisp-test"].Description += "changed\n" },
		"duplicate head alias": func(m map[string]*beads.Issue) {
			m["hisn-wisp-test"].Description += "\ncommit-sha: " + strings.Repeat("a", 40)
		},
		"foreign rig": func(m map[string]*beads.Issue) {
			m["hisn-wisp-test"].Description = strings.ReplaceAll(m["hisn-wisp-test"].Description, "rig: hisn", "rig: bridge_town_core")
		},
		"closed source":  func(m map[string]*beads.Issue) { m["hisn-test.1"].Status = "closed" },
		"blocked source": func(m map[string]*beads.Issue) { m["hisn-test.1"].Status = "blocked" },
		"held source": func(m map[string]*beads.Issue) {
			m["hisn-test.1"].Labels = append(m["hisn-test.1"].Labels, "needs-operator-rollout")
		},
		"reassigned source": func(m map[string]*beads.Issue) { m["hisn-test.1"].Assignee = "hisn/polecats/obsidian" },
		"other MR": func(m map[string]*beads.Issue) {
			m["hisn-test.1"].Labels = append(m["hisn-test.1"].Labels, "awaiting-merge:hisn-wisp-other")
		},
		"inherited prerequisite": func(m map[string]*beads.Issue) { m["hisn-gate"].Status = "open" },
		"missing prerequisite":   func(m map[string]*beads.Issue) { delete(m, "hisn-gate") },
		"held parent":            func(m map[string]*beads.Issue) { m["hisn-parent"].Labels = []string{"needs-operator"} },
		"parent cycle": func(m map[string]*beads.Issue) {
			m["hisn-parent"].Dependencies = append(m["hisn-parent"].Dependencies, beads.IssueDep{ID: "hisn-test.1", DependencyType: "parent-child"})
		},
		"unrelated molecule": func(m map[string]*beads.Issue) { m["hisn-test.1"].Description = "attached_molecule: hisn-wisp-other\n" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			issues, show := mqResumeFixture(t)
			mutate(issues)
			if _, err := validateMQResumeRecords("hisn", "hisn-wisp-test", show); err == nil {
				t.Fatal("unsafe resume accepted")
			}
		})
	}
	_, show := mqResumeFixture(t)
	if _, err := validateMQResumeRecords("bridge_town_core", "hisn-wisp-test", show); err == nil {
		t.Fatal("foreign rig accepted")
	}
}

func TestMQResumeWorkerExactState(t *testing.T) {
	for _, problem := range []string{"", "branch", "head", "dirty", "origin", "remote", "assigned", "running", "unknown-session", "missing", "redirected"} {
		t.Run(problem, func(t *testing.T) {
			_, show := mqResumeFixture(t)
			state, err := validateMQResumeRecords("hisn", "hisn-wisp-test", show)
			if err != nil {
				t.Fatal(err)
			}
			town, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(town, "hisn", "polecats", "quartz", "hisn")
			if err := os.MkdirAll(root, 0755); err != nil {
				t.Fatal(err)
			}
			ops := mqResumeWorkerOps{
				git: func(path string, args ...string) (string, error) {
					if path != root {
						t.Fatalf("wrong worktree %s", path)
					}
					switch strings.Join(args, " ") {
					case "rev-parse --show-toplevel":
						return root, nil
					case "branch --show-current":
						if problem == "branch" {
							return "main", nil
						}
						return state.Record.Branch, nil
					case "rev-parse HEAD":
						if problem == "head" {
							return strings.Repeat("c", 40), nil
						}
						return state.Record.Submitted, nil
					case "status --porcelain":
						if problem == "dirty" {
							return " M source.ts", nil
						}
						return "", nil
					case "remote get-url origin":
						if problem == "origin" {
							return "https://example.invalid/repo", nil
						}
						return "https://github.com/gregorycarter/hisn-core.git", nil
					default:
						if args[0] != "ls-remote" {
							t.Fatalf("unexpected Git command %v", args)
						}
						if problem == "remote" {
							return "", nil
						}
						return state.Record.Submitted + "\trefs/heads/" + state.Record.Branch, nil
					}
				},
				assignments: func(actor string) ([]*beads.Issue, error) {
					if actor != "hisn/polecats/quartz" {
						t.Fatal(actor)
					}
					if problem == "assigned" {
						return []*beads.Issue{{ID: "hisn-other", Status: "hooked"}}, nil
					}
					return nil, nil
				},
				running: func(name string) (bool, error) {
					if name != "hisn-quartz" {
						t.Fatal(name)
					}
					if problem == "unknown-session" {
						return false, errors.New("unknown")
					}
					return problem == "running", nil
				},
			}
			if problem == "missing" {
				if err := os.Remove(root); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Dir(root)); err != nil {
					t.Fatal(err)
				}
			}
			if problem == "redirected" {
				if err := os.Remove(root); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), root); err != nil {
					t.Fatal(err)
				}
			}
			err = validateMQResumeWorkerWith(town, state, ops)
			if (err == nil) != (problem == "") {
				t.Fatalf("problem=%s err=%v", problem, err)
			}
		})
	}
}

func TestMQResumeConcurrentQueueIsIdempotent(t *testing.T) {
	issues, show := mqResumeFixture(t)
	state, err := validateMQResumeRecords("hisn", "hisn-wisp-test", show)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(issues)
	town := t.TempDir()
	var mu sync.Mutex
	var contexts []*beads.Issue
	createCount := 0
	list := func() ([]*beads.Issue, error) {
		mu.Lock()
		defer mu.Unlock()
		return append([]*beads.Issue{}, contexts...), nil
	}
	create := func(title, source string, fields *capacity.SlingContextFields) (*beads.Issue, error) {
		mu.Lock()
		defer mu.Unlock()
		createCount++
		ctx := &beads.Issue{ID: "hisn-wisp-queue", Description: beads.FormatSlingContextDescription(fields)}
		contexts = append(contexts, ctx)
		return ctx, nil
	}
	load := func() (*mqResumeState, error) { return state, nil }
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := queueMQResume(town, state, load, list, create)
			if err != nil && !strings.Contains(err.Error(), "already being slung") {
				t.Errorf("unexpected queue error: %v", err)
			}
		}()
	}
	wg.Wait()
	result, err := queueMQResume(town, state, load, list, create)
	if err != nil {
		t.Fatal(err)
	}
	if createCount != 1 || result.Status != "already-queued" || result.Context != "hisn-wisp-queue" {
		t.Fatalf("count=%d result=%+v", createCount, result)
	}
	after, _ := json.Marshal(issues)
	if string(before) != string(after) {
		t.Fatal("queue mutated original source/molecule/MR")
	}
	fields := beads.ParseSlingContextFields(contexts[0].Description)
	if fields.Formula != "" || fields.ResumeMR != state.MR.ID || fields.ResumeWorker != "quartz" || fields.ResumeHead != state.Record.Submitted {
		t.Fatalf("wrong context: %+v", fields)
	}
}

func TestMQResumeQueueRejectsChangedOrConflictingContext(t *testing.T) {
	_, show := mqResumeFixture(t)
	state, err := validateMQResumeRecords("hisn", "hisn-wisp-test", show)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"changed", "ordinary", "wrong head", "duplicates", "unconfirmed"} {
		t.Run(mode, func(t *testing.T) {
			copyState := *state
			if mode == "changed" {
				copyState.ReceiptHash = "changed"
			}
			fields := &capacity.SlingContextFields{Version: 1, WorkBeadID: state.Source.ID, TargetRig: "hisn", ResumeMR: state.MR.ID, ResumeWorker: "quartz", ResumeHead: state.Record.Submitted, ResumeBranch: state.Record.Branch, ResumeReceipt: state.ReceiptHash}
			if mode == "ordinary" {
				fields.ResumeMR = ""
			}
			if mode == "wrong head" {
				fields.ResumeHead = strings.Repeat("f", 40)
			}
			ctx := &beads.Issue{ID: "hisn-wisp-queue", Description: beads.FormatSlingContextDescription(fields)}
			list := func() ([]*beads.Issue, error) {
				if mode == "unconfirmed" {
					return nil, nil
				}
				if mode == "duplicates" {
					return []*beads.Issue{ctx, ctx}, nil
				}
				return []*beads.Issue{ctx}, nil
			}
			creates := 0
			_, err := queueMQResume(t.TempDir(), state, func() (*mqResumeState, error) { return &copyState, nil }, list, func(string, string, *capacity.SlingContextFields) (*beads.Issue, error) { creates++; return ctx, nil })
			if err == nil {
				t.Fatal("unsafe queue accepted")
			}
			if mode != "unconfirmed" && creates != 0 {
				t.Fatal("unexpected create")
			}
		})
	}
}

func TestMQResumeNeverManuallyStartsAroundPressure(t *testing.T) {
	t.Setenv("GT_DAEMON", "")
	_, err := dispatchMQResume(t.TempDir(), &capacity.SlingContextFields{ResumeMR: "hisn-wisp-test"})
	if err == nil || !strings.Contains(err.Error(), "pressure admission") {
		t.Fatalf("err=%v", err)
	}
}

func TestMQResumeSkippedByOrphanSweep(t *testing.T) {
	before := scheduledContextAssessment{fields: &capacity.SlingContextFields{ResumeMR: "hisn-wisp-test"}, blocked: true, blockers: []string{"hisn-wisp-molecule"}}
	after := before
	if n := sweepOrphanWispBlockers(t.TempDir(), []scheduledContextAssessment{after}, false); n != 0 {
		t.Fatal("recovery molecule swept")
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("recovery changed")
	}
}

func TestMQResumeDispatchPreservesAdmissionAndHookOrdering(t *testing.T) {
	for _, problem := range []string{"", "paused", "full", "pause-after-reservation", "stale-at-launch", "hook-failure", "startup-failure"} {
		t.Run(problem, func(t *testing.T) {
			t.Setenv("GT_DAEMON", "1")
			town := t.TempDir()
			_, show := mqResumeFixture(t)
			state, err := validateMQResumeRecords("hisn", "hisn-wisp-test", show)
			if err != nil {
				t.Fatal(err)
			}
			state.WorkDir = filepath.Join(town, "hisn", "polecats", "quartz", "hisn")
			fields := &capacity.SlingContextFields{Version: 1, WorkBeadID: state.Source.ID, TargetRig: "hisn", ResumeMR: state.MR.ID, ResumeWorker: "quartz", ResumeBranch: state.Record.Branch, ResumeHead: state.Record.Submitted, ResumeReceipt: state.ReceiptHash}
			if problem == "paused" {
				if err := capacity.SaveState(town, &capacity.SchedulerState{Paused: true}); err != nil {
					t.Fatal(err)
				}
			}
			loads, admitted, hooks, launched := 0, 0, 0, 0
			reservation := filepath.Join(town, "test-owned-reservation")
			ops := mqResumeDispatchOps{
				load: func(root string, got *capacity.SlingContextFields) (*mqResumeState, error) {
					loads++
					if root != town || got != fields {
						t.Fatal("wrong recovery")
					}
					if problem == "stale-at-launch" && loads > 1 {
						return nil, errors.New("stale receipt")
					}
					return state, nil
				},
				admit: func(root, rig, bead, operation string) (*polecatAdmissionHandle, polecatCapacitySnapshot, error) {
					admitted++
					if root != town || rig != "hisn" || bead != state.Source.ID || operation != "same-mr-resume" {
						t.Fatal("wrong admission identity")
					}
					if problem == "full" {
						return nil, polecatCapacitySnapshot{}, &polecatCapacityAdmissionError{Reason: "full"}
					}
					if err := os.WriteFile(reservation, []byte("synthetic owned reservation"), 0600); err != nil {
						t.Fatal(err)
					}
					if problem == "pause-after-reservation" {
						if err := capacity.SaveState(town, &capacity.SchedulerState{Paused: true}); err != nil {
							t.Fatal(err)
						}
					}
					return &polecatAdmissionHandle{path: reservation}, polecatCapacitySnapshot{}, nil
				},
				hook: func(got *mqResumeState) error {
					hooks++
					if got != state {
						t.Fatal("different source")
					}
					if problem == "hook-failure" {
						return errors.New("hook write failed")
					}
					return nil
				},
				start: func(worker string, opts polecat.SessionStartOptions) error {
					if worker != "quartz" || opts.WorkDir != state.WorkDir || opts.Issue != state.Source.ID || opts.PreserveBranch != fields.ResumeBranch || opts.PreserveHead != fields.ResumeHead || opts.StartupInstructions != state.Record.Instructions {
						t.Fatal("worker branch/worktree changed")
					}
					if _, err := os.Stat(reservation); err != nil {
						t.Fatal("start lacks reservation")
					}
					if err := opts.BeforeLaunch(); err != nil {
						return err
					}
					if problem == "startup-failure" {
						return errors.New("runtime startup failed")
					}
					launched++
					return nil
				},
			}
			result, err := dispatchMQResumeWith(town, fields, ops)
			if (err == nil) != (problem == "") {
				t.Fatalf("problem=%s err=%v result=%+v", problem, err, result)
			}
			if problem == "" && (result.PolecatName != "quartz" || result.BeadID != state.Source.ID || launched != 1 || admitted != 1 || hooks != 1) {
				t.Fatalf("incorrect success %#v", result)
			}
			if problem != "" && launched != 0 {
				t.Fatal("refused recovery launched")
			}
			if problem == "paused" && (loads != 0 || admitted != 0 || hooks != 0) {
				t.Fatal("paused recovery attempted admission")
			}
			if _, err := os.Stat(reservation); !os.IsNotExist(err) {
				t.Fatal("reservation not released")
			}
			unlock, err := tryAcquireSlingBeadLock(town, state.Source.ID)
			if err != nil {
				t.Fatal("source lock leaked")
			}
			unlock()
		})
	}
}

func TestMQResumeHookWriteMustBeConfirmed(t *testing.T) {
	_, show := mqResumeFixture(t)
	source, _ := show("hisn-test.1")
	for _, problem := range []string{"", "wrong source", "wrong assignee", "wrong status", "workflow lost", "labels lost", "metadata changed", "missing"} {
		t.Run(problem, func(t *testing.T) {
			hooked := *source
			hooked.Status = "hooked"
			hooked.Assignee = "hisn/polecats/quartz"
			result := &hooked
			switch problem {
			case "wrong source":
				hooked.ID = "hisn-other"
			case "wrong assignee":
				hooked.Assignee = "hisn/polecats/obsidian"
			case "wrong status":
				hooked.Status = "closed"
			case "workflow lost":
				hooked.Description = ""
			case "labels lost":
				hooked.Labels = nil
			case "metadata changed":
				hooked.Metadata = json.RawMessage(`{"changed":true}`)
			case "missing":
				result = nil
			}
			err := verifyMQResumeHook(source, result, "hisn/polecats/quartz")
			if (err == nil) != (problem == "") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestMQResumeOutputContractAndCallerIdentity(t *testing.T) {
	_, show := mqResumeFixture(t)
	state, err := validateMQResumeRecords("hisn", "hisn-wisp-test", show)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"ready-to-queue", "queued", "already-queued"} {
		ctx := ""
		if status != "ready-to-queue" {
			ctx = "hisn-wisp-context"
		}
		out, err := json.Marshal(mqResumeOutput(state, status, ctx))
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]string
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatal(err)
		}
		if got["worker"] != "quartz" || got["rig"] != "hisn" || got["mr"] != state.MR.ID || got["source"] != state.Source.ID || got["head"] != state.Record.Submitted || got["context"] != ctx || got["status"] != status {
			t.Fatalf("wrong JSON contract %s", out)
		}
	}
	for _, args := range [][]string{{"", "", ""}, {"hisn/refinery", "hisn", "hisn/refinery"}, {"mayor", "", "mayor/"}} {
		if err := validateMQResumeCaller(args[0], args[1], args[2]); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"bridge_town_core/refinery", "bridge_town_core", "bridge_town_core/refinery"}, {"hisn/polecats/obsidian", "hisn", "hisn/polecats/obsidian"}, {"hisn/refinery", "hisn", "other"}, {"hisn/refinery", "bridge_town_core", "hisn/refinery"}} {
		if err := validateMQResumeCaller(args[0], args[1], args[2]); err == nil {
			t.Fatalf("foreign role accepted %v", args)
		}
	}
}

func TestMQResumeContextOnlyAllowsSchedulerBookkeepingChanges(t *testing.T) {
	expected := &capacity.SlingContextFields{Version: 1, WorkBeadID: "hisn-test.1", TargetRig: "hisn", ResumeMR: "hisn-wisp-test", ResumeWorker: "quartz", ResumeBranch: "polecat/quartz/hisn-test.1+test", ResumeHead: strings.Repeat("a", 40), ResumeReceipt: strings.Repeat("c", 64)}
	actual := *expected
	actual.EnqueuedAt = "later"
	actual.DispatchFailures = 1
	actual.LastFailure = "test failure"
	actual.LastFailureAt = "later"
	if !sameMQResumeContext(&actual, expected) {
		t.Fatal("scheduler bookkeeping invalidated immutable identity")
	}
	for name, mutate := range map[string]func(*capacity.SlingContextFields){"version": func(f *capacity.SlingContextFields) { f.Version = 2 }, "worker": func(f *capacity.SlingContextFields) { f.ResumeWorker = "obsidian" }, "formula": func(f *capacity.SlingContextFields) { f.Formula = "mol-polecat-work" }, "base": func(f *capacity.SlingContextFields) { f.BaseBranch = "main" }, "args": func(f *capacity.SlingContextFields) { f.Args = "new work" }, "agent": func(f *capacity.SlingContextFields) { f.Agent = "other" }, "no merge": func(f *capacity.SlingContextFields) { f.NoMerge = true }, "raw bead": func(f *capacity.SlingContextFields) { f.HookRawBead = true }} {
		t.Run(name, func(t *testing.T) {
			changed := *expected
			mutate(&changed)
			if sameMQResumeContext(&changed, expected) {
				t.Fatal("foreign sling fields accepted")
			}
		})
	}
}
