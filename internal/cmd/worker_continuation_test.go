package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

func TestContinuationPreservesHoldsAndOtherRigs(t *testing.T) {
	for _, tc := range []struct {
		name, id, status, label, metadata string
		allowed                           bool
	}{
		{"unfinished", "kasb-task", "hooked", "", "", true},
		{"in progress", "kasb-task", "in_progress", "", "{}", true},
		{"approved audit task", "kasb-task", "hooked", "", `{"audit_bead":"kasb-4qr4","dispatch_authorization_bead":"kasb-252nt","dispatch_authorized_at":"2026-11-07T18:28:53Z","dispatch_authorized_by":"Greg Carter","planning_key":"E8.3","planning_request":"greg:2026-11-05-codegraph-quality","review_base":"fff904a9"}`, true},
		{"design provenance", "kasb-task", "hooked", "", `{"plan_key":"f12","source_directory":"/design/input","readme_sha256":"abc","screen_sha256":"def","quality_review":"kasb-review","quality_review_base":"123","performance_rank":2,"performance_review":"kasb-performance"}`, true},
		{"audit plus hold", "kasb-task", "hooked", "", `{"audit_bead":"kasb-audit","holdUntil":"2000-01-01T00:00:00Z"}`, false},
		{"audit plus handoff", "kasb-task", "hooked", "", `{"audit_bead":"kasb-audit","handoff":{"lastStep":"DEFERRED"}}`, false},
		{"audit plus run wait", "kasb-task", "hooked", "", `{"planning_key":"E8.3","run_wait":{"run":123}}`, false},
		{"audit plus held label", "kasb-task", "hooked", "dispatch:hold", `{"audit_bead":"kasb-audit"}`, false},
		{"nested audit value", "kasb-task", "hooked", "", `{"audit_bead":{"holdUntil":"2000-01-01T00:00:00Z"}}`, false},
		{"null audit value", "kasb-task", "hooked", "", `{"audit_bead":null}`, false},
		{"invalid rank", "kasb-task", "hooked", "", `{"performance_rank":-1}`, false},
		{"object rank", "kasb-task", "hooked", "", `{"performance_rank":{"holdUntil":"2000-01-01T00:00:00Z"}}`, false},
		{"null rank", "kasb-task", "hooked", "", `{"performance_rank":null}`, false},
		{"malformed metadata", "kasb-task", "hooked", "", `{"audit_bead":`, false},
		{"metadata array", "kasb-task", "hooked", "", `[]`, false},
		{"metadata scalar", "kasb-task", "hooked", "", `true`, false},
		{"terminal", "kasb-task", "closed", "", "", false},
		{"blocked", "kasb-task", "blocked", "", "", false},
		{"other rig", "hisn-task", "hooked", "", "", false},
		{"human", "kasb-task", "hooked", "needs-operator", "", false},
		{"rollout", "kasb-task", "hooked", "needs-operator-rollout", "", false},
		{"review", "kasb-task", "hooked", "needs-review", "", false},
		{"dispatch hold", "kasb-task", "hooked", "dispatch:hold", "", false},
		{"live", "kasb-task", "hooked", "live-validation", "", false},
		{"submitted", "kasb-task", "hooked", "awaiting-merge:kasb-wisp-mr", "", false},
		{"expired hold", "kasb-task", "hooked", "", "{\"holdUntil\":\"2000-01-01T00:00:00Z\"}", false},
		{"unknown metadata", "kasb-task", "hooked", "", "{\"unknown\":true}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &beads.Issue{ID: tc.id, Status: tc.status, Labels: []string{tc.label}, Metadata: []byte(tc.metadata)}
			if got := continuationSourceAllowed(s); got != tc.allowed {
				t.Fatalf("allowed=%v", got)
			}
		})
	}
	if continuationSourceAllowed(nil) {
		t.Fatal("nil source allowed")
	}
}

func TestContinuationPreservesBlockedAndOpenMRs(t *testing.T) {
	for _, status := range []string{"open", "blocked", "in_progress", "unknown", "closed"} {
		mr := &beads.Issue{Status: status, Description: beads.FormatMRFields(&beads.MRFields{SourceIssue: "kasb-task", Branch: "polecat/worker/kasb-task", Rig: "kasb_core", Target: "main"})}
		if continuationMRPending("kasb-task", []*beads.Issue{mr}) != (status != "closed") {
			t.Fatalf("status=%s", status)
		}
		if continuationMRPending("kasb-other", []*beads.Issue{mr}) {
			t.Fatal("wrong source matched")
		}
	}
}

func TestContinuationAttemptsOncePerLaunchIncludingDeliveryFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "delivered", true: "failed"}[failed], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "worker.json")
			e := waitingEvidence{Source: "kasb-task", Launched: time.Now().UTC(), Completed: time.Now().UTC(), Continuation: true}
			calls := 0
			deliver := func() error {
				calls++
				if failed {
					return errors.New("transport failed")
				}
				return nil
			}
			err := continueWorkerOnce(path, e, deliver)
			if (err != nil) != failed {
				t.Fatalf("delivery err=%v", err)
			}
			e.Completed = e.Completed.Add(time.Hour) // another final must not create a loop
			if continueWorkerOnce(path, e, deliver) == nil {
				t.Fatal("second attempt accepted")
			}
			if calls != 1 {
				t.Fatalf("deliveries=%d", calls)
			}
			i, err := os.Stat(path)
			if err != nil || i.Mode().Perm() != 0600 {
				t.Fatalf("private stamp: %v %v", i, err)
			}
			e.Launched = e.Launched.Add(time.Hour)
			_ = continueWorkerOnce(path, e, deliver)
			if calls != 2 {
				t.Fatal("new launch was not considered independently")
			}
		})
	}
}

func TestContinuationFailsClosedOnMissingEvidenceOrCorruptStamp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.json")
	calls := 0
	deliver := func() error { calls++; return nil }
	if continueWorkerOnce(path, waitingEvidence{}, deliver) == nil {
		t.Fatal("empty evidence accepted")
	}
	if err := os.WriteFile(path, []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	e := waitingEvidence{Source: "kasb-task", Launched: time.Now(), Continuation: true}
	if continueWorkerOnce(path, e, deliver) == nil {
		t.Fatal("corrupt stamp accepted")
	}
	if calls != 0 {
		t.Fatal("delivered without evidence")
	}
}

func TestInspectUnfinishedTurnCausalBoundary(t *testing.T) {
	town := t.TempDir()
	root := filepath.Join(town, "kasb_core", "polecats", "worker", "kasb_core")
	home := filepath.Join(town, ".runtime", "codex-homes", "kasb_core", "polecats", "worker")
	receipt := filepath.Join(root, ".delivery", "preflight-receipts", "receipt.json")
	for _, p := range []string{filepath.Dir(receipt), filepath.Join(home, "sessions")} {
		if err := os.MkdirAll(p, 0755); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	launch := map[string]any{"Timestamp": now.Add(-time.Hour), "PID": 101, "Actor": "kasb_core/polecats/worker", "Role": "polecat", "Cwd": root}
	data, _ := json.Marshal(launch)
	if err := os.WriteFile(filepath.Join(home, "launch.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	final := fmt.Sprintf("{\"timestamp\":%q,\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\"}}\n", now.Add(-5*time.Minute).Format(time.RFC3339Nano))
	if err := os.WriteFile(filepath.Join(home, "sessions", "rollout.jsonl"), []byte(final), 0600); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	scripts := map[string]string{
		"tmux":   "case \"$*\" in *list-sessions*) printf 'kasb-worker|1|100|%s\\n' \"$TEST_ATTACHED\";; *pane_pid*) echo 101;; *) exit 1;; esac\n",
		"ps":     "printf '101 1 /opt/codex\\n'; if [ -n \"$TEST_CHILD\" ]; then printf '102 101 %s\\n' \"$TEST_CHILD\"; fi\n",
		"git":    "case \"$*\" in *branch*) echo 'polecat/worker/kasb-task+original';; *rev-parse*) echo aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;; *) exit 1;; esac\n",
		"docker": "printf '%s\\n' \"$TEST_CONTAINER\"\n",
	}
	for name, body := range scripts {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body), 0755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TEST_CHILD", "")
	t.Setenv("TEST_ATTACHED", "0")
	t.Setenv("TEST_CONTAINER", "")
	source := &beads.Issue{ID: "kasb-task", Assignee: "kasb_core/polecats/worker", Status: "hooked"}
	e, err := inspectWaitingWorker(town, source)
	if err != nil || !e.Continuation || e.Preflight != "" {
		t.Fatalf("unfinished turn missed: evidence=%+v err=%v", e, err)
	}
	for _, child := range []string{"python3", "node", "zsh", "unknown-tool"} {
		t.Setenv("TEST_CHILD", child)
		if _, err := inspectWaitingWorker(town, source); err == nil {
			t.Fatalf("active %s accepted", child)
		}
	}
	t.Setenv("TEST_CHILD", "")
	t.Setenv("TEST_ATTACHED", "1")
	if _, err := inspectWaitingWorker(town, source); err == nil {
		t.Fatal("operator-attached session accepted")
	}
	t.Setenv("TEST_ATTACHED", "0")
	t.Setenv("TEST_CONTAINER", "kasb-jest-live /test")
	if _, err := inspectWaitingWorker(town, source); err == nil {
		t.Fatal("live container accepted")
	}
	t.Setenv("TEST_CONTAINER", "")
	for _, status := range []string{"passed", "passed_non_behavioral", "in_progress", "failed", "failed_red_probe"} {
		r := map[string]any{"head_sha": e.Head, "source_bead_id": source.ID, "status": status, "finished_at": now.Add(-6 * time.Minute)}
		b, _ := json.Marshal(r)
		if err := os.WriteFile(receipt, b, 0600); err != nil {
			t.Fatal(err)
		}
		got, err := inspectWaitingWorker(town, source)
		if status == "failed" || status == "failed_red_probe" {
			if err != nil || got.Continuation || got.Preflight == "" {
				t.Fatalf("failed proof behavior changed: %s %v", status, err)
			}
		} else if err == nil {
			t.Fatalf("existing %s proof admitted", status)
		}
	}
	if err := os.WriteFile(receipt, []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectWaitingWorker(town, source); err == nil {
		t.Fatal("corrupt proof treated as absent")
	}
}
