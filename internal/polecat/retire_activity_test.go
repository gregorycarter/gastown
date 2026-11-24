package polecat

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/asyncpreflight"
)

// kasb-84hoc: retire marked sandboxes eligible while an async preflight (or
// its pytest children) ran in them, or while a BLOCKED MR still pointed the
// work back at the same worker.
func TestRetirementRetainsSandboxInUse(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *retirementFixture)
		reason string
	}{
		{"live process in worktree", func(t *testing.T, f *retirementFixture) {
			f.model["openFile"] = filepath.Join(f.clone, "apps", "api", "tests", "conftest.py")
		}, "live_process_in_worktree"},
		{"process inventory unavailable", func(t *testing.T, f *retirementFixture) {
			f.model["openFileError"] = true
		}, "worktree_activity=unknown"},
		{"live async preflight marker", func(t *testing.T, f *retirementFixture) {
			now := time.Now().UTC()
			rigPath := filepath.Dir(filepath.Dir(filepath.Dir(f.clone)))
			path, err := asyncpreflight.MarkerPath(rigPath, "gt-task", strings.Repeat("a", 40))
			if err != nil {
				t.Fatal(err)
			}
			m := &asyncpreflight.Marker{SchemaVersion: asyncpreflight.SchemaVersion, Source: "gt-task", Head: strings.Repeat("a", 40),
				Worktree: f.clone, Status: asyncpreflight.StatusRunning, PID: os.Getpid(), PIDStart: asyncpreflight.ProcessStartTime(os.Getpid()),
				CreatedAt: now.Format(time.RFC3339Nano), StartedAt: now.Format(time.RFC3339Nano), HeartbeatAt: now.Format(time.RFC3339Nano)}
			if m.PIDStart == "" {
				t.Skip("process start time unavailable on this platform")
			}
			if err := asyncpreflight.Save(path, m); err != nil {
				t.Fatal(err)
			}
		}, "async_preflight=running"},
		{"blocked MR for the source on another branch of this worker", func(t *testing.T, f *retirementFixture) {
			f.model["mrs"] = []any{map[string]any{"id": "gt-mr", "status": "blocked", "issue_type": "merge-request", "labels": []string{"gt:merge-request"},
				"description": "branch: polecat/toast/gt-task+new\ntarget: main\nsource_issue: gt-task\n"}}
		}, "pending_mr=gt-mr status=blocked"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRetirementFixture(t)
			tc.change(t, f)
			f.save(t)
			r, err := f.manager.RetireMerged("toast", false)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if r.Status != "retained" || !strings.Contains(r.Reason, tc.reason) {
				t.Fatalf("retirement = %+v, want retained with %q", r, tc.reason)
			}
			if _, err := os.Stat(f.clone); err != nil {
				t.Fatal("sandbox in use was removed")
			}
		})
	}
}

func TestRetirementIgnoresAnotherWorkersMRForTheSource(t *testing.T) {
	f := newRetirementFixture(t)
	f.model["mrs"] = []any{map[string]any{"id": "gt-mr", "status": "open", "issue_type": "merge-request", "labels": []string{"gt:merge-request"},
		"description": "branch: polecat/nux/gt-task+redo\ntarget: main\nsource_issue: gt-task\n"}}
	f.save(t)
	r, err := f.manager.RetireMerged("toast", true)
	if err != nil || r.Status != "eligible" {
		t.Fatalf("a re-dispatch to another worker must not pin this sandbox: %+v %v", r, err)
	}
}
