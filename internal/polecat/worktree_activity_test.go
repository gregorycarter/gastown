package polecat

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

func withProcessPaths(t *testing.T, paths []ProcessPath, err error) {
	t.Helper()
	old := ListProcessPaths
	ListProcessPaths = func() ([]ProcessPath, error) { return paths, err }
	t.Cleanup(func() { ListProcessPaths = old })
}

func TestParseLsofFields(t *testing.T) {
	out := []byte("p12773\ncpython3.12\nfcwd\nn/town/kasb_core/polecats/guzzle/kasb_core\nftxt\nn/usr/bin/python3\nf7\naw\nn/town/log\np1\nclaunchd\nfcwd\nn/\n")
	got := parseLsofFields(out)
	if len(got) != 4 || got[0].PID != 12773 || got[0].Command != "python3.12" || got[0].FD != "cwd" || got[0].Path != "/town/kasb_core/polecats/guzzle/kasb_core" ||
		got[2].FD != "7" || got[2].Access != "w" || got[3].PID != 1 || got[3].Command != "launchd" {
		t.Fatalf("parseLsofFields = %+v", got)
	}
}

func TestProcessPathUsesSandbox(t *testing.T) {
	for _, tc := range []struct {
		p    ProcessPath
		want bool
	}{
		{ProcessPath{FD: "cwd"}, true},
		{ProcessPath{FD: "txt"}, true},
		{ProcessPath{FD: "12", Access: "w"}, true},
		{ProcessPath{FD: "12", Access: "u"}, true},
		{ProcessPath{FD: ""}, true},
		// rig_tokens.py-style scanners only read a transcript for a moment.
		{ProcessPath{FD: "5", Access: "r"}, false},
	} {
		if got := tc.p.usesSandbox(); got != tc.want {
			t.Errorf("%+v.usesSandbox() = %v, want %v", tc.p, got, tc.want)
		}
	}
}

func TestWorktreeActivityBlocker(t *testing.T) {
	rig := t.TempDir()
	sandbox := filepath.Join(rig, "polecats", "shiny")
	if err := os.MkdirAll(filepath.Join(sandbox, "kasb_core"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Run("pytest child in worktree", func(t *testing.T) {
		withProcessPaths(t, []ProcessPath{
			{PID: 1, Command: "launchd", FD: "cwd", Path: "/"},
			{PID: 25597, Command: "pytest", FD: "cwd", Path: filepath.Join(sandbox, "kasb_core", "apps", "api")},
			{PID: 25598, Command: "pytest", FD: "cwd", Path: filepath.Join(sandbox+"-other", "kasb_core")},
			{PID: 66891, Command: "Python", FD: "4", Access: "r", Path: filepath.Join(sandbox, "kasb_core", ".codex", "session.jsonl")},
		}, nil)
		got := WorktreeActivityBlocker(rig, sandbox, time.Now())
		if !strings.Contains(got, "live_process_in_worktree") || !strings.Contains(got, "pid=25597(pytest)") || strings.Contains(got, "25598") || strings.Contains(got, "66891") {
			t.Fatalf("blocker = %q", got)
		}
	})
	t.Run("idle sandbox", func(t *testing.T) {
		withProcessPaths(t, []ProcessPath{{PID: 1, Command: "launchd", Path: "/"}}, nil)
		if got := WorktreeActivityBlocker(rig, sandbox, time.Now()); got != "" {
			t.Fatalf("idle sandbox blocked: %q", got)
		}
	})
	t.Run("own process is not activity", func(t *testing.T) {
		withProcessPaths(t, []ProcessPath{{PID: os.Getpid(), Command: "gt", Path: sandbox}}, nil)
		if got := WorktreeActivityBlocker(rig, sandbox, time.Now()); got != "" {
			t.Fatalf("own process blocked: %q", got)
		}
	})
	t.Run("detection error fails closed", func(t *testing.T) {
		withProcessPaths(t, nil, errors.New("lsof failed"))
		if got := WorktreeActivityBlocker(rig, sandbox, time.Now()); !strings.Contains(got, "worktree_activity=unknown") {
			t.Fatalf("blocker = %q", got)
		}
	})
}

type fakePendingMRFinder struct {
	branchMR  *beads.Issue
	sourceMRs []*beads.Issue
	err       error
}

func (f fakePendingMRFinder) FindMRForBranch(string) (*beads.Issue, error) { return f.branchMR, f.err }
func (f fakePendingMRFinder) FindPendingMRsForIssue(string) ([]*beads.Issue, error) {
	return f.sourceMRs, f.err
}

func TestPendingMRBlocker(t *testing.T) {
	const branch = "polecat/raider/kasb-0osbb.1+mwuantfa"
	mr := func(id, status, mrBranch string) *beads.Issue {
		return &beads.Issue{ID: id, Status: status, Description: "branch: " + mrBranch + "\ntarget: main\nsource_issue: kasb-0osbb.1\n"}
	}
	cases := []struct {
		name   string
		finder fakePendingMRFinder
		want   string
	}{
		{"blocked branch MR", fakePendingMRFinder{branchMR: mr("kasb-wisp-xb2pc", "blocked", branch)}, "pending_mr=kasb-wisp-xb2pc status=blocked"},
		{"closed branch MR", fakePendingMRFinder{branchMR: mr("kasb-wisp-old", "closed", branch)}, ""},
		{"blocked source MR on an older branch of this worker", fakePendingMRFinder{sourceMRs: []*beads.Issue{mr("kasb-wisp-a", "blocked", "polecat/raider/kasb-0osbb.1+older")}}, "pending_mr=kasb-wisp-a status=blocked"},
		{"source MR of another worker", fakePendingMRFinder{sourceMRs: []*beads.Issue{mr("kasb-wisp-b", "open", "polecat/nux/kasb-0osbb.1+redo")}}, ""},
		{"lookup error fails closed", fakePendingMRFinder{err: errors.New("dolt down")}, "lookup_error"},
		{"nothing pending", fakePendingMRFinder{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PendingMRBlocker(tc.finder, "raider", branch, "kasb-0osbb.1")
			if tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("PendingMRBlocker = %q, want %q", got, tc.want)
			}
		})
	}
}
