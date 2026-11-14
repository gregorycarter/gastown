package cmd

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gitpkg "github.com/steveyegge/gastown/internal/git"
)

// fakeRebaseGit lets us drive autoRebaseOnTarget without a real git repo for
// the gating-decision tests.
type fakeRebaseGit struct {
	rebaseErr   error
	rebaseCalls int
	abortCalls  int
}

func (f *fakeRebaseGit) Rebase(onto string) error {
	f.rebaseCalls++
	return f.rebaseErr
}

func (f *fakeRebaseGit) AbortRebase() error {
	f.abortCalls++
	return nil
}

// TestAutoRebaseOnTarget_GatingDecisions verifies the skip/rebase decision
// matrix (gh#3400). The behavior under test is the *decision*, not the actual
// git mechanics — those are exercised separately below against a real repo.
func TestAutoRebaseOnTarget_GatingDecisions(t *testing.T) {
	tests := []struct {
		name          string
		behind        int
		preVerified   bool
		alreadyPushed bool
		wantRebased   bool
		wantSkip      string
		wantCalls     int
	}{
		{
			name:        "not behind: no-op",
			behind:      0,
			wantRebased: false,
			wantSkip:    "",
			wantCalls:   0,
		},
		{
			name:        "behind by 1: rebase runs",
			behind:      1,
			wantRebased: true,
			wantSkip:    "",
			wantCalls:   1,
		},
		{
			name:        "behind by 5: rebase runs",
			behind:      5,
			wantRebased: true,
			wantSkip:    "",
			wantCalls:   1,
		},
		{
			name:        "pre-verified: skip even when behind",
			behind:      3,
			preVerified: true,
			wantRebased: false,
			wantSkip:    "--pre-verified is set",
			wantCalls:   0,
		},
		{
			name:          "already pushed: skip to avoid divergence",
			behind:        3,
			alreadyPushed: true,
			wantRebased:   false,
			wantSkip:      "prior push checkpoint exists",
			wantCalls:     0,
		},
		{
			name:          "pre-verified takes precedence over already-pushed",
			behind:        3,
			preVerified:   true,
			alreadyPushed: true,
			wantRebased:   false,
			wantSkip:      "--pre-verified is set",
			wantCalls:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeRebaseGit{}
			rebased, skipReason, err := autoRebaseOnTarget(fake, "origin/main", tt.behind, tt.preVerified, tt.alreadyPushed)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rebased != tt.wantRebased {
				t.Errorf("rebased = %v, want %v", rebased, tt.wantRebased)
			}
			if skipReason != tt.wantSkip {
				t.Errorf("skipReason = %q, want %q", skipReason, tt.wantSkip)
			}
			if fake.rebaseCalls != tt.wantCalls {
				t.Errorf("rebase calls = %d, want %d", fake.rebaseCalls, tt.wantCalls)
			}
			if fake.abortCalls != 0 {
				t.Errorf("abort calls = %d on success path, want 0", fake.abortCalls)
			}
		})
	}
}

// TestAutoRebaseOnTarget_ConflictAborts verifies that a rebase failure causes
// AbortRebase to fire and the returned error includes remediation guidance.
func TestAutoRebaseOnTarget_ConflictAborts(t *testing.T) {
	fake := &fakeRebaseGit{rebaseErr: errors.New("CONFLICT (content): merge conflict in foo.txt")}

	rebased, skipReason, err := autoRebaseOnTarget(fake, "origin/main", 1, false, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if rebased {
		t.Error("rebased should be false on conflict")
	}
	if skipReason != "" {
		t.Errorf("skipReason should be empty on conflict, got %q", skipReason)
	}
	if fake.rebaseCalls != 1 {
		t.Errorf("expected 1 rebase call, got %d", fake.rebaseCalls)
	}
	if fake.abortCalls != 1 {
		t.Errorf("expected AbortRebase to fire on conflict, got %d calls", fake.abortCalls)
	}
	// Remediation guidance is part of the contract — agents read this message.
	msg := err.Error()
	if !strings.Contains(msg, "auto-rebase onto origin/main failed") {
		t.Errorf("error missing context: %q", msg)
	}
	if !strings.Contains(msg, "git rebase origin/main") {
		t.Errorf("error missing remediation hint: %q", msg)
	}
	if !strings.Contains(msg, "rerun gt done") {
		t.Errorf("error missing rerun hint: %q", msg)
	}
}

// TestAutoRebaseOnTarget_RealRepoSuccess exercises the rebase against a real
// git working tree to confirm the wiring (Rebase call) actually replays the
// branch onto a moved base. (gh#3400, scenario (a) from the bead notes.)
func TestAutoRebaseOnTarget_RealRepoSuccess(t *testing.T) {
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	testRunGit(t, tmp, "init", "--initial-branch", "main", repo)
	testRunGit(t, repo, "config", "user.email", "test@test.com")
	testRunGit(t, repo, "config", "user.name", "Test")

	// Initial commit on main.
	writeRepoFile(t, repo, "README.md", "# initial\n")
	testRunGit(t, repo, "add", ".")
	testRunGit(t, repo, "commit", "-m", "initial")

	// Branch off and add a polecat commit on a non-conflicting file.
	testRunGit(t, repo, "checkout", "-b", "feature")
	writeRepoFile(t, repo, "feature.txt", "feature work\n")
	testRunGit(t, repo, "add", ".")
	testRunGit(t, repo, "commit", "-m", "feature")

	// Move main forward independently — also non-conflicting with feature.
	testRunGit(t, repo, "checkout", "main")
	writeRepoFile(t, repo, "main-new.txt", "new on main\n")
	testRunGit(t, repo, "add", ".")
	testRunGit(t, repo, "commit", "-m", "advance main")

	testRunGit(t, repo, "checkout", "feature")

	g := gitpkg.NewGit(repo)
	rebased, skipReason, err := autoRebaseOnTarget(g, "main", 1, false, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rebased {
		t.Fatalf("expected rebased=true, got false (skip=%q)", skipReason)
	}

	// After rebase, both files must be present and the feature commit must sit
	// on top of the advance-main commit.
	if _, statErr := os.Stat(filepath.Join(repo, "main-new.txt")); statErr != nil {
		t.Errorf("main-new.txt missing after rebase: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(repo, "feature.txt")); statErr != nil {
		t.Errorf("feature.txt missing after rebase: %v", statErr)
	}
}

// TestAutoRebaseOnTarget_RealRepoConflictAborts exercises the conflict path
// against a real git working tree: feature and main both touch the same file,
// rebase fails with a CONFLICT, and AbortRebase must restore the working tree
// so the polecat can address the conflict manually. (gh#3400, scenario (b).)
func TestAutoRebaseOnTarget_RealRepoConflictAborts(t *testing.T) {
	tmp := t.TempDir()
	repo := filepath.Join(tmp, "repo")
	testRunGit(t, tmp, "init", "--initial-branch", "main", repo)
	testRunGit(t, repo, "config", "user.email", "test@test.com")
	testRunGit(t, repo, "config", "user.name", "Test")

	writeRepoFile(t, repo, "shared.txt", "v0\n")
	testRunGit(t, repo, "add", ".")
	testRunGit(t, repo, "commit", "-m", "initial")

	// Feature changes shared.txt to v1.
	testRunGit(t, repo, "checkout", "-b", "feature")
	writeRepoFile(t, repo, "shared.txt", "v1-feature\n")
	testRunGit(t, repo, "add", ".")
	testRunGit(t, repo, "commit", "-m", "feature edit")

	// Main changes shared.txt to a different value — guaranteed conflict.
	testRunGit(t, repo, "checkout", "main")
	writeRepoFile(t, repo, "shared.txt", "v1-main\n")
	testRunGit(t, repo, "add", ".")
	testRunGit(t, repo, "commit", "-m", "main edit")

	testRunGit(t, repo, "checkout", "feature")

	g := gitpkg.NewGit(repo)
	rebased, skipReason, err := autoRebaseOnTarget(g, "main", 1, false, false)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if rebased {
		t.Error("rebased should be false on conflict")
	}
	if skipReason != "" {
		t.Errorf("skipReason should be empty on conflict, got %q", skipReason)
	}

	// AbortRebase is required to leave the working tree in a clean state. After
	// abort, the rebase-merge dir must be gone — otherwise the polecat is stuck
	// in a half-rebased state.
	if _, statErr := os.Stat(filepath.Join(repo, ".git", "rebase-merge")); !os.IsNotExist(statErr) {
		t.Errorf(".git/rebase-merge should not exist after abort (stat err: %v)", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(repo, ".git", "rebase-apply")); !os.IsNotExist(statErr) {
		t.Errorf(".git/rebase-apply should not exist after abort (stat err: %v)", statErr)
	}
}

func writeRepoFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestKasbRecoveryCompletionDoesNotRebaseSubmittedMR(t *testing.T) {
	instructions := recoveryInstructions(mqResumeRecord{Rig: "kasb_core", Worker: "enclave", Source: "kasb-task", MR: "kasb-wisp-original", Branch: "polecat/enclave/kasb-task", Base: "main"})
	// Exercise the actual completion command supplied by native recovery, rather
	// than a hand-picked flag that the old instructions omitted.
	preVerified := strings.Contains(instructions, "followed by gt done --pre-verified")
	fake := &fakeRebaseGit{}
	rebased, _, err := autoRebaseOnTarget(fake, "origin/main", 2, preVerified, false)
	if err != nil || rebased || fake.rebaseCalls != 0 {
		t.Fatalf("native recovery rewrites a verified/pushed existing MR: rebased=%v calls=%d err=%v", rebased, fake.rebaseCalls, err)
	}
}

// hq-i2hc3: a passing receipt for the exact head and source bead is found;
// other heads, other beads, failed runs and per-stage partials are not.
func TestPassingPreflightReceiptForHead(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, ".delivery", "preflight-receipts")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, head, bead, status string) {
		body := `{"head_sha":"` + head + `","source_bead_id":"` + bead + `","status":"` + status + `"}`
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("kasb-x.1-a.json", "aaaa", "kasb-x.1", "failed")
	write("kasb-x.1-b-api-stage.json", "bbbb", "kasb-x.1", "passed")
	write("kasb-x.1-c.json", "cccc", "kasb-x.1", "passed")
	write("kasb-x.2-d.json", "dddd", "kasb-x.2", "passed_non_behavioral")
	write("broken.json", "eeee", "kasb-x.1", "passed\"")

	cases := []struct {
		head, bead string
		want       bool
	}{
		{"cccc", "kasb-x.1", true},
		{"dddd", "kasb-x.2", true},
		{"cccc", "kasb-x.2", false},
		{"aaaa", "kasb-x.1", false},
		{"bbbb", "kasb-x.1", false},
		{"eeee", "kasb-x.1", false},
		{"", "kasb-x.1", false},
	}
	for _, c := range cases {
		got := passingPreflightReceiptForHead(root, c.head, c.bead) != ""
		if got != c.want {
			t.Errorf("head=%q bead=%q: got %v want %v", c.head, c.bead, got, c.want)
		}
	}
	if passingPreflightReceiptForHead(t.TempDir(), "cccc", "kasb-x.1") != "" {
		t.Error("no receipts directory must return empty")
	}
}
