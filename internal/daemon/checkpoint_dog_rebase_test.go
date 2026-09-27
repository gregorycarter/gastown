package daemon

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	out, _ := cmd.CombinedOutput()
	return string(out)
}

func TestCheckpointBlockedDuringConflictedRebase(t *testing.T) {
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	write := func(s string) { _ = os.WriteFile(filepath.Join(dir, "f.txt"), []byte(s), 0o644) }
	write("base\n")
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-qm", "base")
	gitIn(t, dir, "checkout", "-qb", "work")
	write("work\n")
	gitIn(t, dir, "commit", "-qam", "work")
	gitIn(t, dir, "checkout", "-q", "main")
	write("main\n")
	gitIn(t, dir, "commit", "-qam", "main")
	gitIn(t, dir, "checkout", "-q", "work")
	if reason := checkpointBlockedReason(dir); reason != "" {
		t.Fatalf("clean worktree blocked: %s", reason)
	}
	gitIn(t, dir, "rebase", "main") // conflicts
	reason := checkpointBlockedReason(dir)
	if !strings.Contains(reason, "rebase") && !strings.Contains(reason, "unmerged") {
		t.Fatalf("conflicted rebase not blocked, reason=%q", reason)
	}
	gitIn(t, dir, "rebase", "--abort")
	if reason := checkpointBlockedReason(dir); reason != "" {
		t.Fatalf("after abort still blocked: %s", reason)
	}
}
