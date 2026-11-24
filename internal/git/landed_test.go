package git

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMessageNamesIssue(t *testing.T) {
	cases := []struct {
		message string
		want    bool
	}{
		{"fix(api): retry (kasb-9oupz.2)", true},
		{"kasb-9oupz.2: retry", true},
		{"Closes kasb-9oupz.2.", true},
		{"fix (kasb-9oupz.20)", false},
		{"fix (kasb-9oupz.2.1)", false},
		{"fix (xkasb-9oupz.2)", false},
		{"fix (kasb-9oupz.21) and kasb-9oupz.2", true},
		{"unrelated", false},
	}
	for _, tc := range cases {
		if got := MessageNamesIssue(tc.message, "kasb-9oupz.2"); got != tc.want {
			t.Errorf("MessageNamesIssue(%q) = %v, want %v", tc.message, got, tc.want)
		}
	}
}

func landedTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	c.Env = append(os.Environ(), "GIT_AUTHOR_NAME=T", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=T", "GIT_COMMITTER_EMAIL=t@example.invalid")
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestLandedIssueCommitFindsRebasedLanding(t *testing.T) {
	dir := t.TempDir()
	landedTestGit(t, dir, "init", "-b", "main")
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("a.txt", "base\n")
	landedTestGit(t, dir, "add", ".")
	landedTestGit(t, dir, "commit", "-m", "base")
	// The polecat branch holds the original commit.
	landedTestGit(t, dir, "checkout", "-b", "polecat/thunder/kasb-9oupz.2+x")
	write("b.txt", "work\n")
	landedTestGit(t, dir, "add", ".")
	landedTestGit(t, dir, "commit", "-m", "feat: work (kasb-9oupz.2)")
	// main gets the lander's rebased, conflict-resolved equivalent (another
	// patch) plus a sibling bead's commit.
	landedTestGit(t, dir, "checkout", "main")
	write("b.txt", "work, resolved\n")
	landedTestGit(t, dir, "add", ".")
	landedTestGit(t, dir, "commit", "-m", "feat: work (kasb-9oupz.2)")
	landed := landedTestGit(t, dir, "rev-parse", "HEAD")
	write("c.txt", "sibling\n")
	landedTestGit(t, dir, "add", ".")
	landedTestGit(t, dir, "commit", "-m", "feat: sibling (kasb-9oupz.20)")
	landedTestGit(t, dir, "update-ref", "refs/remotes/origin/main", "HEAD")
	landedTestGit(t, dir, "checkout", "polecat/thunder/kasb-9oupz.2+x")

	g := NewGit(dir)
	sha, ref, err := g.LandedIssueCommit("kasb-9oupz.2", nil)
	if err != nil || sha != landed || ref != "origin/main" {
		t.Fatalf("LandedIssueCommit = %q %q %v, want %s on origin/main", sha, ref, err, landed)
	}
	if sha, _, err := g.LandedIssueCommit("kasb-9oupz.3", []string{"main"}); err != nil || sha != "" {
		t.Fatalf("unlanded issue reported landed: %q %v", sha, err)
	}
	if _, _, err := NewGit(t.TempDir()).LandedIssueCommit("kasb-9oupz.2", nil); err == nil {
		t.Fatal("no target ref must be an error, not 'not landed'")
	}
}
