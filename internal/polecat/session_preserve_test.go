package polecat

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/checkpoint"
)

func TestPreservedSessionStartupChecksBeforeHook(t *testing.T) {
	for _, problem := range []string{"", "matching MR", "checkpoint", "foreign checkpoint", "wrong branch", "wrong head", "dirty", "missing hook", "hook failure", "hook changes head", "manual hold"} {
		t.Run(problem, func(t *testing.T) {
			root := t.TempDir()
			run := func(args ...string) string {
				t.Helper()
				c := exec.Command("git", args...)
				c.Dir = root
				c.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Greg Carter", "GIT_AUTHOR_EMAIL=email@gregorycarter.net", "GIT_COMMITTER_NAME=Greg Carter", "GIT_COMMITTER_EMAIL=email@gregorycarter.net")
				out, err := c.CombinedOutput()
				if err != nil {
					t.Fatalf("git %v: %v %s", args, err, out)
				}
				return strings.TrimSpace(string(out))
			}
			run("init", "--quiet")
			run("checkout", "-b", "polecat/quartz/hisn-test")
			run("commit", "--allow-empty", "-m", "synthetic recovery fixture")
			head := run("rev-parse", "HEAD")
			hooks := 0
			status := "blocked"
			opts := SessionStartOptions{Issue: "hisn-test", PreserveBranch: "polecat/quartz/hisn-test", PreserveHead: head, BeforeLaunch: func() error {
				hooks++
				if problem == "hook failure" {
					return errors.New("Dolt unavailable (fake)")
				}
				if problem == "hook changes head" {
					run("commit", "--allow-empty", "-m", "concurrent change")
				}
				status = "hooked"
				return nil
			}}
			switch problem {
			case "matching MR":
				opts.RecoveryMR = "hisn-wisp-existing"
			case "wrong branch":
				opts.PreserveBranch = "main"
			case "wrong head":
				opts.PreserveHead = strings.Repeat("a", 40)
			case "dirty":
				if err := os.WriteFile(filepath.Join(root, "untracked"), []byte("synthetic"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing hook":
				opts.BeforeLaunch = nil
			case "checkpoint", "foreign checkpoint":
				issue := opts.Issue
				if problem == "foreign checkpoint" {
					issue = "hisn-other"
				}
				if err := checkpoint.Write(root, &checkpoint.Checkpoint{Branch: opts.PreserveBranch, LastCommit: head, HookedBead: issue}); err != nil {
					t.Fatal(err)
				}
			}
			err := prepareSessionLaunch(root, opts, func() error {
				if status != "hooked" || problem == "manual hold" {
					return errors.New("source not admitted")
				}
				source := &beads.Issue{ID: opts.Issue, Status: status, Assignee: "hisn/polecats/quartz"}
				if problem == "matching MR" {
					source.Labels = []string{"awaiting-merge:" + opts.RecoveryMR}
				}
				return validateHisnStartSource(source, "hisn/polecats/quartz", opts)
			})
			if (err == nil) != (problem == "" || problem == "checkpoint" || problem == "matching MR") {
				t.Fatalf("problem=%s err=%v", problem, err)
			}
			if problem != "" && problem != "matching MR" && problem != "checkpoint" && problem != "manual hold" && problem != "hook failure" && problem != "hook changes head" && hooks != 0 {
				t.Fatal("invalid state hooked")
			}
			if problem == "checkpoint" {
				if run("status", "--porcelain") != "" {
					t.Fatal("checkpoint still dirties source")
				}
				files, _ := filepath.Glob(filepath.Join(filepath.Dir(root), ".runtime", "recovery-checkpoints", "*.json"))
				if len(files) != 1 {
					t.Fatal("checkpoint not preserved outside repository")
				}
			}
			if problem != "hook changes head" && run("rev-parse", "HEAD") != head {
				t.Fatal("startup mutated branch")
			}
		})
	}
}

func TestOrdinaryStartupCannotBypassBlockedGuard(t *testing.T) {
	hooked := false
	err := prepareSessionLaunch(t.TempDir(), SessionStartOptions{BeforeLaunch: func() error { hooked = true; return nil }}, func() error { return errors.New("blocked source") })
	if err == nil || hooked {
		t.Fatal("ordinary restart bypassed blocked guard")
	}
}
