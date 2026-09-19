package polecat

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreservedSessionStartupChecksBeforeHook(t *testing.T) {
	for _, problem := range []string{"", "wrong branch", "wrong head", "dirty", "missing hook", "hook failure", "hook changes head"} {
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
			opts := SessionStartOptions{Issue: "hisn-test", PreserveBranch: "polecat/quartz/hisn-test", PreserveHead: head, BeforeLaunch: func() error {
				hooks++
				if problem == "hook failure" {
					return errors.New("Dolt unavailable (fake)")
				}
				if problem == "hook changes head" {
					run("commit", "--allow-empty", "-m", "concurrent change")
				}
				return nil
			}}
			switch problem {
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
			}
			err := preparePreservedSession(root, opts)
			if (err == nil) != (problem == "") {
				t.Fatalf("problem=%s err=%v", problem, err)
			}
			if problem != "" && problem != "hook failure" && problem != "hook changes head" && hooks != 0 {
				t.Fatal("invalid state hooked")
			}
			if problem != "hook changes head" && run("rev-parse", "HEAD") != head {
				t.Fatal("startup mutated branch")
			}
		})
	}
}
