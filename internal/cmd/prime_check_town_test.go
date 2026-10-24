package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func runTownPrimeCheck(t *testing.T, info RoleInfo) (map[string]any, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&output)
	err := runPrimeCheck(cmd, info, "")
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result, nil
}

func TestPrimeCheckTownRolesWithGitCeiling(t *testing.T) {
	for _, role := range []Role{RoleDeacon, RoleMayor} {
		t.Run(string(role), func(t *testing.T) {
			town := makeTestGitRepo(t)
			home := filepath.Join(town, string(role))
			if err := os.MkdirAll(home, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("GIT_CEILING_DIRECTORIES", town)
			t.Setenv("TMUX", "")
			// Reproduce the old failure with the same Git command and environment.
			if _, err := primeCheckCommand(home, "git", "branch", "--show-current"); err == nil {
				t.Fatal("Git unexpectedly discovered the town through the ceiling")
			}
			info := RoleInfo{Role: role, Home: home, WorkDir: home, TownRoot: town}
			result, err := runTownPrimeCheck(t, info)
			if err != nil {
				t.Fatal(err)
			}
			canonicalTown, _ := filepath.EvalSymlinks(town)
			if result["ok"] != true || result["cwd"] != home || result["git_cwd"] != canonicalTown || result["head"] == "" {
				t.Fatalf("incorrect role/repository evidence: %v", result)
			}
			if os.Getenv("GIT_CEILING_DIRECTORIES") != town {
				t.Fatal("prime changed the Git ceiling")
			}
		})
	}
}

func TestPrimeCheckTownCannotBorrowParentRepository(t *testing.T) {
	parent := makeTestGitRepo(t)
	town := filepath.Join(parent, "town")
	home := filepath.Join(town, "deacon")
	if err := os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	// Even without a ceiling, an unrelated ancestor cannot satisfy town evidence.
	t.Setenv("GIT_CEILING_DIRECTORIES", "")
	_, err := runTownPrimeCheck(t, RoleInfo{Role: RoleDeacon, Home: home, WorkDir: home, TownRoot: town})
	if err == nil || !strings.Contains(err.Error(), "town repository metadata missing") {
		t.Fatalf("parent repository accepted: %v", err)
	}
}

func TestPrimeCheckTownRejectsEscapedRoleHome(t *testing.T) {
	town := makeTestGitRepo(t)
	home := filepath.Join(town, "deacon")
	if err := os.Symlink(t.TempDir(), home); err != nil {
		t.Fatal(err)
	}
	_, err := runTownPrimeCheck(t, RoleInfo{Role: RoleDeacon, Home: home, WorkDir: home, TownRoot: town})
	if err == nil || !strings.Contains(err.Error(), "outside canonical town") {
		t.Fatalf("escaped role accepted: %v", err)
	}
}

func TestPrimeCheckTownStillRejectsRoleMismatch(t *testing.T) {
	town := makeTestGitRepo(t)
	home := filepath.Join(town, "mayor")
	cwd := filepath.Join(town, "deacon")
	for _, path := range []string{home, cwd} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	_, err := runTownPrimeCheck(t, RoleInfo{Role: RoleMayor, Home: home, WorkDir: cwd, TownRoot: town})
	if err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("role mismatch accepted: %v", err)
	}
}

func TestPrimeCheckWorkerCannotUseTownRepository(t *testing.T) {
	town := makeTestGitRepo(t)
	home := filepath.Join(town, "rig", "polecats", "nux")
	cwd := filepath.Join(home, "rig")
	if err := os.MkdirAll(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CEILING_DIRECTORIES", town)
	_, err := runTownPrimeCheck(t, RoleInfo{Role: RolePolecat, Rig: "rig", Polecat: "nux", Home: home, WorkDir: cwd, TownRoot: town})
	if err == nil || !strings.Contains(err.Error(), "git state read failed") {
		t.Fatalf("worker borrowed the town repository: %v", err)
	}
}
