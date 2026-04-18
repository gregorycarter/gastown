package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindExistingJSONLGitRepoPrefersTownRoot(t *testing.T) {
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	townRoot := filepath.Join(tmp, "town")
	t.Setenv("HOME", homeDir)

	townRepo := filepath.Join(townRoot, ".dolt-archive", "git", ".git")
	legacyRepo := filepath.Join(homeDir, "gt", ".dolt-archive", "git", ".git")
	if err := os.MkdirAll(townRepo, 0o755); err != nil {
		t.Fatalf("mkdir town repo: %v", err)
	}
	if err := os.MkdirAll(legacyRepo, 0o755); err != nil {
		t.Fatalf("mkdir legacy repo: %v", err)
	}

	got := findExistingJSONLGitRepo(townRoot)
	want := filepath.Join(townRoot, ".dolt-archive", "git")
	if got != want {
		t.Fatalf("findExistingJSONLGitRepo() = %q, want %q", got, want)
	}
}

func TestFindExistingJSONLGitRepoFallsBackToLegacyLocations(t *testing.T) {
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	townRoot := filepath.Join(tmp, "town")
	t.Setenv("HOME", homeDir)

	legacyRepo := filepath.Join(homeDir, "gt", ".dolt-archive", "git", ".git")
	if err := os.MkdirAll(legacyRepo, 0o755); err != nil {
		t.Fatalf("mkdir legacy repo: %v", err)
	}

	got := findExistingJSONLGitRepo(townRoot)
	want := filepath.Join(homeDir, "gt", ".dolt-archive", "git")
	if got != want {
		t.Fatalf("findExistingJSONLGitRepo() = %q, want %q", got, want)
	}
}

func TestFindExistingJSONLGitRepoReturnsEmptyWhenMissing(t *testing.T) {
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	townRoot := filepath.Join(tmp, "town")
	t.Setenv("HOME", homeDir)

	if got := findExistingJSONLGitRepo(townRoot); got != "" {
		t.Fatalf("findExistingJSONLGitRepo() = %q, want empty", got)
	}
}
