package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestLatestJSONLBackupSuccessUsesMarkerWithoutRecentCommit(t *testing.T) {
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	townRoot := filepath.Join(tmp, "town")
	t.Setenv("HOME", homeDir)

	gitMeta := filepath.Join(townRoot, ".dolt-archive", "git", ".git")
	if err := os.MkdirAll(gitMeta, 0o755); err != nil {
		t.Fatalf("mkdir git metadata: %v", err)
	}

	marker := filepath.Join(townRoot, ".dolt-archive", jsonlBackupSuccessMarker)
	if err := os.WriteFile(marker, []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	now := time.Now()
	if err := os.Chtimes(marker, now, now); err != nil {
		t.Fatalf("chtimes marker: %v", err)
	}

	got := latestJSONLBackupSuccess(townRoot)
	if got.IsZero() {
		t.Fatalf("latestJSONLBackupSuccess() returned zero time")
	}
	if delta := now.Sub(got); delta < 0 || delta > 2*time.Second {
		t.Fatalf("latestJSONLBackupSuccess() = %v, want near %v", got, now)
	}
}

func TestLatestJSONLBackupSuccessPrefersNewestCandidate(t *testing.T) {
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	townRoot := filepath.Join(tmp, "town")
	t.Setenv("HOME", homeDir)

	legacyGitMeta := filepath.Join(homeDir, ".dolt-archive", "git", ".git")
	townGitMeta := filepath.Join(townRoot, ".dolt-archive", "git", ".git")
	if err := os.MkdirAll(legacyGitMeta, 0o755); err != nil {
		t.Fatalf("mkdir legacy git metadata: %v", err)
	}
	if err := os.MkdirAll(townGitMeta, 0o755); err != nil {
		t.Fatalf("mkdir town git metadata: %v", err)
	}

	legacyMarker := filepath.Join(homeDir, ".dolt-archive", jsonlBackupSuccessMarker)
	townMarker := filepath.Join(townRoot, ".dolt-archive", jsonlBackupSuccessMarker)
	if err := os.WriteFile(legacyMarker, []byte("legacy\n"), 0o644); err != nil {
		t.Fatalf("write legacy marker: %v", err)
	}
	if err := os.WriteFile(townMarker, []byte("town\n"), 0o644); err != nil {
		t.Fatalf("write town marker: %v", err)
	}

	older := time.Now().Add(-45 * time.Minute)
	newer := time.Now()
	if err := os.Chtimes(legacyMarker, older, older); err != nil {
		t.Fatalf("chtimes legacy marker: %v", err)
	}
	if err := os.Chtimes(townMarker, newer, newer); err != nil {
		t.Fatalf("chtimes town marker: %v", err)
	}

	got := latestJSONLBackupSuccess(townRoot)
	if delta := newer.Sub(got); delta < 0 || delta > 2*time.Second {
		t.Fatalf("latestJSONLBackupSuccess() = %v, want near %v", got, newer)
	}
}
