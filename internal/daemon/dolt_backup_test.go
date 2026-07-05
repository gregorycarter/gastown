package daemon

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDoltBackupOffsiteDirDefault(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir: %v", err)
	}

	got := doltBackupOffsiteDir(nil)
	want := filepath.Join(home, "dolt-backups", "gt-dolt-backup")

	if got != want {
		t.Fatalf("doltBackupOffsiteDir(nil) = %q, want %q", got, want)
	}
}

func TestDoltBackupOffsiteDirConfiguredAbsolute(t *testing.T) {
	got := doltBackupOffsiteDir(&DoltBackupConfig{OffsiteDir: "/tmp/custom-dolt-backup"})
	if got != "/tmp/custom-dolt-backup" {
		t.Fatalf("doltBackupOffsiteDir configured absolute = %q", got)
	}
}

func TestDoltBackupOffsiteDirConfiguredTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir: %v", err)
	}

	got := doltBackupOffsiteDir(&DoltBackupConfig{OffsiteDir: "~/custom-dolt-backup"})
	want := filepath.Join(home, "custom-dolt-backup")

	if got != want {
		t.Fatalf("doltBackupOffsiteDir configured tilde = %q, want %q", got, want)
	}
}
