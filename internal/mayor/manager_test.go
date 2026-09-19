package mayor

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/workspace"
)

func TestNewManager(t *testing.T) {
	m := NewManager("/tmp/test-town")
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
	if m.townRoot != "/tmp/test-town" {
		t.Errorf("townRoot = %q, want %q", m.townRoot, "/tmp/test-town")
	}
}

func TestManager_mayorDir(t *testing.T) {
	m := NewManager("/tmp/test-town")
	got := m.mayorDir()
	want := filepath.Join("/tmp/test-town", "mayor")
	if got != want {
		t.Errorf("mayorDir() = %q, want %q", got, want)
	}
}

func TestSessionName_ReturnsConsistentValue(t *testing.T) {
	name := SessionName()
	if name == "" {
		t.Error("SessionName() returned empty string")
	}
	// Verify idempotent
	if SessionName() != name {
		t.Error("SessionName() returned different values on subsequent calls")
	}
}

func TestManager_SessionName_MatchesPackageFunc(t *testing.T) {
	m := NewManager("/tmp/test-town")
	if m.SessionName() != SessionName() {
		t.Errorf("Manager.SessionName() = %q, SessionName() = %q — should match",
			m.SessionName(), SessionName())
	}
}

func TestManager_EnsureNudgePoller(t *testing.T) {
	m := NewManager("/tmp/test-town")
	var gotRoot, gotSession string
	m.startPoller = func(townRoot, session string) (int, error) {
		gotRoot, gotSession = townRoot, session
		return 42, nil
	}

	if err := m.EnsureNudgePoller(); err != nil {
		t.Fatalf("EnsureNudgePoller() error = %v", err)
	}
	if gotRoot != "/tmp/test-town" {
		t.Errorf("poller town root = %q, want %q", gotRoot, "/tmp/test-town")
	}
	if gotSession != SessionName() {
		t.Errorf("poller session = %q, want %q", gotSession, SessionName())
	}
}

func TestManager_EnsureNudgePoller_PropagatesError(t *testing.T) {
	m := NewManager("/tmp/test-town")
	want := errors.New("poller unavailable")
	m.startPoller = func(string, string) (int, error) { return 0, want }

	if err := m.EnsureNudgePoller(); !errors.Is(err, want) {
		t.Errorf("EnsureNudgePoller() error = %v, want %v", err, want)
	}
}

func TestManager_Errors(t *testing.T) {
	if ErrNotRunning.Error() != "mayor not running" {
		t.Errorf("ErrNotRunning = %q", ErrNotRunning)
	}
	if ErrAlreadyRunning.Error() != "mayor already running" {
		t.Errorf("ErrAlreadyRunning = %q", ErrAlreadyRunning)
	}
}

func TestGetMayorPrime(t *testing.T) {
	// Create a temporary directory with town.json
	tmpDir, err := os.MkdirTemp("", "mayor-prime-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create mayor directory
	mayorDir := filepath.Join(tmpDir, "mayor")
	if err := os.MkdirAll(mayorDir, 0755); err != nil {
		t.Fatalf("failed to create mayor dir: %v", err)
	}

	// Create a minimal town.json
	townConfig := &config.TownConfig{
		Name: "test-town",
	}
	townConfigPath := filepath.Join(tmpDir, workspace.PrimaryMarker)
	if err := config.SaveTownConfig(townConfigPath, townConfig); err != nil {
		t.Fatalf("failed to save town config: %v", err)
	}

	// Test GetMayorPrime
	content, err := GetMayorPrime(tmpDir)
	if err != nil {
		t.Fatalf("GetMayorPrime failed: %v", err)
	}

	// Verify content has expected elements
	if !strings.Contains(content, "[prime-rendered-at:") {
		t.Error("GetMayorPrime should contain timestamp marker")
	}
	if !strings.Contains(content, "# Mayor Context") {
		t.Error("GetMayorPrime should render mayor template")
	}
	if !strings.Contains(content, tmpDir) {
		t.Error("GetMayorPrime should contain town root path")
	}
}

func TestGetMayorPrime_InvalidTownRoot(t *testing.T) {
	// Test with non-existent directory - should still return content
	// (town name defaults to "unknown" on error)
	content, err := GetMayorPrime("/nonexistent/path")
	if err != nil {
		t.Fatalf("GetMayorPrime should not fail with invalid town root: %v", err)
	}

	// Should still have the template content
	if !strings.Contains(content, "# Mayor Context") {
		t.Error("GetMayorPrime should render mayor template even with invalid town root")
	}
}
