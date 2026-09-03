package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCloseOnMergeDefaultsTrue(t *testing.T) {
	if !(&TownSettings{}).CloseOnMergeEnabled() {
		t.Fatal("absent workflow config should default to close_on_merge=true")
	}
	if !(*TownWorkflowConfig)(nil).GetCloseOnMerge() {
		t.Fatal("nil workflow config should default to true")
	}
}

func TestCloseOnMergeExplicitFalse(t *testing.T) {
	off := false
	s := &TownSettings{Workflow: &TownWorkflowConfig{CloseOnMerge: &off}}
	if s.CloseOnMergeEnabled() {
		t.Fatal("explicit false must be honoured (upstream behaviour)")
	}
}

func TestCloseOnMergeEnabledForTownReadsSettings(t *testing.T) {
	town := t.TempDir()
	if err := os.MkdirAll(filepath.Join(town, "settings"), 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"type":"town-settings","version":1,"workflow":{"close_on_merge":false}}`
	if err := os.WriteFile(TownSettingsPath(town), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	ResetCloseOnMergeCache()
	t.Cleanup(ResetCloseOnMergeCache)

	if CloseOnMergeEnabledForTown(town) {
		t.Fatal("settings file with close_on_merge=false should disable the lifecycle")
	}
	// Missing settings file → compiled-in default.
	if !CloseOnMergeEnabledForTown(t.TempDir()) {
		t.Fatal("town without settings should default to true")
	}
	if !CloseOnMergeEnabledForTown("") {
		t.Fatal("empty town root should default to true")
	}
}

func TestCloseOnMergeRoundTripsThroughSave(t *testing.T) {
	town := t.TempDir()
	if err := os.MkdirAll(filepath.Join(town, "settings"), 0o755); err != nil {
		t.Fatal(err)
	}
	path := TownSettingsPath(town)
	off := false
	s := NewTownSettings()
	s.Workflow = &TownWorkflowConfig{CloseOnMerge: &off}
	if err := SaveTownSettings(path, s); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadOrCreateTownSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CloseOnMergeEnabled() {
		t.Fatal("close_on_merge=false did not survive save/load")
	}
}
