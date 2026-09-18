package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRigPinnedFormulaIsolation(t *testing.T) {
	town := t.TempDir()
	shared := filepath.Join(town, "shared")
	own := filepath.Join(town, "hisn", "policy")
	for _, dir := range []string{shared, own, filepath.Join(town, "hisn", "settings")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	writeWorkflowSettings(t, town, map[string]string{"formulas_dir": shared})
	for _, dir := range []string{shared, own} {
		if err := os.WriteFile(filepath.Join(dir, "mol-refinery-patrol.formula.toml"), []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	settings := `{"workflow":{"formulas_dir":"` + own + `"}}`
	if err := os.WriteFile(RigSettingsPath(filepath.Join(town, "hisn")), []byte(settings), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := PinnedFormulaFile(town, "hisn", "mol-refinery-patrol")
	if err != nil || got != filepath.Join(own, "mol-refinery-patrol.formula.toml") {
		t.Fatalf("Hisn pin: %s %v", got, err)
	}
	got, err = PinnedFormulaFile(town, "bridge_town_core", "mol-refinery-patrol")
	if err != nil || got != filepath.Join(shared, "mol-refinery-patrol.formula.toml") {
		t.Fatalf("Bridge changed: %s %v", got, err)
	}
	if _, err = PinnedFormulaFile(town, "hisn", "mol-missing"); err == nil {
		t.Fatal("missing explicit rig policy fell through")
	}
	if _, err = PinnedFormulaFile(town, "../hisn", "mol-refinery-patrol"); err == nil {
		t.Fatal("traversal allowed")
	}
	if err := os.WriteFile(filepath.Join(town, "hisn", "config.json"), []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}
	if got := FormulaRigFromPath(town, filepath.Join(town, "hisn", "refinery", "rig")); got != "hisn" {
		t.Fatalf("target route = %q", got)
	}
	if got := FormulaRigFromPath(town, filepath.Dir(town)); got != "" {
		t.Fatalf("outside route = %q", got)
	}
	if err := os.RemoveAll(own); err != nil {
		t.Fatal(err)
	} // Own t.TempDir fixture only.
	if _, err = PinnedFormulaFile(town, "hisn", "mol-refinery-patrol"); err == nil {
		t.Fatal("stale rig policy fell through to Bridge")
	}
}
