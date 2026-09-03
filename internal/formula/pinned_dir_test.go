package formula

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// writeTownSettings writes a minimal settings/config.json with workflow.formulas_dir.
func writeTownSettings(t *testing.T, townRoot, formulasDir string) {
	t.Helper()
	settingsDir := filepath.Join(townRoot, "settings")
	if err := os.MkdirAll(settingsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{
		"type":     "town-settings",
		"version":  1,
		"workflow": map[string]string{"formulas_dir": formulasDir},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(settingsDir, "config.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeFormula(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".formula.toml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestResolveFormulaContentTierOrder pins the resolution order:
// pinned > rig > town > embedded.
func TestResolveFormulaContentTierOrder(t *testing.T) {
	town := t.TempDir()
	pinned := filepath.Join(town, "pinned-formulas")

	const name = "mol-tier-probe"
	writeFormula(t, filepath.Join(town, ".beads", "formulas"), name, "source = town")
	writeFormula(t, filepath.Join(town, "myrig", ".beads", "formulas"), name, "source = rig")

	// No pinned dir configured: rig wins over town.
	got, err := ResolveFormulaContent(name, town, "myrig")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(got) != "source = rig" {
		t.Errorf("without formulas_dir got %q, want the rig copy", got)
	}

	// No rig: town wins.
	got, err = ResolveFormulaContent(name, town, "")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(got) != "source = town" {
		t.Errorf("with no rig got %q, want the town copy", got)
	}

	// Pinned dir configured: it beats both.
	writeFormula(t, pinned, name, "source = pinned")
	writeTownSettings(t, town, pinned)
	got, err = ResolveFormulaContent(name, town, "myrig")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(got) != "source = pinned" {
		t.Errorf("with formulas_dir got %q, want the pinned copy", got)
	}
}

// TestResolveFormulaContentPinnedMissFallsThrough proves a pinned directory
// that lacks the formula does not make it unresolvable.
func TestResolveFormulaContentPinnedMissFallsThrough(t *testing.T) {
	town := t.TempDir()
	pinned := filepath.Join(town, "pinned-formulas")
	if err := os.MkdirAll(pinned, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTownSettings(t, town, pinned)
	writeFormula(t, filepath.Join(town, ".beads", "formulas"), "mol-only-in-town", "source = town")

	got, err := ResolveFormulaContent("mol-only-in-town", town, "myrig")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(got) != "source = town" {
		t.Errorf("got %q, want the town copy", got)
	}
}

// TestResolveFormulaContentFallsBackToEmbedded proves the embedded tier still
// answers when nothing is on disk.
func TestResolveFormulaContentFallsBackToEmbedded(t *testing.T) {
	town := t.TempDir()
	got, err := ResolveFormulaContent("mol-polecat-work", town, "myrig")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) == 0 {
		t.Error("expected embedded content")
	}
}
