package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeWorkflowSettings(t *testing.T, townRoot string, workflow map[string]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(townRoot, "settings"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"type": "town-settings", "version": 1}
	if workflow != nil {
		payload["workflow"] = workflow
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(TownSettingsPath(townRoot), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFormulasDir(t *testing.T) {
	town := t.TempDir()
	pinned := filepath.Join(town, "formulas")
	if err := os.MkdirAll(pinned, 0o755); err != nil {
		t.Fatal(err)
	}

	if got := FormulasDir(""); got != "" {
		t.Errorf("empty town root = %q, want empty", got)
	}

	writeWorkflowSettings(t, town, nil)
	if got := FormulasDir(town); got != "" {
		t.Errorf("unset workflow = %q, want empty", got)
	}

	writeWorkflowSettings(t, town, map[string]string{"formulas_dir": pinned})
	if got := FormulasDir(town); got != pinned {
		t.Errorf("configured dir = %q, want %q", got, pinned)
	}

	// A relative path is rejected: bd is handed this as an explicit file path
	// from varying working directories, so only an absolute path is meaningful.
	writeWorkflowSettings(t, town, map[string]string{"formulas_dir": "relative/formulas"})
	if got := FormulasDir(town); got != "" {
		t.Errorf("relative dir = %q, want empty", got)
	}

	// A stale path must degrade to the historical tiers, not break resolution.
	writeWorkflowSettings(t, town, map[string]string{"formulas_dir": filepath.Join(town, "gone")})
	if got := FormulasDir(town); got != "" {
		t.Errorf("missing dir = %q, want empty", got)
	}

	// A file where a directory is expected is also rejected.
	notADir := filepath.Join(town, "afile")
	if err := os.WriteFile(notADir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	writeWorkflowSettings(t, town, map[string]string{"formulas_dir": notADir})
	if got := FormulasDir(town); got != "" {
		t.Errorf("file as dir = %q, want empty", got)
	}
}

func TestFormulaFileIn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mol-probe.formula.toml")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := FormulaFileIn(dir, "mol-probe"); got != path {
		t.Errorf("bare name = %q, want %q", got, path)
	}
	if got := FormulaFileIn(dir, "mol-probe.formula.toml"); got != path {
		t.Errorf("suffixed name = %q, want %q", got, path)
	}
	if got := FormulaFileIn(dir, "mol-absent"); got != "" {
		t.Errorf("absent formula = %q, want empty", got)
	}
	if got := FormulaFileIn("", "mol-probe"); got != "" {
		t.Errorf("empty dir = %q, want empty", got)
	}
	if got := FormulaFileIn(dir, ""); got != "" {
		t.Errorf("empty name = %q, want empty", got)
	}
}
