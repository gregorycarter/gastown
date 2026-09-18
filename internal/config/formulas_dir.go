package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FormulaRigFromPath determines routing from the target work directory, never
// the invoking agent's environment (which may name another rig).
func FormulaRigFromPath(townRoot, workDir string) string {
	rel, err := filepath.Rel(townRoot, workDir)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return ""
	}
	rig := strings.Split(rel, string(filepath.Separator))[0]
	if _, err := os.Stat(filepath.Join(townRoot, rig, "config.json")); err != nil {
		return ""
	}
	return rig
}

// PinnedFormulaFile keeps rendering, verification and cooking on the same
// explicit file. An opted-in rig pin takes precedence; a broken/missing rig pin
// fails closed instead of accidentally executing another rig's town policy.
// Rigs without an override retain the existing town-pinned behavior.
func PinnedFormulaFile(townRoot, rigName, name string) (string, error) {
	if name == "" || filepath.Base(name) != name {
		return "", nil
	}
	if townRoot != "" && rigName != "" {
		if filepath.Base(rigName) != rigName || rigName == "." || rigName == ".." {
			return "", fmt.Errorf("invalid formula rig")
		}
		settingsPath := RigSettingsPath(filepath.Join(townRoot, rigName))
		data, err := os.ReadFile(settingsPath)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if err == nil {
			var settings struct {
				Workflow *WorkflowConfig `json:"workflow"`
			}
			if err := json.Unmarshal(data, &settings); err != nil {
				return "", fmt.Errorf("rig formula settings: %w", err)
			}
			if settings.Workflow != nil && settings.Workflow.FormulasDir != "" {
				dir := settings.Workflow.FormulasDir
				if !filepath.IsAbs(dir) {
					return "", fmt.Errorf("rig formula pin must be absolute")
				}
				if file := FormulaFileIn(dir, name); file != "" {
					return file, nil
				}
				return "", fmt.Errorf("rig %s pinned formula %s missing; refusing town fallback", rigName, name)
			}
		}
	}
	return FormulaFileIn(FormulasDir(townRoot), name), nil
}

// FormulasDir returns the town's pinned formula directory
// (workflow.formulas_dir in settings/config.json), or "" when unset.
//
// The path must be absolute and must exist as a directory; anything else is
// treated as unset so a stale setting degrades to the historical resolution
// order rather than making every formula unresolvable.
func FormulasDir(townRoot string) string {
	if townRoot == "" {
		return ""
	}
	ts, err := LoadOrCreateTownSettings(TownSettingsPath(townRoot))
	if err != nil || ts == nil || ts.Workflow == nil {
		return ""
	}
	dir := ts.Workflow.FormulasDir
	if dir == "" || !filepath.IsAbs(dir) {
		return ""
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return ""
	}
	return dir
}

// FormulaFileIn returns the path a formula file would occupy inside dir,
// or "" when dir is empty or the file is absent. The name may be given with or
// without the .formula.toml suffix.
func FormulaFileIn(dir, name string) string {
	if dir == "" || name == "" {
		return ""
	}
	filename := name
	if filepath.Ext(filename) != ".toml" {
		filename += ".formula.toml"
	}
	path := filepath.Join(dir, filename)
	if info, err := os.Stat(path); err != nil || info.IsDir() {
		return ""
	}
	return path
}
