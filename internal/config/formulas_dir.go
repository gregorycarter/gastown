package config

import (
	"os"
	"path/filepath"
)

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
