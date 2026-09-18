package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPatrolHQStorageUsesExplicitRigProcedure(t *testing.T) {
	town := t.TempDir()
	own := filepath.Join(town, "hisn-procedures")
	shared := filepath.Join(town, "shared")
	for _, dir := range []string{own, shared, filepath.Join(town, "settings"), filepath.Join(town, "hisn", "settings")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	name := "mol-refinery-patrol.formula.toml"
	for _, dir := range []string{own, shared} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("fixture"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(town, "settings", "config.json"), []byte(`{"workflow":{"formulas_dir":"`+shared+`"}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(town, "hisn", "settings", "config.json"), []byte(`{"workflow":{"formulas_dir":"`+own+`"}}`), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := PatrolConfig{BeadsDir: town, Assignee: "hisn/refinery", PatrolMolName: "mol-refinery-patrol"}
	got, err := pinnedPatrolFormula(cfg)
	if err != nil || got != filepath.Join(own, name) {
		t.Fatalf("HQ patrol selected %q: %v", got, err)
	}
	cfg.Assignee = "bridge_town_core/refinery"
	got, err = pinnedPatrolFormula(cfg)
	if err != nil || got != filepath.Join(shared, name) {
		t.Fatalf("Bridge changed: %q %v", got, err)
	}
	cfg.Assignee = "hisn/witness"
	cfg.PatrolMolName = "mol-witness-patrol"
	if _, err := pinnedPatrolFormula(cfg); err == nil {
		t.Fatal("missing Hisn procedure fell back")
	}
}
