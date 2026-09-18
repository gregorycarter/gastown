package formula

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRigPinRenderedInsteadOfSharedTownPatrol(t *testing.T) {
	town := t.TempDir()
	shared := filepath.Join(town, "shared")
	own := filepath.Join(town, "hisn", "policy")
	writeTownSettings(t, town, shared)
	writeFormula(t, shared, "mol-refinery-patrol", "bridge-only")
	writeFormula(t, own, "mol-refinery-patrol", "hisn-only")
	dir := filepath.Join(town, "hisn", "settings")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"workflow":{"formulas_dir":"`+own+`"}}`), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveFormulaContent("mol-refinery-patrol", town, "hisn")
	if err != nil || string(got) != "hisn-only" {
		t.Fatalf("%s %v", got, err)
	}
	got, err = ResolveFormulaContent("mol-refinery-patrol", town, "bridge_town_core")
	if err != nil || string(got) != "bridge-only" {
		t.Fatalf("%s %v", got, err)
	}
	if _, err = ResolveFormulaContent("mol-polecat-work", town, "hisn"); err == nil {
		t.Fatal("missing Hisn formula used another policy")
	}
}
