package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimeCheckHashesEffectiveRigPolicy(t *testing.T) {
	town := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	pins := filepath.Join(town, "hisn/pins")
	settings, _ := json.Marshal(map[string]any{"workflow": map[string]string{"formulas_dir": pins}})
	write(filepath.Join(town, "hisn/settings/config.json"), string(settings))
	write(filepath.Join(town, "CLAUDE.md"), "town")
	write(filepath.Join(town, "hisn/AGENTS.md"), "rig policy")
	write(filepath.Join(town, "hisn/refinery/rig/AGENTS.md"), "repository policy")
	write(filepath.Join(town, ".beads/formulas/mol-refinery-patrol.formula.toml"), "foreign town formula")
	pin := filepath.Join(pins, "mol-refinery-patrol.formula.toml")
	write(pin, "hisn formula v1")
	info := RoleInfo{Role: RoleRefinery, TownRoot: town, Rig: "hisn"}
	repo := filepath.Join(town, "hisn/refinery/rig")
	first, err := effectivePolicyHashes(info, repo)
	if err != nil || len(first) != 4 {
		t.Fatalf("hashes=%v error=%v", first, err)
	}
	if first["AGENTS.md"] == first["rig/AGENTS.md"] {
		t.Fatal("rig/repository policy labels collided")
	}
	write(pin, "hisn formula v2")
	second, err := effectivePolicyHashes(info, repo)
	if err != nil || first["mol-refinery-patrol.formula.toml"] == second["mol-refinery-patrol.formula.toml"] {
		t.Fatal("rig pin change invisible", err)
	}
	if err := os.Remove(pin); err != nil {
		t.Fatal(err)
	}
	if _, err := effectivePolicyHashes(info, repo); err == nil {
		t.Fatal("broken rig pin silently fell back to town")
	}
}
