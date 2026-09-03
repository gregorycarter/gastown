package doctor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFormulaFile(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".formula.toml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFormulaVersion(t *testing.T) {
	if got := formulaVersion([]byte("name = \"x\"\nversion = 14\n")); got != "14" {
		t.Errorf("version = %q, want 14", got)
	}
	if got := formulaVersion([]byte("version = \"22\"\n")); got != "22" {
		t.Errorf("quoted version = %q, want 22", got)
	}
	if got := formulaVersion([]byte("name = \"x\"\n")); got != "?" {
		t.Errorf("missing version = %q, want ?", got)
	}
}

func TestDivergedFormulas(t *testing.T) {
	base := t.TempDir()
	townDir := filepath.Join(base, "town")
	rigDir := filepath.Join(base, "rig")

	// Same content in both tiers → no divergence.
	writeFormulaFile(t, townDir, "mol-same", "version = 3\nbody")
	writeFormulaFile(t, rigDir, "mol-same", "version = 3\nbody")
	// Different content → divergence.
	writeFormulaFile(t, townDir, "mol-drifted", "version = 16\ntown body")
	writeFormulaFile(t, rigDir, "mol-drifted", "version = 22\nrig body")
	// Only in one tier → not a divergence.
	writeFormulaFile(t, townDir, "mol-town-only", "version = 1")

	tiers := []formulaTier{{label: "town", dir: townDir}, {label: "rig r", dir: rigDir}}
	got := divergedFormulas(tiers)

	if len(got) != 1 {
		t.Fatalf("expected exactly one divergence, got %v", got)
	}
	if !strings.Contains(got[0], "mol-drifted") {
		t.Errorf("divergence %q should name mol-drifted", got[0])
	}
	for _, want := range []string{"v16", "v22", "town", "rig r"} {
		if !strings.Contains(got[0], want) {
			t.Errorf("divergence %q missing %q", got[0], want)
		}
	}
}

func TestDivergedFormulasMissingDirsAreInert(t *testing.T) {
	tiers := []formulaTier{
		{label: "pinned", dir: filepath.Join(t.TempDir(), "absent")},
		{label: "town", dir: filepath.Join(t.TempDir(), "also-absent")},
	}
	if got := divergedFormulas(tiers); len(got) != 0 {
		t.Errorf("missing directories should produce no findings, got %v", got)
	}
}

func TestResolveBeadsRedirect(t *testing.T) {
	base := t.TempDir()
	beadsDir := filepath.Join(base, "rig", ".beads")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if got := resolveBeadsRedirect(beadsDir); got != "" {
		t.Errorf("no redirect file = %q, want empty", got)
	}

	// Redirect targets are relative to the directory CONTAINING .beads,
	// matching beads.ResolveBeadsDir — this is the live bridge_town_core shape.
	target := filepath.Join(base, "rig", "mayor", "rig", ".beads")
	if err := os.WriteFile(filepath.Join(beadsDir, "redirect"), []byte("mayor/rig/.beads\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := resolveBeadsRedirect(beadsDir); got != filepath.Clean(target) {
		t.Errorf("relative redirect = %q, want %q", got, target)
	}

	if err := os.WriteFile(filepath.Join(beadsDir, "redirect"), []byte(beadsDir+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := resolveBeadsRedirect(beadsDir); got != "" {
		t.Errorf("self-redirect = %q, want empty", got)
	}
}

func TestFormulaSourceCheckCleanTown(t *testing.T) {
	town := t.TempDir()
	writeFormulaFile(t, filepath.Join(town, ".beads", "formulas"), "mol-only", "version = 1")

	c := NewFormulaSourceCheck()
	res := c.Run(&CheckContext{TownRoot: town})
	if res.Status != StatusOK {
		t.Errorf("status = %v (%s), want OK", res.Status, res.Message)
	}
}

func TestResolveBeadsRedirectFollowsChain(t *testing.T) {
	base := t.TempDir()
	first := filepath.Join(base, "rig", ".beads")
	second := filepath.Join(base, "rig", "mayor", "rig", ".beads")
	final := filepath.Join(base, "canonical", ".beads")
	for _, d := range []string{first, second, final} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(first, "redirect"), []byte("mayor/rig/.beads\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(second, "redirect"), []byte(final+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if got := resolveBeadsRedirect(first); got != filepath.Clean(final) {
		t.Errorf("chained redirect = %q, want %q", got, final)
	}
}

func TestRegisteredRigNames(t *testing.T) {
	town := t.TempDir()
	if got := registeredRigNames(town); got != nil {
		t.Errorf("missing rigs.json = %v, want nil", got)
	}

	if err := os.MkdirAll(filepath.Join(town, "mayor"), 0o755); err != nil {
		t.Fatal(err)
	}
	body := `{"version":1,"rigs":{"bridge_town_core":{"prefix":"bt"},"alpha":{}}}`
	if err := os.WriteFile(filepath.Join(town, "mayor", "rigs.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	got := registeredRigNames(town)
	if len(got) != 2 || got[0] != "alpha" || got[1] != "bridge_town_core" {
		t.Errorf("registered rigs = %v, want [alpha bridge_town_core]", got)
	}

	// A directory that merely looks like a rig must not be picked up.
	if err := os.MkdirAll(filepath.Join(town, "events", ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := registeredRigNames(town); len(got) != 2 {
		t.Errorf("unregistered directory should not appear: %v", got)
	}
}
