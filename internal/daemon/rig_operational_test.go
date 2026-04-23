package daemon

import (
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestIsRigOperational_MissingBead verifies that when a rig bead does not exist
// in an initialized database, the rig is treated as operational (not docked/parked).
// Regression test for gt-fex: missing rig bead caused warning loops and patrol exclusion.
func TestIsRigOperational_MissingBead(t *testing.T) {
	// Use /tmp instead of t.TempDir() because bd init hangs on macOS when the
	// working directory is under /var/folders (the default t.TempDir() location).
	tmpDir, err := os.MkdirTemp("/tmp", "rigoperational_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	rigName := "testrig"
	rigPath := filepath.Join(tmpDir, rigName)
	if err := os.MkdirAll(rigPath, 0755); err != nil {
		t.Fatal(err)
	}

	// Create config.json with a prefix
	configPath := filepath.Join(rigPath, "config.json")
	configJSON := `{"beads": {"prefix": "tr"}}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatal(err)
	}

	// Create town-level .beads with routes.jsonl
	townBeads := filepath.Join(tmpDir, ".beads")
	if err := os.MkdirAll(townBeads, 0755); err != nil {
		t.Fatal(err)
	}
	routesContent := `{"prefix":"tr-","path":"testrig"}`
	if err := os.WriteFile(filepath.Join(townBeads, "routes.jsonl"), []byte(routesContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Initialize beads database so we can test "issue not found" vs "database not found"
	beadsDir := filepath.Join(rigPath, ".beads")
	if err := os.MkdirAll(beadsDir, 0755); err != nil {
		t.Fatal(err)
	}
	bdInitArgs := []string{"init", "--prefix", "tr"}
	if p := os.Getenv("GT_DOLT_PORT"); p != "" {
		bdInitArgs = append(bdInitArgs, "--server-port", p)
	}
	cmd := exec.Command("bd", bdInitArgs...)
	cmd.Dir = rigPath
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("bd init not available, skipping: %v\nOutput: %s", err, output)
	}

	d := &Daemon{
		config: &Config{
			TownRoot: tmpDir,
		},
		logger: log.New(io.Discard, "", 0),
	}

	// The rig bead "tr-rig-testrig" does not exist in the database.
	// It should be treated as operational (no docked/parked label).
	operational, reason := d.isRigOperational(rigName)
	if !operational {
		t.Errorf("isRigOperational should return true when rig bead is missing (no docked/parked status), got false with reason=%q", reason)
	}
}
