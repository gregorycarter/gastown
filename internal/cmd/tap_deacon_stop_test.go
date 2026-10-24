package cmd

import (
	"bytes"
	"encoding/json"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"testing"

	"github.com/steveyegge/gastown/internal/deacon"
)

func TestDeaconStopHonorsOperatorControls(t *testing.T) {
	root := t.TempDir()
	if !deaconShouldContinue(root) {
		t.Fatal("enabled patrol should continue")
	}
	if err := deacon.Pause(root, "operator pause", "human"); err != nil {
		t.Fatal(err)
	}
	if deaconShouldContinue(root) {
		t.Fatal("paused patrol must stop")
	}
	if err := deacon.Resume(root); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "settings")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"disabled_patrols":["deacon"]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if deaconShouldContinue(root) {
		t.Fatal("disabled patrol must stop")
	}
}

func TestDeaconStopRequiresRealWaitAndScopesIdentity(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "deacon")
	for _, dir := range []string{home, filepath.Join(root, "mayor")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "mayor", "town.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(home)
	t.Setenv("GT_ROLE", "deacon")
	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := runTapDeaconStop(cmd, nil); err != nil {
		t.Fatal(err)
	}
	var decision map[string]string
	if err := json.Unmarshal(out.Bytes(), &decision); err != nil {
		t.Fatal(err)
	}
	if decision["decision"] != "block" || decision["reason"] == "" {
		t.Fatalf("missing continuation: %s", out.String())
	}
	out.Reset()
	t.Setenv("GT_ROLE", "mayor")
	if err := runTapDeaconStop(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatal("hook changed another role's turn")
	}
	t.Setenv("GT_ROLE", "deacon")
	t.Chdir(filepath.Join(root, "mayor"))
	if err := runTapDeaconStop(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatal("borrowed role worktree allowed")
	}
}
