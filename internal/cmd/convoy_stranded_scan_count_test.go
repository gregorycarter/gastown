package cmd

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/steveyegge/gastown/internal/config"
)

// strandedSlingContextScans runs findStrandedConvoys against a mock bd with
// the given number of single-issue convoys and returns how many times the
// open sling-context scan was issued.
func strandedSlingContextScans(t *testing.T, convoys int) int {
	t.Helper()
	binDir := t.TempDir()
	townRoot := t.TempDir()
	beadsDir := filepath.Join(townRoot, ".beads")
	if err := os.MkdirAll(beadsDir, 0755); err != nil {
		t.Fatalf("mkdir beads: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(townRoot, "mayor"), 0755); err != nil {
		t.Fatalf("mkdir mayor: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(townRoot, "rig", ".beads"), 0755); err != nil {
		t.Fatalf("mkdir rig beads: %v", err)
	}
	writeJSONFile(t, filepath.Join(townRoot, "mayor", "rigs.json"), &config.RigsConfig{Version: config.CurrentRigsVersion, Rigs: map[string]config.RigEntry{"rig": {}}})
	if err := os.WriteFile(filepath.Join(beadsDir, "routes.jsonl"), []byte(`{"prefix":"gt-","path":"gastown/mayor/rig"}`+"\n"), 0644); err != nil {
		t.Fatalf("write routes: %v", err)
	}

	var list []string
	for i := 0; i < convoys; i++ {
		list = append(list, `{"id":"hq-scan-`+strconv.Itoa(i)+`","title":"Convoy"}`)
	}
	logPath := filepath.Join(binDir, "calls.log")
	script := `#!/bin/sh
echo "$*" >> "` + logPath + `"
for arg in "$@"; do
  case "$arg" in
    --*) ;;
    *) cmd="$arg"; break ;;
  esac
done
case "$cmd" in
  list) echo '[` + strings.Join(list, ",") + `]' ;;
  sql) echo '[{"depends_on_id":"gt-work1"}]' ;;
  dep) echo '[{"id":"gt-work1","title":"Work","status":"open","issue_type":"task","assignee":"","dependency_type":"tracks"}]' ;;
  show) echo '[{"id":"gt-work1","title":"Work","status":"open","issue_type":"task","assignee":"","blocked_by":[],"blocked_by_count":0,"dependencies":[]}]' ;;
  *) echo '[]' ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(script), 0755); err != nil {
		t.Fatalf("write mock bd: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Chdir(townRoot)

	stranded, err := findStrandedConvoys(townRoot)
	if err != nil {
		t.Fatalf("findStrandedConvoys() error: %v", err)
	}
	if len(stranded) != convoys {
		t.Fatalf("expected %d stranded convoys, got %d", convoys, len(stranded))
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read call log: %v", err)
	}
	return strings.Count(string(calls), "gt:sling-context")
}

// TestFindStrandedConvoysScansSlingContextsOncePerRun guards the daemon's
// 30 s stranded scan: the rig-wide open sling-context listing must not be
// repeated for every open convoy.
func TestFindStrandedConvoysScansSlingContextsOncePerRun(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping convoy test on Windows")
	}
	one := strandedSlingContextScans(t, 1)
	five := strandedSlingContextScans(t, 5)
	if one == 0 {
		t.Fatal("expected the stranded scan to list open sling contexts")
	}
	if five != one {
		t.Fatalf("sling-context scans grew with convoy count: 1 convoy=%d, 5 convoys=%d", one, five)
	}
}
