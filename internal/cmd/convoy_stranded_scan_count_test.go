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

// strandedRigScans runs findStrandedConvoys against a mock bd with the given
// number of single-issue convoys and returns how many times the rig-wide open
// sling-context and agent-bead listings were issued.
func strandedRigScans(t *testing.T, convoys int) (slingContexts, agentLists int) {
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
	for _, dir := range []string{filepath.Join(townRoot, "rig", ".beads"), filepath.Join(townRoot, "rig", "polecats"), filepath.Join(townRoot, "rig", "mayor", "rig", ".beads")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
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
	return strings.Count(string(calls), "gt:sling-context"), strings.Count(string(calls), "--label=gt:agent")
}

// TestFindStrandedConvoysScansRigsOncePerRun guards the daemon's stranded
// scan: rig-wide sling-context listings must not be repeated for every open
// convoy, and agent beads are not listed at all (stranded detection never
// reads worker identity).
func TestFindStrandedConvoysScansRigsOncePerRun(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping convoy test on Windows")
	}
	contextsOne, agentsOne := strandedRigScans(t, 1)
	contextsFive, agentsFive := strandedRigScans(t, 5)
	if contextsOne == 0 {
		t.Fatalf("expected the stranded scan to list sling contexts, got %d", contextsOne)
	}
	if contextsFive != contextsOne {
		t.Errorf("sling-context scans grew with convoy count: 1 convoy=%d, 5 convoys=%d", contextsOne, contextsFive)
	}
	if agentsOne != 0 || agentsFive != 0 {
		t.Errorf("stranded scan listed agent beads: 1 convoy=%d, 5 convoys=%d; want 0", agentsOne, agentsFive)
	}
}

// TestCheckSingleConvoySkipsAgentListing guards the per-convoy completion
// check the daemon runs: it reads tracked statuses only and must not list
// gt:agent beads in every rig.
func TestCheckSingleConvoySkipsAgentListing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping convoy test on Windows")
	}
	binDir := t.TempDir()
	townRoot := t.TempDir()
	beadsDir := filepath.Join(townRoot, ".beads")
	for _, dir := range []string{beadsDir, filepath.Join(townRoot, "mayor"), filepath.Join(townRoot, "rig", "polecats"), filepath.Join(townRoot, "rig", "mayor", "rig", ".beads")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	writeJSONFile(t, filepath.Join(townRoot, "mayor", "rigs.json"), &config.RigsConfig{Version: config.CurrentRigsVersion, Rigs: map[string]config.RigEntry{"rig": {}}})
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
  sql) echo '[{"depends_on_id":"gt-work1"}]' ;;
  show)
    case "$*" in
      *hq-cv-1*) echo '[{"id":"hq-cv-1","title":"Convoy","status":"open","issue_type":"convoy","labels":[]}]' ;;
      *) echo '[{"id":"gt-work1","title":"Work","status":"open","issue_type":"task","assignee":"","dependencies":[]}]' ;;
    esac ;;
  *) echo '[]' ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(script), 0755); err != nil {
		t.Fatalf("write mock bd: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Chdir(townRoot)

	if err := checkSingleConvoy(beadsDir, "hq-cv-1", true); err != nil {
		t.Fatalf("checkSingleConvoy: %v", err)
	}
	calls, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read call log: %v", err)
	}
	if !strings.Contains(string(calls), "show") {
		t.Fatalf("mock bd was not used: %s", calls)
	}
	if n := strings.Count(string(calls), "--label=gt:agent"); n != 0 {
		t.Errorf("completion check listed agent beads %d time(s); want 0\n%s", n, calls)
	}
}
