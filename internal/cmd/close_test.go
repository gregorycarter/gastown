package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractBeadIDs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "single bead ID",
			args: []string{"gt-abc"},
			want: []string{"gt-abc"},
		},
		{
			name: "multiple bead IDs",
			args: []string{"gt-abc", "gt-def"},
			want: []string{"gt-abc", "gt-def"},
		},
		{
			name: "bead ID with boolean flags",
			args: []string{"--force", "gt-abc", "--suggest-next"},
			want: []string{"gt-abc"},
		},
		{
			name: "bead ID with short boolean flag",
			args: []string{"-f", "gt-abc"},
			want: []string{"gt-abc"},
		},
		{
			name: "bead ID with reason flag (separate value)",
			args: []string{"gt-abc", "--reason", "Done"},
			want: []string{"gt-abc"},
		},
		{
			name: "bead ID with reason flag (= form)",
			args: []string{"gt-abc", "--reason=Done"},
			want: []string{"gt-abc"},
		},
		{
			name: "bead ID with short reason flag",
			args: []string{"-r", "Done", "gt-abc"},
			want: []string{"gt-abc"},
		},
		{
			name: "bead ID with comment alias",
			args: []string{"--comment", "Finished", "gt-abc"},
			want: []string{"gt-abc"},
		},
		{
			name: "bead ID with session flag",
			args: []string{"gt-abc", "--session", "sess-123"},
			want: []string{"gt-abc"},
		},
		{
			name: "bead ID with db flag",
			args: []string{"--db", "/path/to/db", "gt-abc"},
			want: []string{"gt-abc"},
		},
		{
			name: "no bead IDs (flags only)",
			args: []string{"--force", "--reason", "cleanup"},
			want: nil,
		},
		{
			name: "empty args",
			args: []string{},
			want: nil,
		},
		{
			name: "multiple IDs with mixed flags",
			args: []string{"--force", "gt-abc", "--reason", "Done", "hq-cv-xyz", "-v"},
			want: []string{"gt-abc", "hq-cv-xyz"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractBeadIDs(tt.args)
			if len(got) != len(tt.want) {
				t.Fatalf("extractBeadIDs(%v) = %v, want %v", tt.args, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("extractBeadIDs(%v)[%d] = %q, want %q", tt.args, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestExtractCascadeFlag(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantCascade bool
		wantArgs    []string
	}{
		{
			name:        "no cascade flag",
			args:        []string{"gt-abc", "--force"},
			wantCascade: false,
			wantArgs:    []string{"gt-abc", "--force"},
		},
		{
			name:        "cascade flag present",
			args:        []string{"gt-abc", "--cascade"},
			wantCascade: true,
			wantArgs:    []string{"gt-abc"},
		},
		{
			name:        "cascade flag with other flags",
			args:        []string{"--cascade", "gt-abc", "--reason", "Done"},
			wantCascade: true,
			wantArgs:    []string{"gt-abc", "--reason", "Done"},
		},
		{
			name:        "empty args",
			args:        []string{},
			wantCascade: false,
			wantArgs:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCascade, gotArgs := extractCascadeFlag(tt.args)
			if gotCascade != tt.wantCascade {
				t.Errorf("extractCascadeFlag(%v) cascade = %v, want %v", tt.args, gotCascade, tt.wantCascade)
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("extractCascadeFlag(%v) args = %v, want %v", tt.args, gotArgs, tt.wantArgs)
			}
			for i := range gotArgs {
				if gotArgs[i] != tt.wantArgs[i] {
					t.Errorf("extractCascadeFlag(%v) args[%d] = %q, want %q", tt.args, i, gotArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

func TestChildBeadUnmarshal(t *testing.T) {
	jsonData := `[{"id":"gt-abc","status":"open"},{"id":"gt-def","status":"closed"}]`
	var children []childBead
	if err := json.Unmarshal([]byte(jsonData), &children); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("got %d children, want 2", len(children))
	}
	if children[0].ID != "gt-abc" || children[0].Status != "open" {
		t.Errorf("child[0] = %+v, want {ID:gt-abc Status:open}", children[0])
	}
	if children[1].ID != "gt-def" || children[1].Status != "closed" {
		t.Errorf("child[1] = %+v, want {ID:gt-def Status:closed}", children[1])
	}
}

// setupNotifyTestWorkspace creates a temporary town workspace with bd and gt
// stubs for testing notification functions.
func setupNotifyTestWorkspace(t *testing.T, bdResponses map[string]string) (townRoot, gtLogPath string) {
	t.Helper()

	townRoot = t.TempDir()

	// Create workspace markers.
	if err := os.MkdirAll(filepath.Join(townRoot, "mayor"), 0755); err != nil {
		t.Fatalf("mkdir mayor: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(townRoot, ".beads"), 0755); err != nil {
		t.Fatalf("mkdir .beads: %v", err)
	}

	// Write routes so beads routing resolves prefixes to rigs.
	routes := `{"prefix":"bd-","path":"external_rig/.beads"}
{"prefix":"gt-","path":"gastown_rig/.beads"}
`
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"), []byte(routes), 0644); err != nil {
		t.Fatalf("write routes: %v", err)
	}

	// Create rig directories.
	for _, rig := range []string{"external_rig", "gastown_rig"} {
		if err := os.MkdirAll(filepath.Join(townRoot, rig, ".beads"), 0755); err != nil {
			t.Fatalf("mkdir %s: %v", rig, err)
		}
	}

	// Install bd stub.
	binDir := filepath.Join(townRoot, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}

	var bdScript strings.Builder
	bdScript.WriteString("#!/bin/sh\n")
	bdScript.WriteString("case \"$*\" in\n")
	for pattern, response := range bdResponses {
		bdScript.WriteString(fmt.Sprintf("  '%s')\n", pattern))
		bdScript.WriteString(fmt.Sprintf("    echo '%s'\n", response))
		bdScript.WriteString("    exit 0\n")
		bdScript.WriteString("    ;;\n")
	}
	bdScript.WriteString("esac\n")
	bdScript.WriteString("exit 0\n")

	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(bdScript.String()), 0755); err != nil {
		t.Fatalf("write bd stub: %v", err)
	}

	// Install gt stub that logs mail send commands.
	gtLogPath = filepath.Join(townRoot, "gt.log")
	gtScript := fmt.Sprintf(`#!/bin/sh
echo "CMD:$*" >> %q
exit 0
`, gtLogPath)
	if err := os.WriteFile(filepath.Join(binDir, "gt"), []byte(gtScript), 0755); err != nil {
		t.Fatalf("write gt stub: %v", err)
	}

	// Inject bin/ into PATH.
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Change cwd to town root with cleanup.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	if err := os.Chdir(townRoot); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	return townRoot, gtLogPath
}

func TestNotifyCrossRigDependents(t *testing.T) {
	// Build bd show response for a closed external bead with cross-rig dependents.
	showJSON, _ := json.Marshal([]map[string]interface{}{{
		"id":    "bd-closed1",
		"title": "External blocker",
		"dependents": []map[string]interface{}{
			{"id": "gt-dep1", "title": "Blocked task one", "dependency_type": "blocks"},
			{"id": "gt-dep2", "title": "Blocked task two", "dependency_type": "blocks"},
			{"id": "gt-same", "title": "Same rig dep", "dependency_type": "depends_on"},
		},
	}})

	bdResponses := map[string]string{
		"show bd-closed1 --json": string(showJSON),
	}

	_, gtLogPath := setupNotifyTestWorkspace(t, bdResponses)

	notifyCrossRigDependents([]string{"bd-closed1"})

	// Read gt log and verify mail commands.
	logData, err := os.ReadFile(gtLogPath)
	if err != nil {
		t.Fatalf("read gt log: %v", err)
	}
	log := string(logData)

	// Should notify gastown_rig/witness.
	if !strings.Contains(log, "mail send gastown_rig/witness") {
		t.Errorf("expected notification to gastown_rig/witness, got log:\n%s", log)
	}

	// Should mention the closed dependency.
	if !strings.Contains(log, "Dependency resolved: bd-closed1") {
		t.Errorf("expected subject with closed bead id, got log:\n%s", log)
	}

	// Should list unblocked dependents.
	if !strings.Contains(log, "Unblocked: gt-dep1") {
		t.Errorf("expected gt-dep1 in notification, got log:\n%s", log)
	}
	if !strings.Contains(log, "Unblocked: gt-dep2") {
		t.Errorf("expected gt-dep2 in notification, got log:\n%s", log)
	}

	// Should NOT include the depends_on (non-blocks) dependent.
	if strings.Contains(log, "gt-same") {
		t.Errorf("non-blocks dependent gt-same should not appear in notification")
	}
}

func TestNotifyCrossRigDependents_NoCrossRig(t *testing.T) {
	// Same-rig dependents should NOT trigger a notification.
	showJSON, _ := json.Marshal([]map[string]interface{}{{
		"id":    "gt-closed1",
		"title": "Internal blocker",
		"dependents": []map[string]interface{}{
			{"id": "gt-dep1", "title": "Same rig blocked", "dependency_type": "blocks"},
		},
	}})

	bdResponses := map[string]string{
		"show gt-closed1 --json": string(showJSON),
	}

	_, gtLogPath := setupNotifyTestWorkspace(t, bdResponses)

	notifyCrossRigDependents([]string{"gt-closed1"})

	logData, err := os.ReadFile(gtLogPath)
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("read gt log: %v", err)
		}
		// No log file means no notifications were sent — expected.
		return
	}
	if len(logData) > 0 {
		t.Errorf("expected no notifications for same-rig dependents, got log:\n%s", string(logData))
	}
}

func TestNotifyCrossRigDependents_MultipleBeads(t *testing.T) {
	showJSON1, _ := json.Marshal([]map[string]interface{}{{
		"id":    "bd-ext1",
		"title": "External one",
		"dependents": []map[string]interface{}{
			{"id": "gt-depA", "title": "Task A", "dependency_type": "blocks"},
		},
	}})
	showJSON2, _ := json.Marshal([]map[string]interface{}{{
		"id":    "bd-ext2",
		"title": "External two",
		"dependents": []map[string]interface{}{
			{"id": "gt-depB", "title": "Task B", "dependency_type": "blocks"},
		},
	}})

	bdResponses := map[string]string{
		"show bd-ext1 --json": string(showJSON1),
		"show bd-ext2 --json": string(showJSON2),
	}

	_, gtLogPath := setupNotifyTestWorkspace(t, bdResponses)

	notifyCrossRigDependents([]string{"bd-ext1", "bd-ext2"})

	logData, err := os.ReadFile(gtLogPath)
	if err != nil {
		t.Fatalf("read gt log: %v", err)
	}
	log := string(logData)

	if !strings.Contains(log, "bd-ext1") {
		t.Errorf("expected notification for bd-ext1, got log:\n%s", log)
	}
	if !strings.Contains(log, "bd-ext2") {
		t.Errorf("expected notification for bd-ext2, got log:\n%s", log)
	}
}
