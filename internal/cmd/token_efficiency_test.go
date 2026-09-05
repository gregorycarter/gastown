package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

func TestPatrolWakeRouting(t *testing.T) {
	witness := patrolWakePolicy{role: RoleWitness, rig: "bridge_town_core", actor: "bridge_town_core/witness"}
	for _, tt := range []struct {
		name, event  string
		wake, urgent bool
	}{
		{"self prime", `{"type":"session_start","actor":"bridge_town_core/witness"}`, false, false},
		{"other prime", `{"type":"session_start","actor":"bridge_town_core/refinery"}`, false, false},
		{"patrol", `{"type":"patrol_complete","actor":"bridge_town_core/refinery"}`, false, false},
		{"other rig", `{"type":"done","actor":"elsewhere/polecats/nux"}`, false, false},
		{"worker done", `{"type":"done","actor":"bridge_town_core/polecats/nux"}`, true, false},
		{"spawn", `{"type":"spawn","actor":"mayor","payload":{"rig":"bridge_town_core"}}`, true, false},
		{"urgent mail", `{"type":"mail","actor":"mayor/","payload":{"to":"bridge_town_core/witness","subject":"P1: help"}}`, true, true},
		{"other mail", `{"type":"mail","actor":"deacon","payload":{"to":"mayor/","subject":"P1: help"}}`, false, false},
		{"terminal receipt", `{"type":"mail","actor":"bridge_town_core/refinery","payload":{"to":"bridge_town_core/witness","subject":"ACK: merged"}}`, false, false},
		{"worker death", `{"type":"session_death","actor":"daemon","payload":{"agent":"bridge_town_core/polecats/nux"}}`, true, true},
		{"partial JSON", `{"type":`, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wake, urgent := witness.classify(tt.event)
			if wake != tt.wake || urgent != tt.urgent {
				t.Fatalf("got (%v,%v), want (%v,%v)", wake, urgent, tt.wake, tt.urgent)
			}
		})
	}
}

func TestPatrolWakePartialAndUrgentBypass(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go func() {
		time.Sleep(250 * time.Millisecond)
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return
		}
		defer f.Close()
		_, _ = f.WriteString("{\"type\":\"done\",\"actor\":\"rig/polecats/nux\"}\n{\"type\":\"mail\",")
		time.Sleep(250 * time.Millisecond)
		_, _ = f.WriteString("\"actor\":\"mayor\",\"payload\":{\"to\":\"rig/witness\",\"subject\":\"HELP\"}}\n")
	}()
	start := time.Now()
	got, err := waitForEventsFileFiltered(ctx, path, patrolWakePolicy{role: RoleWitness, rig: "rig", actor: "rig/witness", debounce: 10 * time.Second})
	if err != nil || got.Reason != "signal" {
		t.Fatalf("got %+v, %v", got, err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("urgent mail was delayed by debounce")
	}
}

func TestPrimeCheckRejectsSameRoleDifferentWorker(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "rig/polecats/nux/rig")
	if err := os.MkdirAll(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	info := RoleInfo{Role: RolePolecat, Rig: "rig", Polecat: "toast", WorkDir: cwd, TownRoot: root, Home: filepath.Join(root, "rig/polecats/toast")}
	if checkRoleLocation(info) == nil {
		t.Fatal("different worker accepted")
	}
	info.Polecat = "nux"
	info.Home = filepath.Join(root, "rig/polecats/nux")
	if err := checkRoleLocation(info); err != nil {
		t.Fatal(err)
	}
}

func TestPrimeCheckWorkAssignment(t *testing.T) {
	info := RoleInfo{Role: RolePolecat, Rig: "rig", Polecat: "nux"}
	issue := &beads.Issue{ID: "bt-work", Status: "in_progress", Assignee: "rig/polecats/nux"}
	if err := checkWorkIdentity(info, "polecat/nux/bt-work@123", "", issue); err != nil {
		t.Fatal(err)
	}
	issue.Assignee = "rig/polecats/toast"
	if checkWorkIdentity(info, "polecat/nux/bt-work@123", "", issue) == nil {
		t.Fatal("foreign assignment accepted")
	}
	issue.Assignee = "rig/polecats/nux"
	if checkWorkIdentity(info, "polecat/nux/bt-other@123", "", issue) == nil {
		t.Fatal("foreign branch accepted")
	}
	issue.Status = "closed"
	if checkWorkIdentity(info, "polecat/nux/bt-work@123", "", issue) == nil {
		t.Fatal("closed work accepted")
	}
}

func TestPrimeCheckMRDescription(t *testing.T) {
	info := RoleInfo{Role: RoleRefinery}
	issue := &beads.Issue{ID: "bt-wisp-mr", Type: "merge-request", Description: "branch: polecat/nux/bt-work+123\ncommit_sha: " + strings.Repeat("a", 40)}
	if err := checkWorkIdentity(info, "main", "", issue); err != nil {
		t.Fatal(err)
	}
	issue.Description = "branch: work\ncommit_sha: invalid"
	if checkWorkIdentity(info, "main", "", issue) == nil {
		t.Fatal("invalid receipt accepted")
	}
}
