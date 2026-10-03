package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestDoneForceClosesBlockedMoleculeSteps reproduces the mol-polecat-work
// orphan leak: formula steps are chained with blocks deps, and bd refuses a
// plain close of a blocked step. gt done then force-closed the root anyway and
// bd purge deleted it, leaving every step open and parentless. Steps must be
// closed with --force so they finish with their molecule.
func TestDoneForceClosesBlockedMoleculeSteps(t *testing.T) {
	closes := runDoneWithChainedMolecule(t, "hooked")
	for _, id := range []string{"gt-step-1", "gt-step-2", "gt-step-3", "gt-wisp-xyz"} {
		if !strings.Contains(closes, id) {
			t.Errorf("%s was NOT closed (blocked steps need --force)\nClose calls:\n%s", id, closes)
		}
	}
}

// TestDoneClosesMoleculeOfAlreadyClosedBead covers polecats that close their
// own no-change work (bd close --reason "no-changes: ...") before gt done:
// the molecule must still be closed even though the bead is terminal.
func TestDoneClosesMoleculeOfAlreadyClosedBead(t *testing.T) {
	closes := runDoneWithChainedMolecule(t, "closed")
	for _, id := range []string{"gt-step-1", "gt-step-2", "gt-step-3", "gt-wisp-xyz"} {
		if !strings.Contains(closes, id) {
			t.Errorf("%s was NOT closed for an already-closed source bead\nClose calls:\n%s", id, closes)
		}
	}
	if strings.Contains(closes, "gt-base-123") {
		t.Errorf("already-closed source bead must not be closed again\nClose calls:\n%s", closes)
	}
}

func runDoneWithChainedMolecule(t *testing.T, baseStatus string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell script bd stub not supported on Windows")
	}

	townRoot := t.TempDir()
	for _, dir := range []string{"mayor", filepath.Join(".beads", "locks"), "gastown", "bin"} {
		if err := os.MkdirAll(filepath.Join(townRoot, dir), 0755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"),
		[]byte(`{"prefix":"gt-","path":"gastown"}`+"\n"), 0644); err != nil {
		t.Fatalf("write routes.jsonl: %v", err)
	}

	closesLog := filepath.Join(townRoot, "closes.log")
	// close: steps are blocked by their predecessor, so bd only closes them
	// with --force (mirrors "cannot close blocked issue ... use --force").
	bdScript := fmt.Sprintf(`#!/bin/sh
while [ "$1" = "--allow-stale" ]; do shift; done
cmd="$1"
shift || true
case "$cmd" in
  show)
    case "$1" in
      gt-gastown-polecat-nux)
        echo '[{"id":"gt-gastown-polecat-nux","title":"Polecat nux","status":"open","hook_bead":"gt-base-123","agent_state":"working"}]' ;;
      gt-base-123)
        echo '[{"id":"gt-base-123","title":"Base bead","status":"%s","description":"attached_molecule: gt-wisp-xyz"}]' ;;
      gt-wisp-xyz)
        echo '[{"id":"gt-wisp-xyz","title":"mol-polecat-work","status":"open","ephemeral":true}]' ;;
    esac
    ;;
  list)
    if echo "$*" | grep -q "parent=gt-wisp-xyz"; then
      echo '[{"id":"gt-step-1","title":"Load context","status":"open"},{"id":"gt-step-2","title":"Implement","status":"open"},{"id":"gt-step-3","title":"Submit","status":"open"}]'
    else
      echo '[]'
    fi
    ;;
  close)
    force=0
    for arg in "$@"; do [ "$arg" = "--force" ] && force=1; done
    for arg in "$@"; do
      case "$arg" in
        gt-step-2|gt-step-3)
          if [ "$force" = 0 ]; then
            echo "cannot close blocked issue: $arg is blocked (use --force to override)" >&2
            exit 1
          fi ;;
      esac
    done
    for arg in "$@"; do
      case "$arg" in --*) continue ;; esac
      echo "$arg" >> "%s"
    done
    ;;
esac
exit 0
`, baseStatus, closesLog)
	binDir := filepath.Join(townRoot, "bin")
	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(bdScript), 0755); err != nil {
		t.Fatalf("write bd stub: %v", err)
	}

	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GT_ROLE", "polecat")
	t.Setenv("GT_RIG", "gastown")
	t.Setenv("GT_POLECAT", "nux")
	t.Setenv("GT_CREW", "")
	t.Setenv("TMUX_PANE", "")

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	if err := os.Chdir(filepath.Join(townRoot, "gastown")); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	updateAgentStateOnDone(filepath.Join(townRoot, "gastown"), townRoot, ExitCompleted, "gt-base-123")

	closesBytes, err := os.ReadFile(closesLog)
	if err != nil {
		t.Fatalf("no beads were closed: %v", err)
	}
	return string(closesBytes)
}
