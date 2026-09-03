package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/steveyegge/gastown/internal/beads"
)

// findCwdBeadsWorkDir finds the nearest .beads directory by walking up from CWD.
// It intentionally ignores BEADS_DIR for callers whose target is implied by
// the current rig worktree rather than inherited session environment.
func findCwdBeadsWorkDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	path := cwd
	for {
		if _, err := os.Stat(filepath.Join(path, ".beads")); err == nil {
			return path, nil
		}

		parent := filepath.Dir(path)
		if parent == path {
			break
		}
		path = parent
	}

	return "", fmt.Errorf("no .beads directory found")
}

// resolveAgentTrackingBeadsDir resolves the bead database used for agent state.
// Agent tracking follows the agent's current rig, so cwd-local redirects must
// win over an inherited town-level BEADS_DIR. The env-first resolver remains a
// fallback for contexts that do not have a cwd-local .beads directory.
func resolveAgentTrackingBeadsDir() (string, error) {
	workDir, err := findCwdBeadsWorkDir()
	if err != nil {
		workDir, err = findLocalBeadsDir()
	}
	if err != nil {
		return "", err
	}

	beadsDir := beads.ResolveBeadsDir(workDir)
	if beadsDir == "" {
		return "", fmt.Errorf("not in a beads workspace")
	}
	return beadsDir, nil
}

// agentBeadExistsIn reports whether agentBead can be read from beadsDir.
func agentBeadExistsIn(agentBead, beadsDir string) bool {
	if agentBead == "" || beadsDir == "" {
		return false
	}
	_, err := getAllAgentLabels(agentBead, beadsDir)
	return err == nil
}

// townAgentTrackingBeadsDir returns the town-level beads dir, or "" if not found.
func townAgentTrackingBeadsDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	townRoot := beads.FindTownRoot(cwd)
	if townRoot == "" {
		return ""
	}
	return beads.ResolveBeadsDir(beads.GetTownBeadsPath(townRoot))
}

// resolveAgentTrackingBeadsDirFor resolves the beads database holding agentBead.
//
// Agent beads are TOWN-owned. That is the codebase's own contract: every
// singular agent-bead operation routes through Beads.agentBeadTarget(), which
// returns ForAgentBead() -- a client rooted unconditionally at the town.
// GetAgentBead, UpdateAgentState, ClearAgentActiveMRIfMatches and the rest all
// honour it.
//
// This resolver did not, and that is hq-dpkp8. It walked up from the CURRENT
// DIRECTORY instead. For a rig-scoped role (Refinery, Witness) the cwd resolves
// to the rig database, which holds no agent beads, so every agent-state read and
// write failed with "no issue found". The idle-count failure warns; the
// heartbeat failure is discarded by its caller, so the loop kept running and
// looked healthy while the heartbeat never advanced -- 44h on the Refinery,
// 105h on the Witness (2026-09-01).
//
// Resolve to the town database first, matching ForAgentBead. Fall back to the
// cwd-local database only when the bead is genuinely not town-side, so an
// isolated or test layout that keeps agent beads rig-side still works.
func resolveAgentTrackingBeadsDirFor(agentBead string) (string, error) {
	local, localErr := resolveAgentTrackingBeadsDir()

	if townDir := townAgentTrackingBeadsDir(); townDir != "" {
		if agentBead == "" || agentBeadExistsIn(agentBead, townDir) {
			return townDir, nil
		}
	}
	if localErr != nil {
		return "", localErr
	}
	return local, nil
}
