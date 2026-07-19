package reaper

import (
	"fmt"
	"path/filepath"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/git"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/rig"
	"github.com/steveyegge/gastown/internal/tmux"
)

// PolecatReconcileResult summarizes worktree reconciliation performed after
// AutoClose closes stale polecat agent beads.
type PolecatReconcileResult struct {
	// Reconciled holds "<rig>/<name>" addresses whose worktree was cleaned up.
	Reconciled []string `json:"reconciled,omitempty"`
	// Skipped holds addresses left alone (no worktree left, or a live tmux
	// session means the closure was premature and the polecat is still working).
	Skipped []string `json:"skipped,omitempty"`
	// Errors holds "<rig>/<name>: <error>" for reconciliation attempts that failed.
	Errors []string `json:"errors,omitempty"`
}

// ReconcilePolecatAgentClosures inspects AutoClose's ClosedEntries for polecat
// agent beads and reconciles each one's worktree, so a leftover worktree
// (blocked by a stash, uncommitted work, etc.) doesn't leak a capacity slot
// forever after its bead is closed as stale. See gt-gjxb / gt-oapo: a closed
// agent bead alone doesn't free the polecat's capacity slot — the snapshot
// still counts a leftover directory as recovery_blocked with no code path
// that ever clears it.
//
// Non-polecat closed entries (regular issues) are ignored. dryRun reports
// what would be reconciled without touching any worktree.
func ReconcilePolecatAgentClosures(townRoot string, entries []ClosedEntry, dryRun bool) *PolecatReconcileResult {
	result := &PolecatReconcileResult{}

	var rigsConfig *config.RigsConfig
	rigMgrs := make(map[string]*rig.Manager)
	polecatMgrs := make(map[string]*polecat.Manager)

	for _, entry := range entries {
		rigName, role, name, ok := beads.ParseAgentBeadID(entry.ID)
		if !ok || role != "polecat" || rigName == "" || name == "" {
			continue
		}
		addr := rigName + "/" + name

		if dryRun {
			result.Reconciled = append(result.Reconciled, addr+" (dry-run)")
			continue
		}

		pm, cached := polecatMgrs[rigName]
		if !cached {
			var err error
			pm, err = loadPolecatManagerForRig(townRoot, rigName, &rigsConfig, rigMgrs)
			polecatMgrs[rigName] = pm
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", addr, err))
				continue
			}
		}
		if pm == nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: rig unavailable", addr))
			continue
		}

		reconciled, err := pm.ReconcileStaleClosedWorktree(name)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", addr, err))
			continue
		}
		if reconciled {
			result.Reconciled = append(result.Reconciled, addr)
		} else {
			result.Skipped = append(result.Skipped, addr)
		}
	}
	return result
}

// loadPolecatManagerForRig loads (and caches) the rigs config and a polecat
// Manager for rigName. Returns a nil Manager with an error if the rig or its
// config can't be loaded — callers should record the error and move on rather
// than aborting the whole reconciliation pass.
func loadPolecatManagerForRig(townRoot, rigName string, rigsConfig **config.RigsConfig, rigMgrs map[string]*rig.Manager) (*polecat.Manager, error) {
	if *rigsConfig == nil {
		cfg, err := config.LoadRigsConfig(filepath.Join(townRoot, "mayor", "rigs.json"))
		if err != nil {
			return nil, fmt.Errorf("loading rigs config: %w", err)
		}
		*rigsConfig = cfg
	}

	rigMgr, ok := rigMgrs[rigName]
	if !ok {
		rigMgr = rig.NewManager(townRoot, *rigsConfig, git.NewGit(townRoot))
		rigMgrs[rigName] = rigMgr
	}

	r, err := rigMgr.GetRig(rigName)
	if err != nil {
		return nil, fmt.Errorf("loading rig: %w", err)
	}

	return polecat.NewManager(r, git.NewGit(r.Path), tmux.NewTmux()), nil
}
