package artifact

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// DetectSafety discovers conservative local safety facts without network calls
// or inspecting process/container environments. Lifecycle callers with richer
// canonical state should OR those facts into the returned value.
func DetectSafety(root, scope string) SafetyState {
	state := SafetyState{}
	if scope == "rig" || scope == "polecat" {
		cmd := exec.Command("git", "-C", root, "status", "--porcelain", "--untracked-files=normal") //nolint:gosec // fixed executable
		if out, err := cmd.Output(); err == nil {
			if strings.TrimSpace(string(out)) != "" {
				state.Dirty = true
			}
		} else {
			state.Reasons = append(state.Reasons, "git-state-unverified")
		}
	}
	mrMarker, mrErr := markerExists(root, map[string]bool{".active-mr": true, "active_mr": true}, 2)
	if mrErr != nil {
		state.Reasons = append(state.Reasons, "mr-state-unverified")
	} else if os.Getenv("GT_ACTIVE_MR") != "" || mrMarker {
		state.ActiveMR = true
	}
	if scope == "ci" {
		state.RunnerHooksVerified = runnerHooksInstalled(root)
		runnerMarker, runnerErr := markerExists(root, map[string]bool{
			".job-starting":   true,
			".runner-active":  true,
			".job-active":     true,
			".handoff-active": true,
		}, 6)
		activeJobEntry, activeJobErr := activeCIJobEntryExists(root)
		if runnerErr != nil || activeJobErr != nil {
			state.ActiveRunner = true
			state.Reasons = append(state.Reasons, "runner-state-unverified")
		} else if os.Getenv("GITHUB_ACTIONS") == "true" || os.Getenv("RUNNER_TRACKING_ID") != "" || runnerMarker || activeJobEntry {
			state.ActiveRunner = true
		}
	}
	return state
}

func activeCIJobEntryExists(root string) (bool, error) {
	entries, err := os.ReadDir(filepath.Join(root, "shared", ".ci-active-jobs"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(entries) > 0, nil
}

func runnerHooksInstalled(root string) bool {
	workRoot := filepath.Join(root, "work")
	entries, err := os.ReadDir(workRoot)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		return false
	}
	foundRunner := false
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runnerRoot := filepath.Join(workRoot, entry.Name())
		if !looksLikeRunnerWorkdir(runnerRoot) {
			continue
		}
		foundRunner = true
		sentinel := filepath.Join(workRoot, entry.Name(), ".ci-job-hooks-installed")
		data, readErr := os.ReadFile(sentinel)
		if readErr != nil || string(data) != CIHookProtocolVersion+"\n" {
			return false
		}
	}
	return foundRunner
}

func looksLikeRunnerWorkdir(root string) bool {
	for _, marker := range []string{"_actions", "_temp", ".pnpm-store"} {
		if info, err := os.Stat(filepath.Join(root, marker)); err == nil && info.IsDir() {
			return true
		}
	}
	return false
}

func markerExists(root string, names map[string]bool, maxDepth int) (bool, error) {
	found := false
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil
		}
		depth := len(strings.Split(filepath.ToSlash(rel), "/"))
		if entry.IsDir() && depth > maxDepth {
			return filepath.SkipDir
		}
		if !entry.IsDir() && names[entry.Name()] {
			found = true
		}
		return nil
	})
	return found, err
}
