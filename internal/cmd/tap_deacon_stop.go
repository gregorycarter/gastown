package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/daemon"
	"github.com/steveyegge/gastown/internal/deacon"
	"github.com/steveyegge/gastown/internal/workspace"
)

var tapDeaconStopCmd = &cobra.Command{
	Use:          "deacon-stop-check",
	Short:        "Keep an enabled Deacon patrol from ending at an idle prompt",
	RunE:         runTapDeaconStop,
	SilenceUsage: true,
}

func init() { tapCmd.AddCommand(tapDeaconStopCmd) }

// Stop hooks in Claude Code and Codex share the decision/reason response.
// This hook neither writes a synthetic heartbeat nor starts background work.
// The next model turn must execute the bounded wait and resume the patrol.
func runTapDeaconStop(cmd *cobra.Command, _ []string) error {
	if os.Getenv("GT_ROLE") != "deacon" {
		return nil
	}
	townRoot, err := workspace.FindFromCwd()
	if err != nil || townRoot == "" {
		return nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	// Do not attach continuous patrol behavior to a borrowed role or worktree.
	home, err := filepath.EvalSymlinks(filepath.Join(townRoot, "deacon"))
	if err != nil {
		return nil
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil || cwd != home {
		return nil
	}
	if !deaconShouldContinue(townRoot) {
		return nil
	}
	return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]string{
		"decision": "block",
		"reason":   "Deacon is a continuous patrol. Do not finish at the input prompt or merely claim to be waiting. Run gt prime --check, refresh your own heartbeat, then execute gt mol step await-signal --agent-bead hq-deacon --backoff-base 5m --backoff-mult 2 --backoff-max 15m as a foreground tool call. Wait for its completion; on signal or timeout run the appropriate full/reduced patrol, report it, and repeat. Preserve the existing hook; do not create duplicate patrols, shell loops, background waits, or synthetic heartbeats. Honor an explicit human pause/shutdown and existing recovery safety holds.",
	})
}

func deaconShouldContinue(townRoot string) bool {
	if daemon.IsShutdownInProgress(townRoot) {
		return false
	}
	paused, _, err := deacon.IsPaused(townRoot)
	if err != nil || paused {
		return false
	}
	if !daemon.IsPatrolEnabled(daemon.LoadPatrolConfig(townRoot), "deacon") {
		return false
	}
	// The daemon also honors town-level disabled_patrols.
	data, err := os.ReadFile(filepath.Join(townRoot, "settings", "config.json"))
	if err == nil {
		var settings struct {
			DisabledPatrols []string `json:"disabled_patrols"`
		}
		if json.Unmarshal(data, &settings) != nil {
			return false
		}
		for _, role := range settings.DisabledPatrols {
			if role == "deacon" {
				return false
			}
		}
	} else if !os.IsNotExist(err) {
		return false
	}
	return true
}
