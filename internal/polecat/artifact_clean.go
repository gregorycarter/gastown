package polecat

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/steveyegge/gastown/internal/artifact"
	"github.com/steveyegge/gastown/internal/config"
)

// runArtifactCleanupHook evaluates the per-rig generic artifact policy at the
// pre-reuse lifecycle point. ReuseIdlePolecat has already applied its canonical
// workstate veto; this method repeats that decision before every mutation so an
// MR/assignment/state change during the filesystem scan fails closed.
func (m *Manager) runArtifactCleanupHook(name, clonePath string) (artifact.Result, bool, error) {
	townRoot := filepath.Dir(m.rig.Path)
	policy, err := config.ResolveArtifactCleanupPolicy(townRoot, m.rig.Path)
	if err != nil {
		return artifact.Result{}, true, err
	}
	if !policy.Enabled || !policy.OnPolecatReuse {
		return artifact.Result{}, false, nil
	}

	safetyCheck := func() artifact.SafetyState {
		state := artifact.DetectSafety(clonePath, "polecat")
		current, loadErr := m.loadFromBeads(name)
		if loadErr != nil {
			state.Reasons = append(state.Reasons, "polecat-state-unverified")
			return state
		}
		if current.Issue == "" {
			switch current.State {
			case StateWorking, StateStalled, StateReviewNeeded:
				current.State = StateIdle
			}
		}
		decision := m.reuseDecisionForPolecat(name, current.State)
		state.MRVerified = true
		if !decision.Reusable {
			state.Reasons = append(state.Reasons, "polecat-not-reusable: "+decision.Reason)
		}
		return state
	}
	return executeArtifactCleanupHook(clonePath, policy, safetyCheck)
}

func executeArtifactCleanupHook(clonePath string, policy artifact.Policy, safetyCheck func() artifact.SafetyState) (artifact.Result, bool, error) {
	if !policy.Enabled || !policy.OnPolecatReuse {
		return artifact.Result{}, false, nil
	}
	result, err := artifact.Clean(artifact.Options{
		Root:                  clonePath,
		ProtectionRoot:        clonePath,
		Policy:                policy,
		Apply:                 policy.Mode == artifact.ModeApply,
		Scope:                 "polecat",
		HookPoint:             "polecat-pre-reuse",
		Safety:                safetyCheck(),
		RequireGit:            true,
		RequireIgnored:        true,
		RequireMRVerification: true,
		SafetyCheck:           safetyCheck,
	})
	return result, true, err
}

func formatArtifactHookResult(result artifact.Result) string {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Sprintf("artifact-clean: considered=%d freed=%d refused=%t", result.BytesConsidered, result.BytesFreed, result.Refused)
	}
	return "artifact-clean: " + string(data)
}
