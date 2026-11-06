package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/steveyegge/gastown/internal/scheduler/capacity"
)

// Andon state is owned by the incident controller. Dispatch only reads it: a
// missing file keeps ordinary operation; an unreadable or invalid file preserves
// the rig's hold. Priority, --force and worker recovery never bypass the list.
type andonHoldError struct{ reason string }

func (e *andonHoldError) Error() string { return e.reason }

var andonRigName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func checkAndonDispatch(townRoot, rig, workBeadID string) error {
	if townRoot == "" || rig == "" {
		return nil
	}
	if !andonRigName.MatchString(rig) {
		return &andonHoldError{fmt.Sprintf("andon: invalid rig %q", rig)}
	}
	path := filepath.Join(townRoot, ".runtime", "andon", rig+".json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	invalid := func() error {
		return &andonHoldError{fmt.Sprintf("andon: %s state unavailable or invalid; preserve recovery hold", rig)}
	}
	if err != nil {
		return invalid()
	}
	var state struct {
		SchemaVersion *int   `json:"schema_version"`
		Active        *bool  `json:"active"`
		AllowedBeads  *[]any `json:"allowed_beads"`
		IncidentID    string `json:"incident_id"`
	}
	if json.Unmarshal(data, &state) != nil || state.SchemaVersion == nil || *state.SchemaVersion != 1 ||
		state.Active == nil || state.AllowedBeads == nil {
		return invalid()
	}
	allowed := false
	for _, value := range *state.AllowedBeads {
		bead, ok := value.(string)
		if !ok {
			return invalid()
		}
		allowed = allowed || (workBeadID != "" && bead == workBeadID)
	}
	if !*state.Active || allowed {
		return nil
	}
	return &andonHoldError{fmt.Sprintf("andon: %s recovery %s holds work %s; only listed recovery beads may start", rig, state.IncidentID, workBeadID)}
}

func schedulerAndonHold(townRoot string) func(capacity.PendingBead) string {
	return func(b capacity.PendingBead) string {
		if err := checkAndonDispatch(townRoot, b.TargetRig, b.WorkBeadID); err != nil {
			return err.Error()
		}
		return ""
	}
}

// Filter before capacity planning so an affected rig at the queue head cannot
// consume slots needed by remediation or by another rig.
func planDispatchWithAndon(townRoot string, free, batch int, ready []capacity.PendingBead, wip capacity.WIPCap,
	respawnHold func(capacity.PendingBead) string) (capacity.DispatchPlan, []capacity.Deferral) {
	admitted, held := capacity.HoldReady(ready, "andon", schedulerAndonHold(townRoot))
	plan, deferred := capacity.PlanDispatchWithHolds(free, batch, admitted, wip, respawnHold)
	if len(held) > 0 {
		plan.Skipped += len(held)
		if plan.Reason == "none" {
			plan.Reason = "andon"
		} else {
			plan.Reason += "+andon"
		}
	}
	return plan, append(held, deferred...)
}

func annotateAndon(assessments []scheduledContextAssessment, townRoot string) {
	hold := schedulerAndonHold(townRoot)
	for i := range assessments {
		a := &assessments[i]
		a.andonHold = ""
		if a.ready && a.fields != nil {
			a.andonHold = hold(pendingBeadFromAssessment(*a))
		}
	}
}

// Targets may be bare rigs or explicit rig/worker paths. Town roles have no
// matching per-rig state. Self-sling calls this after resolving its agent ID.
func checkAndonTarget(townRoot, target, workBeadID string) error {
	if target == "" || target == "." {
		return nil
	}
	return checkAndonDispatch(townRoot, strings.Split(target, "/")[0], workBeadID)
}
