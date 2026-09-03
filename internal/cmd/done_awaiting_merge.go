package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
)

// awaitingMergeBeads is the beads surface needed to hold a source bead open
// for a pending merge request. *beads.Beads satisfies it.
type awaitingMergeBeads interface {
	FindOpenMRsForIssue(issueID string) ([]*beads.Issue, error)
	Update(id string, opts beads.UpdateOptions) error
}

// mrHoldDecision records why gt done did or did not hold a source bead open.
type mrHoldDecision struct {
	Hold   bool
	MRID   string
	Reason string
}

// decideMRHold reports whether gt done should hold hookedBead open until its
// merge request lands instead of closing it on submission (hq-19pxc: the source
// bead was closed four seconds after the MR bead was created, so when the gate
// failed there was nothing left watching to reopen it).
//
// "An MR was created" is proven by an open merge-request bead pointing at this
// source, not by a flag: gt done reaches this code after creating the MR, and
// the same evidence is what the Refinery and the convoy feeder read. That keeps
// no-merge, review-only, DEFERRED and MR-creation-failed exits on the existing
// close path with no extra bookkeeping.
func decideMRHold(bd awaitingMergeBeads, closeOnMerge bool, exitType, hookedBeadID string, hookedBead *beads.Issue) mrHoldDecision {
	if !closeOnMerge {
		return mrHoldDecision{Reason: "workflow.close_on_merge=false"}
	}
	if exitType != ExitCompleted {
		return mrHoldDecision{Reason: "exit_type=" + exitType}
	}
	if hookedBeadID == "" || bd == nil {
		return mrHoldDecision{Reason: "no source bead"}
	}
	// Ephemeral formula machinery (molecule wisps, workflow step beads) is not
	// merged — it is burned on completion and must keep closing.
	if strings.Contains(hookedBeadID, "-wisp-") || strings.Contains(hookedBeadID, "-wfs-") {
		return mrHoldDecision{Reason: "ephemeral bead"}
	}
	if fields := beads.ParseAttachmentFields(hookedBead); fields != nil {
		switch {
		case fields.NoMerge:
			return mrHoldDecision{Reason: "no_merge"}
		case fields.ReviewOnly:
			return mrHoldDecision{Reason: "review_only"}
		case strings.EqualFold(strings.TrimSpace(fields.MergeStrategy), "local"):
			return mrHoldDecision{Reason: "merge_strategy:local"}
		}
	}

	openMRs, err := bd.FindOpenMRsForIssue(hookedBeadID)
	if err != nil {
		// Fail closed to the upstream behaviour: an unverifiable queue state
		// must not silently leave work beads open forever.
		return mrHoldDecision{Reason: fmt.Sprintf("MR lookup failed: %v", err)}
	}
	if len(openMRs) == 0 {
		return mrHoldDecision{Reason: "no open MR for source"}
	}

	// Newest MR wins when a supersede pass has not run yet.
	mrID := openMRs[len(openMRs)-1].ID
	return mrHoldDecision{Hold: true, MRID: mrID, Reason: "open MR " + mrID}
}

// holdSourceForMerge parks the source bead in in_progress with no assignee and
// an awaiting-merge:<mr> label. in_progress keeps it out of `bd ready` and out
// of the capacity scheduler's status=="open" readiness test, so nothing
// re-dispatches it while the MR is in the queue; the label is what
// postMergeMR removes and what the convoy feeder and gt doctor read.
func holdSourceForMerge(bd awaitingMergeBeads, hookedBeadID string, hookedBead *beads.Issue, mrID string) error {
	inProgress := string(beads.StatusInProgress)
	noAssignee := ""
	opts := beads.UpdateOptions{
		Status:    &inProgress,
		Assignee:  &noAssignee,
		AddLabels: []string{beads.AwaitingMergeLabel(mrID)},
	}
	for _, existing := range beads.AwaitingMergeLabels(hookedBead) {
		if existing != opts.AddLabels[0] {
			opts.RemoveLabels = append(opts.RemoveLabels, existing)
		}
	}
	return bd.Update(hookedBeadID, opts)
}

// applyMRHoldOrClose is the single decision point gt done uses in place of an
// unconditional Close. It returns true when the bead was held (caller must not
// close it).
func applyMRHoldOrClose(bd awaitingMergeBeads, townRoot, exitType, hookedBeadID string, hookedBead *beads.Issue) bool {
	decision := decideMRHold(bd, config.CloseOnMergeEnabledForTown(townRoot), exitType, hookedBeadID, hookedBead)
	if !decision.Hold {
		fmt.Fprintf(os.Stderr, "Closing source bead %s on done (%s)\n", hookedBeadID, decision.Reason)
		return false
	}
	if err := holdSourceForMerge(bd, hookedBeadID, hookedBead, decision.MRID); err != nil {
		// Non-fatal, but loud: falling through to the close would resurrect the
		// exact bug this replaces, so leave the bead as-is for the Witness.
		fmt.Fprintf(os.Stderr, "Warning: couldn't hold source bead %s for MR %s: %v\n", hookedBeadID, decision.MRID, err)
		return true
	}
	fmt.Fprintf(os.Stderr, "Source bead %s held open (in_progress, %s) until %s lands\n",
		hookedBeadID, beads.AwaitingMergeLabel(decision.MRID), decision.MRID)
	return true
}
