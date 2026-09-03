package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/style"
)

// orphanWispGracePeriod is how long a wisp is immune from the orphan sweep.
// A polecat that has just been dispatched may not have a tmux session yet,
// and a session that died seconds ago may be mid-restart. Ten minutes matches
// the Mayor's own "never nuke a polecat younger than 10 minutes" rule.
const orphanWispGracePeriod = 10 * time.Minute

// orphanWispCloseReason is recorded on every wisp the sweep closes.
const orphanWispCloseReason = "orphaned molecule: polecat session gone"

// isWispID reports whether a bead ID names an ephemeral wisp
// (e.g. bt-wisp-nod3m, hq-wisp-dmbqwa).
func isWispID(id string) bool {
	return strings.Contains(id, "-wisp-")
}

// orphanWispCandidates returns the wisp blockers that may be closed to unblock
// a work bead, or nil when the bead must be left alone.
//
// A bead qualifies only when EVERY blocker is a wisp whose owning polecat has
// no live session and which is older than the grace period. One real blocker,
// one live owner, or one unreadable wisp and the whole bead is skipped: a
// half-unblocked bead would dispatch into work someone else is still doing.
func orphanWispCandidates(
	blockers []string,
	blockerInfo map[string]beadStatusInfo,
	workAssignee string,
	now time.Time,
	sessionDead func(assignee string) bool,
) []string {
	if len(blockers) == 0 {
		return nil
	}

	candidates := make([]string, 0, len(blockers))
	for _, blocker := range blockers {
		if !isWispID(blocker) {
			return nil // a real dependency — the bead is genuinely blocked
		}
		info, found := blockerInfo[blocker]
		if !found {
			return nil // cannot read it, cannot prove it is orphaned
		}
		if beads.IssueStatus(info.Status).IsTerminal() {
			continue // already closed; not what is holding the bead
		}
		if info.CreatedAt == "" {
			return nil
		}
		created, err := time.Parse(time.RFC3339, info.CreatedAt)
		if err != nil {
			return nil
		}
		if now.Sub(created) < orphanWispGracePeriod {
			return nil // too young: its polecat may still be starting up
		}

		owner := strings.TrimSpace(info.Assignee)
		if owner == "" {
			owner = strings.TrimSpace(workAssignee)
		}
		if owner != "" && !sessionDead(owner) {
			return nil // the polecat is alive and working through the molecule
		}
		candidates = append(candidates, blocker)
	}

	if len(candidates) == 0 {
		return nil
	}
	return candidates
}

// sweepOrphanWispBlockers closes mol-polecat-work wisps that are the only
// thing blocking a scheduled work bead and whose polecat is gone. Returns the
// number of wisps closed (or, in dry-run, that would be closed).
//
// Wisps are burned by `gt polecat nuke`, `gt sling` re-attach and `gt done`,
// but never by idle-reap or crash detection — so a killed polecat leaves the
// wisp bonded, the bead drops out of `bd ready`, and the scheduler reports
// "0 ready" until an operator runs `bd close --force` by hand.
func sweepOrphanWispBlockers(townRoot string, assessments []scheduledContextAssessment, dryRun bool) int {
	var wispIDs []string
	var pending []scheduledContextAssessment
	for _, a := range assessments {
		if a.ready || !a.blocked || len(a.blockers) == 0 {
			continue
		}
		allWisps := true
		for _, blocker := range a.blockers {
			if !isWispID(blocker) {
				allWisps = false
				break
			}
		}
		if !allWisps {
			continue
		}
		pending = append(pending, a)
		wispIDs = append(wispIDs, a.blockers...)
	}
	if len(pending) == 0 {
		return 0
	}

	blockerInfo := batchFetchBeadInfoByIDs(townRoot, wispIDs)
	now := time.Now()

	closed := 0
	for _, a := range pending {
		candidates := orphanWispCandidates(a.blockers, blockerInfo, a.info.Assignee, now, isHookedAgentDeadFn)
		if len(candidates) == 0 {
			continue
		}

		workBeadID := ""
		if a.fields != nil {
			workBeadID = a.fields.WorkBeadID
		}
		if dryRun {
			for _, wisp := range candidates {
				fmt.Printf("%s Would close orphaned molecule %s blocking %s\n",
					style.Dim.Render("○"), wisp, workBeadID)
				closed++
			}
			continue
		}

		bd := beadsForContextRecord(a.context)
		for _, wisp := range candidates {
			// Close the molecule's own steps first: bd close does not
			// cascade, and open steps keep the root un-closable.
			if _, err := forceCloseDescendants(bd, wisp); err != nil {
				style.PrintWarning("orphan-wisp sweep: could not close steps of %s: %v", wisp, err)
			}
			if err := bd.ForceCloseWithReason(orphanWispCloseReason, wisp); err != nil {
				style.PrintWarning("orphan-wisp sweep: could not close %s: %v", wisp, err)
				continue
			}
			if workBeadID != "" {
				removeMoleculeBonds(bd, workBeadID, wisp)
			}
			closed++
			fmt.Printf("%s Closed orphaned molecule %s — unblocks %s (%s)\n",
				style.Bold.Render("✓"), wisp, workBeadID, orphanWispCloseReason)
		}
	}
	return closed
}

// detachOrphanedMolecule burns the molecule attached to a work bead and
// removes its dependency bonds, so the bead is dispatchable again. Shared by
// `gt unsling` and the orphan-wisp sweep; `gt polecat nuke` has its own copy
// that also resets the polecat's agent bead.
//
// Best effort: every failure is a warning, never an error. The caller has
// already done the thing that matters (clearing the hook).
func detachOrphanedMolecule(bd *beads.Beads, workBeadID, reason string) {
	if bd == nil || workBeadID == "" {
		return
	}
	issue, err := bd.Show(workBeadID)
	if err != nil || issue == nil {
		return
	}
	attachment := beads.ParseAttachmentFields(issue)
	if attachment == nil || attachment.AttachedMolecule == "" {
		return
	}
	moleculeID := attachment.AttachedMolecule

	if _, err := forceCloseDescendants(bd, moleculeID); err != nil {
		style.PrintWarning("could not close steps of %s: %v", moleculeID, err)
	}
	if _, err := bd.DetachMoleculeWithAudit(workBeadID, beads.DetachOptions{
		Operation: "burn",
		Reason:    reason,
	}); err != nil {
		style.PrintWarning("could not detach molecule %s from %s: %v", moleculeID, workBeadID, err)
		return
	}
	removeMoleculeBonds(bd, workBeadID, moleculeID)
	if err := bd.ForceCloseWithReason(reason, moleculeID); err != nil {
		style.PrintWarning("could not close molecule %s: %v", moleculeID, err)
		return
	}
	fmt.Printf("  %s burned stale molecule %s from work bead %s\n",
		style.Success.Render("✓"), moleculeID, workBeadID)
}
