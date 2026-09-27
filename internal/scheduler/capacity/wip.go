package capacity

import (
	"fmt"
	"strings"
)

// WIPCap is the per-rig work-in-progress limit on unlanded merge requests.
// When a rig's pending-MR count reaches its limit, dispatch of NEW work for
// that rig is deferred; work that drains the pile (same-worker MR/dependency
// recovery), P0 beads and landing-unblocking labels still dispatch.
type WIPCap struct {
	// PendingMRs is the number of polecats per rig holding an unlanded MR.
	PendingMRs map[string]int
	// Limit returns the cap for a rig; <= 0 disables it for that rig.
	Limit func(rig string) int
	// ExemptLabels are work-bead labels that bypass the cap.
	ExemptLabels []string
}

// Deferral records one ready bead held back at planning time. Held beads never
// enter the batch, so they cannot use up a batch or capacity slot.
type Deferral struct {
	Bead   PendingBead
	Rig    string
	Kind   string // "wip-cap" | "respawn-limit"
	Reason string
}

// WIPDeferral is kept as an alias for readability at WIP-cap call sites.
type WIPDeferral = Deferral

// Deferral kinds.
const (
	DeferralWIPCap       = "wip-cap"
	DeferralRespawnLimit = "respawn-limit"
)

// WIPCapReason renders the deferral reason for a rig at its cap.
func WIPCapReason(pending, limit int) string {
	return fmt.Sprintf("wip-cap: %d pending MRs >= %d", pending, limit)
}

// RigReason returns the deferral reason for rig, or "" when the rig is under
// its cap (or the cap is disabled).
func (w WIPCap) RigReason(rig string) string {
	if w.Limit == nil {
		return ""
	}
	limit := w.Limit(rig)
	if limit <= 0 {
		return ""
	}
	pending := w.PendingMRs[rig]
	if pending < limit {
		return ""
	}
	return WIPCapReason(pending, limit)
}

// WIPCapExempt reports whether b may dispatch even when its rig is at the cap,
// and why: recovery contexts resume an existing worker on an existing MR (they
// shrink the pile), P0 beads are never held, and exempt labels mark work that
// unblocks landing.
func WIPCapExempt(b PendingBead, exemptLabels []string) (bool, string) {
	if b.Context != nil && b.Context.IsRecovery() {
		return true, "recovery"
	}
	if b.HasPriority && b.Priority == 0 {
		return true, "p0"
	}
	for _, label := range b.Labels {
		for _, exempt := range exemptLabels {
			if label == exempt {
				return true, "label:" + label
			}
		}
	}
	return false, ""
}

// Apply splits ready into beads admitted for planning and beads deferred by
// the WIP cap. Order of admitted beads is preserved.
func (w WIPCap) Apply(ready []PendingBead) ([]PendingBead, []WIPDeferral) {
	var admitted []PendingBead
	var deferred []WIPDeferral
	for _, b := range ready {
		reason := w.RigReason(b.TargetRig)
		if reason == "" {
			admitted = append(admitted, b)
			continue
		}
		if ok, _ := WIPCapExempt(b, w.ExemptLabels); ok {
			admitted = append(admitted, b)
			continue
		}
		deferred = append(deferred, Deferral{Bead: b, Rig: b.TargetRig, Kind: DeferralWIPCap, Reason: reason})
	}
	return admitted, deferred
}

// HoldReady splits ready into beads admitted for planning and beads held by
// hold (a non-empty reason holds the bead). Order is preserved.
func HoldReady(ready []PendingBead, kind string, hold func(PendingBead) string) ([]PendingBead, []Deferral) {
	if hold == nil {
		return ready, nil
	}
	var admitted []PendingBead
	var held []Deferral
	for _, b := range ready {
		if reason := hold(b); reason != "" {
			held = append(held, Deferral{Bead: b, Rig: b.TargetRig, Kind: kind, Reason: reason})
			continue
		}
		admitted = append(admitted, b)
	}
	return admitted, held
}

// PlanDispatchWithWIPCap applies the WIP cap and then PlanDispatch.
func PlanDispatchWithWIPCap(availableCapacity, batchSize int, ready []PendingBead, wip WIPCap) (DispatchPlan, []Deferral) {
	return PlanDispatchWithHolds(availableCapacity, batchSize, ready, wip, nil)
}

// PlanDispatchWithHolds removes per-bead holds before planning: first beads
// whose respawn limit is exhausted (respawnHold returns a reason), then beads
// held by the rig's WIP cap. Held beads count as skipped and never occupy a
// batch or capacity slot, so the next ready beads are planned in their place.
// The plan reason carries each hold kind ("wip-cap", "respawn-limit"), alone
// when nothing else was dispatchable or as a "+kind" suffix otherwise.
func PlanDispatchWithHolds(availableCapacity, batchSize int, ready []PendingBead, wip WIPCap, respawnHold func(PendingBead) string) (DispatchPlan, []Deferral) {
	admitted, deferred := HoldReady(ready, DeferralRespawnLimit, respawnHold)
	admitted, wipDeferred := wip.Apply(admitted)
	deferred = append(deferred, wipDeferred...)
	if len(deferred) == 0 {
		return PlanDispatch(availableCapacity, batchSize, ready), nil
	}
	var kinds []string
	seen := map[string]bool{}
	for _, d := range deferred {
		if !seen[d.Kind] {
			seen[d.Kind] = true
			kinds = append(kinds, d.Kind)
		}
	}
	plan := PlanDispatch(availableCapacity, batchSize, admitted)
	plan.Skipped += len(deferred)
	if len(admitted) == 0 || plan.Reason == "none" {
		plan.Reason = strings.Join(kinds, "+")
	} else {
		plan.Reason += "+" + strings.Join(kinds, "+")
	}
	return plan, deferred
}
