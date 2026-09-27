package capacity

import "fmt"

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

// WIPDeferral records one bead held back by the WIP cap.
type WIPDeferral struct {
	Bead   PendingBead
	Rig    string
	Reason string
}

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
		deferred = append(deferred, WIPDeferral{Bead: b, Rig: b.TargetRig, Reason: reason})
	}
	return admitted, deferred
}

// PlanDispatchWithWIPCap applies the WIP cap and then PlanDispatch. Deferred
// beads count as skipped; when the cap held back beads the reason carries
// "wip-cap" ("wip-cap" alone when nothing else was dispatchable).
func PlanDispatchWithWIPCap(availableCapacity, batchSize int, ready []PendingBead, wip WIPCap) (DispatchPlan, []WIPDeferral) {
	admitted, deferred := wip.Apply(ready)
	if len(deferred) == 0 {
		return PlanDispatch(availableCapacity, batchSize, ready), nil
	}
	plan := PlanDispatch(availableCapacity, batchSize, admitted)
	plan.Skipped += len(deferred)
	if len(admitted) == 0 || plan.Reason == "none" {
		plan.Reason = "wip-cap"
	} else {
		plan.Reason += "+wip-cap"
	}
	return plan, deferred
}
