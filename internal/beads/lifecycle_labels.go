package beads

import "strings"

// Lifecycle labels for the close-on-landed workflow (town setting
// workflow.close_on_merge). See GAS_TOWN_DEV_CYCLE_REVIEW_2026-09-02 item 6.
const (
	// AwaitingMergeLabelPrefix marks a source bead that has been submitted to
	// the merge queue and is held open until its MR lands. The suffix is the
	// MR bead ID, so the queue entry is recoverable from the source bead alone.
	AwaitingMergeLabelPrefix = "awaiting-merge:"

	// LabelMRRejected marks a source bead whose MR was rejected by the merge
	// gate. It is informational — the durable rejection data lives in the
	// rejected_sha / rejected_branch / rejected_reason description fields.
	LabelMRRejected = "mr-rejected"
)

// AwaitingMergeLabel returns the awaiting-merge label for an MR bead ID.
// An empty MR ID yields the bare prefix-less marker "awaiting-merge:unknown"
// so the bead is still recognisably held rather than silently unlabelled.
func AwaitingMergeLabel(mrID string) string {
	mrID = strings.TrimSpace(mrID)
	if mrID == "" {
		mrID = "unknown"
	}
	return AwaitingMergeLabelPrefix + mrID
}

// AwaitingMergeLabels returns every awaiting-merge:* label on the issue.
// More than one means an earlier hold was never cleared.
func AwaitingMergeLabels(issue *Issue) []string {
	if issue == nil {
		return nil
	}
	var found []string
	for _, l := range issue.Labels {
		if strings.HasPrefix(l, AwaitingMergeLabelPrefix) {
			found = append(found, l)
		}
	}
	return found
}

// AwaitingMergeMR returns the MR bead ID a source bead is held for, or "" when
// the bead carries no awaiting-merge label.
func AwaitingMergeMR(issue *Issue) string {
	for _, l := range AwaitingMergeLabels(issue) {
		if id := strings.TrimSpace(strings.TrimPrefix(l, AwaitingMergeLabelPrefix)); id != "" && id != "unknown" {
			return id
		}
	}
	return ""
}

// IsAwaitingMerge reports whether a source bead is held open for a pending MR.
func IsAwaitingMerge(issue *Issue) bool {
	return len(AwaitingMergeLabels(issue)) > 0
}

// LabelsContainAwaitingMerge is the string-slice form used by callers that
// only have raw labels (convoy tracking data, JSON payloads).
func LabelsContainAwaitingMerge(labels []string) bool {
	for _, l := range labels {
		if strings.HasPrefix(l, AwaitingMergeLabelPrefix) {
			return true
		}
	}
	return false
}
