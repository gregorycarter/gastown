package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
)

// rejectionMemoryBeads is the beads surface needed to read and clear the
// rejected-candidate record on a source bead.
type rejectionMemoryBeads interface {
	Show(id string) (*beads.Issue, error)
	Update(id string, opts beads.UpdateOptions) error
}

// minSHAPrefix is the shortest abbreviation treated as a real SHA. Anything
// shorter is ambiguous enough that a prefix match would be a coin flip.
const minSHAPrefix = 7

// sameCommit compares two commit SHAs that may be abbreviated to different
// lengths — the Refinery records what the MR bead carried (often 7 chars,
// "9a2bf2d" in hq-tx4md) while git hands us the full 40.
func sameCommit(a, b string) bool {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if len(a) < minSHAPrefix || len(b) < minSHAPrefix {
		return false
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	return strings.HasPrefix(b, a)
}

// ErrRejectedCandidate is returned when a submission repeats a SHA the merge
// gate already refused.
type ErrRejectedCandidate struct {
	IssueID string
	SHA     string
	Branch  string
	Reason  string
}

func (e *ErrRejectedCandidate) Error() string {
	msg := fmt.Sprintf("commit %s on %s was already rejected by the merge gate for %s", e.SHA, e.Branch, e.IssueID)
	if e.Reason != "" {
		msg += "\n  reason: " + e.Reason
	}
	return msg + "\n  Push a fix first: a new commit on the same branch is a new candidate, the same commit is not."
}

// guardRejectedCandidate implements SHA-keyed rejection memory (hq-tx4md).
//
// The Refinery was right in principle — "the unchanged rejected SHA will stay
// untouched until a new SHA arrives" — but had nowhere durable to keep the SHA,
// so a re-push to the same branch with a genuinely different commit
// (9a2bf2d → e1e30e2) was classified as the candidate it had already rejected
// and the fix stalled for hours.
//
// Resubmitting the SAME commit is refused here rather than three hours later in
// the Refinery's backoff. A DIFFERENT commit clears the memory, so the next
// queue scan sees an unencumbered candidate.
func guardRejectedCandidate(bd rejectionMemoryBeads, closeOnMerge bool, issueID, commitSHA string) error {
	if !closeOnMerge || bd == nil || strings.TrimSpace(issueID) == "" || strings.TrimSpace(commitSHA) == "" {
		return nil
	}
	issue, err := bd.Show(issueID)
	if err != nil || issue == nil {
		return nil // Unreadable source: never block a submission on it.
	}
	fields := beads.ParseRejectionFields(issue)
	if fields == nil || fields.RejectedSHA == "" {
		return nil
	}

	if sameCommit(fields.RejectedSHA, commitSHA) {
		return &ErrRejectedCandidate{
			IssueID: issueID,
			SHA:     fields.RejectedSHA,
			Branch:  fields.RejectedBranch,
			Reason:  fields.RejectedReason,
		}
	}

	// New SHA: the rejection no longer describes what is being submitted.
	cleared := beads.SetRejectionFields(issue, nil)
	opts := beads.UpdateOptions{Description: &cleared, RemoveLabels: []string{beads.LabelMRRejected}}
	if err := bd.Update(issueID, opts); err != nil {
		// Non-fatal: a stale record makes the Refinery cautious, not wrong.
		return nil
	}
	return nil
}

// rejectionMemo answers "is this MR a candidate the gate already refused?" for
// a whole queue listing, reading each source bead at most once. Every Show is a
// bd round trip, so a queue of N MRs must not cost N lookups of the same source.
type rejectionMemo struct {
	bd      rejectionMemoryBeads
	enabled bool
	seen    map[string]string
}

func newRejectionMemo(bd rejectionMemoryBeads, enabled bool) *rejectionMemo {
	return &rejectionMemo{bd: bd, enabled: enabled, seen: map[string]string{}}
}

// rejectedSHA returns the SHA recorded as rejected for a source bead, or "".
func (m *rejectionMemo) rejectedSHA(issueID string) string {
	issueID = strings.TrimSpace(issueID)
	if m == nil || !m.enabled || m.bd == nil || issueID == "" {
		return ""
	}
	if sha, ok := m.seen[issueID]; ok {
		return sha
	}
	sha := ""
	if issue, err := m.bd.Show(issueID); err == nil && issue != nil {
		if fields := beads.ParseRejectionFields(issue); fields != nil {
			sha = fields.RejectedSHA
		}
	}
	m.seen[issueID] = sha
	return sha
}

// isRejectedCandidate reports whether an MR's head is a SHA the gate already
// refused for its own source bead.
func (m *rejectionMemo) isRejectedCandidate(fields *beads.MRFields) bool {
	if fields == nil || strings.TrimSpace(fields.CommitSHA) == "" {
		return false
	}
	return sameCommit(m.rejectedSHA(fields.SourceIssue), fields.CommitSHA)
}

// rejectionMemoryFor picks the source-routed beads client when gt done managed
// to resolve one, falling back to the rig client. A nil *beads.Beads in an
// interface is not a nil interface, so the choice must happen on the concrete
// type.
func rejectionMemoryFor(sourceBD, fallback *beads.Beads) rejectionMemoryBeads {
	if sourceBD != nil {
		return sourceBD
	}
	if fallback != nil {
		return fallback
	}
	return nil
}

// closeOnMergeForRig resolves the town setting from a rig path
// (<town>/<rig>), the shape most refinery/MQ commands already hold.
func closeOnMergeForRig(rigPath string) bool {
	return config.CloseOnMergeEnabledForTown(filepath.Dir(strings.TrimRight(rigPath, string(filepath.Separator))))
}
