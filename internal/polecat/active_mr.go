package polecat

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

// DefaultPhantomMRAge is how old a completion must be before an active_mr that
// resolves in NO database is treated as debris rather than pending work.
//
// Observed 2026-09-01 on bridge_town_core (bt-w25y): four polecats held
// active_mr pointers to MR wisps absent from both databases. pending_mr does
// not count toward scheduler capacity, but it blocks sandbox reuse and
// non-force nuke, so those four sandboxes sat against the 30-directory cap
// with no record that their MRs had ever closed.
const DefaultPhantomMRAge = 24 * time.Hour

// IssueReader is the subset of beads lookup needed to classify active_mr.
type IssueReader interface {
	Show(issueID string) (*beads.Issue, error)
}

// ActiveMRInput describes the active merge-request context for a polecat.
type ActiveMRInput struct {
	ActiveMR        string
	SourceIssueHint string
	RequireGitSafe  bool
	GitSafe         bool

	// TownReader is the second database to consult before declaring an MR bead
	// truly absent. MR wisps live rig-side and agent beads town-side, so a
	// lookup in one database alone cannot prove absence.
	TownReader IssueReader

	// CompletionTime is the agent bead's completion_time (RFC3339). It is the
	// durable timestamp the terminal-MR snapshot writes, and the only evidence
	// available once the MR wisp itself has been compacted away.
	CompletionTime string

	// PhantomReleaseAfter enables the absent-from-both-databases release once
	// CompletionTime is older than this. Zero disables it entirely, which is
	// how workflow.close_on_merge=false keeps upstream behaviour.
	PhantomReleaseAfter time.Duration

	// Now overrides the clock in tests.
	Now time.Time
}

// ActiveMRAssessment is the shared active_mr classification used by recovery,
// reuse, and witness paths. Pending is fail-closed: lookup/source uncertainty
// remains blocking unless the stale MR and terminal source are both proven.
type ActiveMRAssessment struct {
	ActiveMR       string
	Pending        bool
	Reason         string
	MRStatus       string
	SourceIssue    string
	SourceTerminal bool
	Stale          bool

	// PhantomReleased records that the hold was dropped because the MR bead
	// resolves in no database and the completion is older than
	// PhantomReleaseAfter — not because the source was proven terminal.
	PhantomReleased bool
}

// AssessActiveMR returns whether active_mr still represents work pending in the
// merge queue. Missing/terminal MRs are stale only when the source issue is
// known terminal and, if requested, direct git state is safe.
func AssessActiveMR(reader IssueReader, in ActiveMRInput) ActiveMRAssessment {
	mrID := strings.TrimSpace(in.ActiveMR)
	if mrID == "" {
		return ActiveMRAssessment{}
	}
	result := ActiveMRAssessment{ActiveMR: mrID, Pending: true}
	if reader == nil {
		result.Reason = fmt.Sprintf("active_mr=%s status=unverified", mrID)
		return result
	}

	mr, err := reader.Show(mrID)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			return assessStaleActiveMR(reader, in, result, "missing", nil)
		}
		result.Reason = fmt.Sprintf("active_mr=%s status=lookup_error: %v", mrID, err)
		return result
	}
	if mr == nil {
		return assessStaleActiveMR(reader, in, result, "missing", nil)
	}

	result.MRStatus = mr.Status
	if !beads.IssueStatus(mr.Status).IsTerminal() {
		result.Reason = fmt.Sprintf("active_mr=%s status=%s", mrID, mr.Status)
		return result
	}
	return assessStaleActiveMR(reader, in, result, mr.Status, mr)
}

func assessStaleActiveMR(reader IssueReader, in ActiveMRInput, result ActiveMRAssessment, mrStatus string, mr *beads.Issue) ActiveMRAssessment {
	result.MRStatus = mrStatus
	result.Stale = true
	sourceIssue := sourceIssueForActiveMR(in.SourceIssueHint, mr)
	result.SourceIssue = sourceIssue
	terminal, reason := terminalSourceIssue(reader, sourceIssue)
	result.SourceTerminal = terminal
	if !terminal {
		if released, age := phantomActiveMR(in, mrStatus); released {
			// gt polecat reconcile owns anything younger; this only releases
			// holds nothing can ever repair.
			result.Pending = false
			result.Reason = ""
			result.PhantomReleased = true
			log.Printf("polecat: releasing phantom active_mr=%s — absent from rig and town databases, completed %s ago (>%s); source %s",
				result.ActiveMR, age.Round(time.Minute), in.PhantomReleaseAfter, sourceIssueOrUnknown(sourceIssue))
			return result
		}
		result.Reason = fmt.Sprintf("active_mr=%s status=%s %s", result.ActiveMR, mrStatus, reason)
		return result
	}
	if in.RequireGitSafe && !in.GitSafe {
		result.Reason = fmt.Sprintf("active_mr=%s status=%s source_issue=%s git_state=unsafe", result.ActiveMR, mrStatus, sourceIssue)
		return result
	}
	result.Pending = false
	result.Reason = ""
	return result
}

func sourceIssueForActiveMR(hint string, mr *beads.Issue) string {
	if mr != nil {
		if fields := beads.ParseMRFields(mr); fields != nil {
			if source := normalizeSourceIssue(fields.SourceIssue); source != "" {
				return source
			}
		}
	}
	return normalizeSourceIssue(hint)
}

func normalizeSourceIssue(source string) string {
	source = strings.TrimSpace(source)
	if strings.EqualFold(source, "null") {
		return ""
	}
	return source
}

func terminalSourceIssue(reader IssueReader, sourceIssue string) (bool, string) {
	if sourceIssue == "" {
		return false, "source_issue=<missing>"
	}
	if reader == nil {
		return false, fmt.Sprintf("source_issue=%s source_status=unverified", sourceIssue)
	}
	issue, err := reader.Show(sourceIssue)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) {
			return false, fmt.Sprintf("source_issue=%s source_status=missing", sourceIssue)
		}
		return false, fmt.Sprintf("source_issue=%s source_status=lookup_error: %v", sourceIssue, err)
	}
	if issue == nil {
		return false, fmt.Sprintf("source_issue=%s source_status=missing", sourceIssue)
	}
	if beads.IssueStatus(issue.Status).IsTerminal() {
		return true, ""
	}
	return false, fmt.Sprintf("source_issue=%s source_status=%s", sourceIssue, issue.Status)
}

// phantomActiveMR reports whether an active_mr hold is unrepairable debris: the
// MR bead resolves in neither the rig nor the town database, and the agent's
// own completion timestamp is older than PhantomReleaseAfter.
//
// Both halves are required. Absence from one database proves nothing (MR wisps
// are rig-side, agent beads town-side), and a recent absence is exactly the
// window in which gt polecat reconcile repairs the pointer from the terminal
// snapshot — releasing it early would race that repair.
func phantomActiveMR(in ActiveMRInput, mrStatus string) (bool, time.Duration) {
	if in.PhantomReleaseAfter <= 0 || mrStatus != "missing" {
		return false, 0
	}
	if in.TownReader == nil {
		return false, 0 // Cannot prove absence from both databases.
	}
	if issue, err := in.TownReader.Show(strings.TrimSpace(in.ActiveMR)); err == nil && issue != nil {
		return false, 0 // Still resolvable town-side.
	} else if err != nil && !errors.Is(err, beads.ErrNotFound) {
		return false, 0 // A lookup error is not proof of absence.
	}

	completed := strings.TrimSpace(in.CompletionTime)
	if completed == "" {
		return false, 0
	}
	at, err := time.Parse(time.RFC3339, completed)
	if err != nil {
		return false, 0
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}
	age := now.Sub(at)
	if age <= in.PhantomReleaseAfter {
		return false, 0
	}
	return true, age
}

func sourceIssueOrUnknown(sourceIssue string) string {
	if strings.TrimSpace(sourceIssue) == "" {
		return "<unknown>"
	}
	return sourceIssue
}
