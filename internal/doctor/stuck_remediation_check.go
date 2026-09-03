package doctor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/mail"
	"github.com/steveyegge/gastown/internal/session"
	"github.com/steveyegge/gastown/internal/tmux"
)

// StuckRemediationCheck reports merge requests that read as healthy pending
// work but cannot move: the queue says "ready", the Refinery is busy, the gate
// is green and no heartbeat is stale, yet nothing in the system can produce the
// correction the MR is waiting for (hq-19pxc).
//
// Three shapes qualify as "awaiting correction":
//
//   - the source bead is terminal, so a rejection has nothing to reopen and no
//     polecat can ever be dispatched against it;
//   - the head SHA is the one the merge gate already refused (hq-tx4md), so the
//     candidate is a replay, not a fix;
//   - the worker has no live session AND there is unread rejection/FIX_NEEDED
//     mail for it, i.e. the remediation request was delivered to nobody.
//
// Report-only: the repair is a human or Mayor decision (reject, re-sling, or
// close the MR), not something a doctor pass should guess at.
type StuckRemediationCheck struct {
	BaseCheck
}

// NewStuckRemediationCheck creates the stuck-remediation check.
func NewStuckRemediationCheck() *StuckRemediationCheck {
	return &StuckRemediationCheck{
		BaseCheck: BaseCheck{
			CheckName:        "stuck-remediation",
			CheckDescription: "Detect merge requests that report ready but cannot be corrected",
			CheckCategory:    CategoryCleanup,
		},
	}
}

type stuckMR struct {
	rig     string
	mrID    string
	source  string
	branch  string
	worker  string
	reasons []string
}

// Run inspects every rig's open merge queue.
func (c *StuckRemediationCheck) Run(ctx *CheckContext) *CheckResult {
	result := &CheckResult{Name: c.Name(), Category: c.Category(), Status: StatusOK}

	if !config.CloseOnMergeEnabledForTown(ctx.TownRoot) {
		result.Message = "skipped (workflow.close_on_merge is off)"
		return result
	}

	var stuck []stuckMR
	scanned := 0
	for _, rigName := range c.findRigs(ctx) {
		rigPath := filepath.Join(ctx.TownRoot, rigName)
		b := beads.New(rigPath)
		mrs, err := b.ListMergeRequests(beads.ListOptions{
			Status:   "open",
			Label:    "gt:merge-request",
			Priority: -1,
			Rig:      rigName,
		})
		if err != nil {
			continue
		}
		for _, mr := range mrs {
			if mr.Status != "open" {
				continue
			}
			fields := beads.ParseMRFields(mr)
			if fields == nil {
				continue
			}
			// Wisps are shared across rigs in the Dolt server.
			if fields.Rig != "" && !strings.EqualFold(fields.Rig, rigName) {
				continue
			}
			scanned++
			// A blocked MR already shows as blocked; it is not the silent case.
			if beads.HasUnresolvedBlockers(mr) {
				continue
			}
			if reasons := classifyStuckMR(b, ctx.TownRoot, rigName, fields); len(reasons) > 0 {
				stuck = append(stuck, stuckMR{
					rig: rigName, mrID: mr.ID, source: fields.SourceIssue,
					branch: fields.Branch, worker: fields.Worker, reasons: reasons,
				})
			}
		}
	}

	if len(stuck) == 0 {
		result.Message = fmt.Sprintf("no stuck remediations (%d open MRs scanned)", scanned)
		return result
	}

	result.Status = StatusWarning
	result.Message = fmt.Sprintf("%d merge request(s) awaiting correction with no producer of a fix", len(stuck))
	for _, s := range stuck {
		result.Details = append(result.Details, fmt.Sprintf("%s/%s awaiting correction: source=%s branch=%s worker=%s (%s)",
			s.rig, s.mrID, orNone(s.source), orNone(s.branch), orNone(s.worker), strings.Join(s.reasons, "; ")))
	}
	result.FixHint = "Reject the MR to reopen and re-dispatch its source (gt mq reject <rig> <mr> -r '<reason>'), or close it if the work is obsolete."
	return result
}

// classifyStuckMR returns the reasons an otherwise-ready MR cannot move.
func classifyStuckMR(reader stuckSourceReader, townRoot, rigName string, fields *beads.MRFields) []string {
	var reasons []string

	source := strings.TrimSpace(fields.SourceIssue)
	var sourceIssue *beads.Issue
	if source != "" && reader != nil {
		if issue, err := reader.Show(source); err == nil && issue != nil {
			sourceIssue = issue
		}
	}

	switch {
	case source == "":
		reasons = append(reasons, "MR has no source issue")
	case sourceIssue == nil:
		reasons = append(reasons, "source issue not readable")
	case beads.IssueStatus(strings.TrimSpace(sourceIssue.Status)).IsTerminal():
		// The hq-19pxc shape: closed at submit, so the rejection was inert.
		reasons = append(reasons, "source is "+strings.TrimSpace(sourceIssue.Status))
	}

	if sourceIssue != nil && strings.TrimSpace(fields.CommitSHA) != "" {
		if rf := beads.ParseRejectionFields(sourceIssue); rf != nil && rf.RejectedSHA != "" {
			if sameCommitSHA(rf.RejectedSHA, fields.CommitSHA) {
				reasons = append(reasons, "head "+fields.CommitSHA+" is the already-rejected candidate")
			}
		}
	}

	if !workerSessionAlive(rigName, fields.Worker) {
		if subject := unreadCorrectionMail(townRoot, rigName, fields.Worker); subject != "" {
			reasons = append(reasons, fmt.Sprintf("worker %s has no session and unread %q", fields.Worker, subject))
		} else if sourceIssue != nil && beads.HasLabel(sourceIssue, beads.LabelMRRejected) {
			reasons = append(reasons, fmt.Sprintf("worker %s has no session and the source is labelled %s", fields.Worker, beads.LabelMRRejected))
		}
	}

	return reasons
}

type stuckSourceReader interface {
	Show(id string) (*beads.Issue, error)
}

// workerSessionAlive is a var so tests can run without tmux.
var workerSessionAlive = func(rigName, worker string) bool {
	worker = strings.TrimPrefix(strings.TrimSpace(worker), "polecats/")
	if idx := strings.LastIndex(worker, "/"); idx != -1 {
		worker = worker[idx+1:]
	}
	if worker == "" || rigName == "" {
		return true // Unknown worker: not evidence of a stall.
	}
	alive, err := tmux.NewTmux().HasSession(session.PolecatSessionName(session.PrefixFor(rigName), worker))
	if err != nil {
		return true
	}
	return alive
}

// unreadCorrectionMail returns the subject of an unread FIX_NEEDED / rejection
// message waiting in the worker's inbox, or "".
var unreadCorrectionMail = func(townRoot, rigName, worker string) string {
	worker = strings.TrimPrefix(strings.TrimSpace(worker), "polecats/")
	if worker == "" || rigName == "" || townRoot == "" {
		return ""
	}
	workDir := filepath.Join(townRoot, rigName, "polecats", worker, rigName)
	if _, err := os.Stat(workDir); err != nil {
		workDir = filepath.Join(townRoot, rigName)
	}
	mb := mail.NewMailboxFromAddress(fmt.Sprintf("%s/polecats/%s", rigName, worker), workDir)
	msgs, err := mb.ListUnread()
	if err != nil {
		return ""
	}
	for _, msg := range msgs {
		subject := strings.ToUpper(msg.Subject)
		if strings.Contains(subject, "FIX_NEEDED") || strings.Contains(subject, "REJECT") ||
			strings.Contains(strings.ToUpper(msg.Body), "MERGE REJECTION") {
			return msg.Subject
		}
	}
	return ""
}

// sameCommitSHA compares possibly-abbreviated SHAs (7-char floor).
func sameCommitSHA(a, b string) bool {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if len(a) < 7 || len(b) < 7 {
		return false
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	return strings.HasPrefix(b, a)
}

func orNone(s string) string {
	if strings.TrimSpace(s) == "" {
		return "(none)"
	}
	return s
}

// findRigs returns the rigs to scan.
func (c *StuckRemediationCheck) findRigs(ctx *CheckContext) []string {
	if ctx.RigName != "" {
		return []string{ctx.RigName}
	}
	entries, err := os.ReadDir(ctx.TownRoot)
	if err != nil {
		return nil
	}
	var rigs []string
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || entry.Name() == "mayor" {
			continue
		}
		if info, err := os.Stat(filepath.Join(ctx.TownRoot, entry.Name(), "polecats")); err == nil && info.IsDir() {
			rigs = append(rigs, entry.Name())
		}
	}
	return rigs
}
