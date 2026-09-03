package refinery

import (
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/session"
	"github.com/steveyegge/gastown/internal/tmux"
	"github.com/steveyegge/gastown/internal/util"
)

// RejectOptions controls what a rejection does beyond closing the MR bead.
type RejectOptions struct {
	// Notify nudges a live worker session (the upstream --notify behaviour).
	Notify bool
	// NoResling suppresses re-dispatch when the worker session is dead.
	// The rejection is still recorded and the source bead still reopened.
	NoResling bool
	// CloseOnMerge is the town setting. When false, RejectMR behaves exactly
	// as upstream: the source bead is left untouched and only the nudge fires.
	CloseOnMerge bool
}

// rejectionSourceBeads is the beads surface a rejection needs on the source.
type rejectionSourceBeads interface {
	Show(id string) (*beads.Issue, error)
	Update(id string, opts beads.UpdateOptions) error
	AddComment(id, comment string) error
}

// rejectionOutcome is the audit trail of one rejection, logged by the caller.
type rejectionOutcome struct {
	SourceIssue string
	Recorded    bool // rejected_* fields + MERGE REJECTION comment written
	Reopened    bool // terminal or held source flipped back to open
	HoldCleared bool // awaiting-merge:<mr> removed
	Notes       []string
}

func (o *rejectionOutcome) note(format string, args ...interface{}) {
	o.Notes = append(o.Notes, fmt.Sprintf(format, args...))
}

// mergeRejectionComment is the note the polecat work formula looks for
// (mol-polecat-work.formula.toml:146-158 keys on "MERGE REJECTION" plus a
// branch). Before this, no Go code ever wrote it — the memory was whatever the
// Refinery agent happened to type.
func mergeRejectionComment(mr *MergeRequest, reason string) string {
	return fmt.Sprintf("MERGE REJECTION: branch=%s sha=%s reason=%s",
		strings.TrimSpace(mr.Branch), strings.TrimSpace(mr.CommitSHA), singleLine(reason))
}

// applyRejectionToSource records the rejected candidate on the source work bead
// and puts the bead back into a dispatchable state.
//
// hq-19pxc: gt mq reject's contract is "the source issue is NOT closed (work is
// not done)", which is right only while the source is still open. With the
// source already closed by gt done, the rejection was inert — a no-op with a
// success message.
func applyRejectionToSource(bd rejectionSourceBeads, mr *MergeRequest, reason string) rejectionOutcome {
	outcome := rejectionOutcome{SourceIssue: strings.TrimSpace(mr.IssueID)}
	if outcome.SourceIssue == "" {
		outcome.note("no source issue on MR %s", mr.ID)
		return outcome
	}
	if bd == nil {
		outcome.note("no beads client")
		return outcome
	}

	issue, err := bd.Show(outcome.SourceIssue)
	if err != nil || issue == nil {
		outcome.note("source %s unreadable: %v", outcome.SourceIssue, err)
		return outcome
	}
	if why := beads.ConcreteWorkIssueRejectReason(issue); why != "" {
		outcome.note("source %s is not a concrete work bead (%s)", outcome.SourceIssue, why)
		return outcome
	}

	newDesc := beads.SetRejectionFields(issue, &beads.RejectionFields{
		RejectedSHA:    strings.TrimSpace(mr.CommitSHA),
		RejectedBranch: strings.TrimSpace(mr.Branch),
		RejectedReason: singleLine(reason),
	})
	opts := beads.UpdateOptions{Description: &newDesc, AddLabels: []string{beads.LabelMRRejected}}

	status := beads.IssueStatus(strings.TrimSpace(issue.Status))
	held := beads.AwaitingMergeLabels(issue)
	if len(held) > 0 {
		opts.RemoveLabels = append(opts.RemoveLabels, held...)
		outcome.HoldCleared = true
	}
	switch {
	case status.IsTerminal():
		// The case that made this a bug: a rejection against a closed bead.
		open := string(beads.StatusOpen)
		opts.Status = &open
		outcome.Reopened = true
		outcome.note("source %s was %s — reopened", outcome.SourceIssue, status)
	case len(held) > 0:
		open := string(beads.StatusOpen)
		opts.Status = &open
		outcome.Reopened = true
		outcome.note("source %s released from %v — reopened", outcome.SourceIssue, held)
	default:
		outcome.note("source %s left %s (already dispatchable)", outcome.SourceIssue, status)
	}

	if err := bd.Update(outcome.SourceIssue, opts); err != nil {
		outcome.note("could not record rejection on %s: %v", outcome.SourceIssue, err)
		outcome.Reopened = false
		outcome.HoldCleared = false
		return outcome
	}
	outcome.Recorded = true

	if err := bd.AddComment(outcome.SourceIssue, mergeRejectionComment(mr, reason)); err != nil {
		outcome.note("could not comment on %s: %v", outcome.SourceIssue, err)
	}
	return outcome
}

// workerSessionName maps an MR's worker to its tmux session.
func workerSessionName(rigName, worker string) string {
	worker = strings.TrimSpace(worker)
	worker = strings.TrimPrefix(worker, "polecats/")
	if idx := strings.LastIndex(worker, "/"); idx != -1 {
		worker = worker[idx+1:]
	}
	if worker == "" || rigName == "" {
		return ""
	}
	return session.PolecatSessionName(session.PrefixFor(rigName), worker)
}

// isWorkerSessionAlive reports whether the MR's worker still has a session.
// Uncertainty (no session name, tmux error) is reported as alive: a spurious
// nudge is cheaper than a spurious re-dispatch.
var isWorkerSessionAlive = func(rigName, worker string) bool {
	name := workerSessionName(rigName, worker)
	if name == "" {
		return true
	}
	alive, err := tmux.NewTmux().HasSession(name)
	if err != nil {
		return true
	}
	return alive
}

// reslingRejectedWork re-dispatches a source bead onto its existing branch so a
// polecat picks the work back up with the rejected commits in place.
// `gt sling --branch` resolves to ScheduleOptions{ResumeBranch: ...} in both
// deferred and direct dispatch modes; --force is required because the source
// bead may still be hooked or in_progress from the previous attempt.
var reslingRejectedWork = func(workDir, issueID, rigName, branch string) error {
	args := []string{"sling", issueID, rigName, "--force"}
	if strings.TrimSpace(branch) != "" {
		args = append(args, "--branch", branch)
	}
	cmd := exec.Command("gt", args...)
	util.SetDetachedProcessGroup(cmd)
	cmd.Dir = workDir
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// recoverRejectedWork is the "a rejection must produce a fix attempt" half of
// RejectMR. A live worker gets the nudge it always got; a dead one — the normal
// case, because gt done kills the session on every clean completion — gets the
// work re-dispatched onto its own branch instead of a nudge into the void.
func (m *Manager) recoverRejectedWork(mr *MergeRequest, reason string, opts RejectOptions, outcome *rejectionOutcome) {
	logf := func(format string, args ...interface{}) {
		if m.output != nil {
			_, _ = fmt.Fprintf(m.output, format, args...)
		}
	}

	alive := isWorkerSessionAlive(m.rig.Name, mr.Worker)
	if alive {
		outcome.note("worker %s session alive — nudging", mr.Worker)
		if opts.Notify {
			m.notifyWorkerRejected(mr, reason)
		}
		logf("[Refinery] Rejection for %s: worker %s alive, %s\n",
			mr.IssueID, mr.Worker, nudgeDisposition(opts.Notify))
		return
	}

	outcome.note("worker %s session dead", mr.Worker)
	if opts.NoResling {
		logf("[Refinery] Rejection for %s: worker %s dead, re-sling suppressed (--no-resling)\n", mr.IssueID, mr.Worker)
		return
	}
	if strings.TrimSpace(mr.IssueID) == "" {
		logf("[Refinery] Rejection for %s: worker dead and no source issue to re-sling\n", mr.ID)
		return
	}
	townRoot := filepath.Dir(m.rig.Path)
	if err := reslingRejectedWork(townRoot, mr.IssueID, m.rig.Name, mr.Branch); err != nil {
		outcome.note("re-sling failed: %v", err)
		logf("[Refinery] Warning: could not re-sling %s onto %s: %v\n  repair with: gt sling %s %s --branch %s --force\n",
			mr.IssueID, mr.Branch, err, mr.IssueID, m.rig.Name, mr.Branch)
		return
	}
	outcome.note("re-slung onto %s", mr.Branch)
	logf("[Refinery] Rejection for %s: worker %s dead — re-slung onto %s\n", mr.IssueID, mr.Worker, mr.Branch)
}

func nudgeDisposition(notify bool) string {
	if notify {
		return "nudged"
	}
	return "no nudge requested"
}

func singleLine(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}

// logRejectionOutcome prints the decision trail so a rejection is never silent.
func logRejectionOutcome(out io.Writer, mr *MergeRequest, outcome rejectionOutcome) {
	if out == nil {
		return
	}
	_, _ = fmt.Fprintf(out, "[Refinery] Rejected %s (source %s): recorded=%v reopened=%v hold_cleared=%v\n",
		mr.ID, outcome.SourceIssue, outcome.Recorded, outcome.Reopened, outcome.HoldCleared)
	for _, note := range outcome.Notes {
		_, _ = fmt.Fprintf(out, "  %s\n", note)
	}
}
