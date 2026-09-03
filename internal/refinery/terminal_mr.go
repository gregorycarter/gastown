package refinery

import (
	"time"
	"errors"
	"fmt"
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
)

type terminalMRCloseOptions struct {
	Reason        string
	MergeCommit   string
	AgentBeadHint string
	MissingOK     bool
	ExpectedMR    *MergeRequest
}

type terminalMRCloseResult struct {
	MRID                  string
	SourceIssue           string
	AgentBead             string
	Closed                bool
	AlreadyTerminal       bool
	AgentActiveMRCleared  bool
	AgentActiveMRClearErr error
	// TerminalSnapshotErr records a failure to persist the durable terminal
	// snapshot. A non-nil value means a failed clear may NOT be repairable.
	TerminalSnapshotErr error
}

func closeTerminalMR(b *beads.Beads, mrID string, opts terminalMRCloseOptions) (*terminalMRCloseResult, error) {
	mrID = strings.TrimSpace(mrID)
	result := &terminalMRCloseResult{MRID: mrID}
	if b == nil || mrID == "" {
		return result, nil
	}

	issue, err := b.Show(mrID)
	if err != nil {
		if errors.Is(err, beads.ErrNotFound) && opts.MissingOK {
			return result, nil
		}
		return result, fmt.Errorf("fetch MR for close: %w", err)
	}
	if issue == nil {
		return result, nil
	}

	fields := beads.ParseMRFields(issue)
	if fields == nil {
		fields = &beads.MRFields{}
	}
	result.SourceIssue = strings.TrimSpace(fields.SourceIssue)
	result.AgentBead = firstNonEmpty(opts.AgentBeadHint, fields.AgentBead)
	if err := validateTerminalMRCloseSnapshot(mrID, fields, opts.ExpectedMR); err != nil {
		return result, err
	}

	status := beads.IssueStatus(strings.TrimSpace(issue.Status))
	switch {
	case status == beads.StatusOpen:
		if opts.MergeCommit != "" {
			fields.MergeCommit = opts.MergeCommit
		}
		if closeReason := normalizedMRCloseReason(opts.Reason); closeReason != "" {
			fields.CloseReason = closeReason
		}
		if result.AgentBead != "" && strings.TrimSpace(fields.AgentBead) == "" {
			fields.AgentBead = result.AgentBead
		}

		newDesc := beads.SetMRFields(issue, fields)
		if err := b.Update(mrID, beads.UpdateOptions{Description: &newDesc}); err != nil {
			return result, fmt.Errorf("record MR close metadata: %w", err)
		}
		if err := b.CloseWithReason(opts.Reason, mrID); err != nil {
			return result, fmt.Errorf("close MR: %w", err)
		}
		result.Closed = true
	case status.IsTerminal():
		result.AlreadyTerminal = true
	default:
		return result, nil
	}

	if result.AgentBead != "" {
		// Persist the terminal snapshot BEFORE clearing the pointer.
		//
		// The MR is ephemeral: once it is terminal it becomes eligible for wisp
		// compaction and will eventually vanish. If the clear below fails and we
		// have written nothing, the agent is left pointing at an id that no longer
		// resolves, and ClearAgentActiveMRIfMatches can never match it again --
		// the evidence needed to repair the pointer is exactly the thing that got
		// destroyed. Writing first means a failed clear is always repairable.
		//
		// Observed 2026-09-01 on bridge_town_core: four polecats held active_mr
		// pointers to MR wisps absent from both databases, each consuming a
		// capacity slot, with no record that those MRs had ever closed cleanly.
		result.TerminalSnapshotErr = persistTerminalMRSnapshot(b, result.AgentBead, mrID, result.SourceIssue)

		cleared, clearErr := b.ForAgentBead().ClearAgentActiveMRIfMatches(result.AgentBead, mrID)
		result.AgentActiveMRCleared = cleared
		result.AgentActiveMRClearErr = clearErr
	}
	return result, nil
}

// persistTerminalMRSnapshot records, on the agent bead, that mrID reached a
// terminal state and which source issue it belonged to. This is the durable
// evidence `gt polecat reconcile` needs to clear a stale pointer after the MR
// itself has been compacted away.
//
// It never overwrites a NEWER MR id: if the agent has already moved on to a
// replacement MR, the completion metadata for that replacement wins.
func persistTerminalMRSnapshot(b *beads.Beads, agentBead, mrID, sourceIssue string) error {
	agentBead = strings.TrimSpace(agentBead)
	mrID = strings.TrimSpace(mrID)
	if agentBead == "" || mrID == "" {
		return nil
	}
	target := b.ForAgentBead()
	_, fields, err := target.GetAgentBead(agentBead)
	if err != nil {
		return fmt.Errorf("reading agent bead %s for terminal snapshot: %w", agentBead, err)
	}
	if fields == nil {
		return nil
	}
	// Only record when this MR is the one the agent is actually holding. A
	// mismatch means a replacement MR is already attached and owns the metadata.
	if existing := strings.TrimSpace(fields.ActiveMR); existing != "" && existing != mrID {
		return nil
	}

	updates := beads.AgentFieldUpdates{}
	if strings.TrimSpace(fields.MRID) != mrID {
		updates.MRID = &mrID
	}
	if sourceIssue = strings.TrimSpace(sourceIssue); sourceIssue != "" &&
		strings.TrimSpace(fields.LastSourceIssue) != sourceIssue {
		updates.LastSourceIssue = &sourceIssue
	}
	if strings.TrimSpace(fields.CompletionTime) == "" {
		now := time.Now().UTC().Format(time.RFC3339)
		updates.CompletionTime = &now
	}
	if updates.MRID == nil && updates.LastSourceIssue == nil && updates.CompletionTime == nil {
		return nil
	}
	if err := target.UpdateAgentDescriptionFields(agentBead, updates); err != nil {
		return fmt.Errorf("persisting terminal MR snapshot on %s: %w", agentBead, err)
	}
	return nil
}

func validateTerminalMRCloseSnapshot(mrID string, fields *beads.MRFields, expected *MergeRequest) error {
	if expected == nil || fields == nil {
		return nil
	}
	checks := []struct {
		name string
		got  string
		want string
	}{
		{name: "branch", got: fields.Branch, want: expected.Branch},
		{name: "source_issue", got: fields.SourceIssue, want: expected.IssueID},
		{name: "commit_sha", got: fields.CommitSHA, want: expected.CommitSHA},
	}
	if strings.TrimSpace(expected.TargetBranch) != "" {
		checks = append(checks, struct {
			name string
			got  string
			want string
		}{name: "target", got: fields.Target, want: expected.TargetBranch})
	}
	for _, check := range checks {
		got := strings.TrimSpace(check.got)
		want := strings.TrimSpace(check.want)
		if want != "" && got != want {
			return fmt.Errorf("MR %s changed after merge proof: %s=%q, verified %q", mrID, check.name, got, want)
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
