package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/polecat"
	"github.com/steveyegge/gastown/internal/tmux"
)

// errPolecatReconcileRefused signals a refusal without printing a second error
// message; the report already lists every reason.
var errPolecatReconcileRefused = errors.New("reconciliation refused")

// Verdicts returned by `gt polecat reconcile`.
const (
	reconcileVerdictSafe    = "SAFE_TO_RECONCILE"
	reconcileVerdictRefused = "REFUSED"
	reconcileVerdictNoop    = "NOTHING_TO_RECONCILE"
)

// selfCleanStepTitle is the internal mol-polecat-work step that a polecat leaves
// behind when terminal cleanup fails partway. It is the ONLY hooked/assigned work
// this command will close; anything else blocks reconciliation.
const selfCleanStepTitle = "Submit work and self-clean"

// reconcileMinSessionAge refuses reconciliation for a polecat whose session was
// created recently, even if that session has since gone: a young session means the
// scheduler may still be setting the polecat up.
const reconcileMinSessionAge = 10 * time.Minute

var (
	polecatReconcileDryRun bool
	polecatReconcileJSON   bool
)

// polecatReconcileReport is the auditable record of one reconciliation attempt.
type polecatReconcileReport struct {
	Rig       string `json:"rig"`
	Polecat   string `json:"polecat"`
	AgentBead string `json:"agent_bead"`
	AgentDB   string `json:"agent_db"`

	SourceIssue       string `json:"source_issue,omitempty"`
	SourceIssueStatus string `json:"source_issue_status,omitempty"`

	ActiveMR       string `json:"active_mr,omitempty"`
	ActiveMRStatus string `json:"active_mr_status,omitempty"`

	GitState    string `json:"git_state"`
	SessionName string `json:"session,omitempty"`

	SelfCleanStep       string `json:"self_clean_step,omitempty"`
	SelfCleanStepStatus string `json:"self_clean_step_status,omitempty"`

	Planned  []string `json:"planned_actions"`
	Applied  []string `json:"applied_actions"`
	Refusals []string `json:"refusals"`
	Verdict  string   `json:"verdict"`
	DryRun   bool     `json:"dry_run"`
}

var polecatReconcileCmd = &cobra.Command{
	Use:   "reconcile <rig>/<polecat>",
	Short: "Reconcile stale terminal lifecycle state for a dormant polecat",
	Long: `Clear a stale active_mr pointer and close an orphaned self-clean step for a
polecat whose work has already landed.

This repairs the debris left when terminal MR cleanup fails partway: the MR is
closed and burned, but the agent bead keeps pointing at it. The pointer can then
never be matched again, because the MR it names no longer exists. Such a polecat
reports NEEDS_MQ_SUBMIT forever and consumes a capacity slot.

The command refuses unless EVERY precondition holds:

  1. the agent identity exists (agent beads are town-owned)
  2. no live tmux session for the polecat, and none created recently
  3. no active hook/product work, except the internal self-clean step
  4. the source product bead is terminal in the owning rig database
  5. active_mr is missing or terminal -- an open or blocked MR refuses
  6. the worktree has no uncommitted files and no branch-local stash
  7. no unpreserved commits: the work survives on origin or a target ref
  8. the pointer is cleared with a compare-and-set against its current value

Any lookup that cannot be resolved refuses with the exact error rather than
guessing. It never deletes: the self-clean step is CLOSED, and no worktree,
branch, wisp or MR is removed. Running it twice is a no-op success.

This is deliberately not a nuke. After reconciling, run
'gt polecat check-recovery' for an independent verdict before any nuke.

Examples:
  gt polecat reconcile bridge_town_core/dag --dry-run
  gt polecat reconcile bridge_town_core/dag
  gt polecat reconcile bridge_town_core/dag --json`,
	Args:         cobra.ExactArgs(1),
	RunE:         runPolecatReconcile,
	SilenceUsage: true,
}

func init() {
	polecatReconcileCmd.Flags().BoolVar(&polecatReconcileDryRun, "dry-run", false,
		"Report what would change without mutating anything")
	polecatReconcileCmd.Flags().BoolVar(&polecatReconcileJSON, "json", false,
		"Output the report as JSON")
	polecatCmd.AddCommand(polecatReconcileCmd)
}

func runPolecatReconcile(cmd *cobra.Command, args []string) error {
	rigName, polecatName, err := parseAddress(args[0])
	if err != nil {
		return err
	}

	mgr, r, err := getPolecatManager(rigName)
	if err != nil {
		return err
	}
	p, err := mgr.Get(polecatName)
	if err != nil {
		return fmt.Errorf("polecat '%s' not found in rig '%s'", polecatName, rigName)
	}

	bd := beads.New(r.Path)
	agentBeadID := polecatBeadIDForRig(r, rigName, polecatName)

	report := &polecatReconcileReport{
		Rig:       rigName,
		Polecat:   polecatName,
		AgentBead: agentBeadID,
		AgentDB:   "town",
		DryRun:    polecatReconcileDryRun,
		Planned:   []string{},
		Applied:   []string{},
		Refusals:  []string{},
	}

	// --- precondition 1: identity exists (routed to the town db) --------------
	agentIssue, fields, agentErr := bd.GetAgentBead(agentBeadID)
	if agentErr != nil {
		report.Refusals = append(report.Refusals,
			fmt.Sprintf("agent_bead=%s lookup_error: %v", agentBeadID, agentErr))
		return emitReconcileReport(report)
	}
	if agentIssue == nil || fields == nil {
		report.Refusals = append(report.Refusals,
			fmt.Sprintf("agent_bead=%s not found in town database", agentBeadID))
		return emitReconcileReport(report)
	}
	report.SourceIssue = firstNonEmptyReconcile(p.Issue, fields.LastSourceIssue)
	report.ActiveMR = strings.TrimSpace(fields.ActiveMR)

	// --- precondition 2: no live session, and none created recently ----------
	if t := tmux.NewTmux(); t != nil {
		if names, listErr := t.ListSessions(); listErr == nil {
			if name, ok := newPolecatSessionSet(names).lookup(rigName, polecatName); ok {
				report.SessionName = name
				report.Refusals = append(report.Refusals,
					fmt.Sprintf("session=%s is live; restart or stop it before reconciling", name))
				if created, cErr := t.GetSessionCreatedTime(name); cErr == nil {
					if age := time.Since(created); age < reconcileMinSessionAge {
						report.Refusals = append(report.Refusals,
							fmt.Sprintf("session=%s age=%s is younger than %s", name,
								age.Round(time.Second), reconcileMinSessionAge))
					}
				}
			}
		} else {
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("tmux lookup_error: %v", listErr))
		}
	}

	// --- precondition 3: no active work except the self-clean step -----------
	selfCleanID, hookRefusals := reconcileInspectAssignedWork(bd, rigName, polecatName, fields, report)
	report.Refusals = append(report.Refusals, hookRefusals...)

	// --- precondition 4: source product bead is terminal ---------------------
	if report.SourceIssue == "" {
		report.Refusals = append(report.Refusals,
			"source_issue is unknown; cannot prove the work landed")
	} else {
		issue, showErr := bd.Show(report.SourceIssue)
		switch {
		case showErr != nil:
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("source_issue=%s status=lookup_error: %v", report.SourceIssue, showErr))
		case issue == nil:
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("source_issue=%s not found in rig database", report.SourceIssue))
		default:
			report.SourceIssueStatus = issue.Status
			if !beads.IssueStatus(issue.Status).IsTerminal() {
				report.Refusals = append(report.Refusals,
					fmt.Sprintf("source_issue=%s status=%s is not terminal", report.SourceIssue, issue.Status))
			}
		}
	}

	// --- precondition 5: active_mr missing or terminal -----------------------
	if report.ActiveMR != "" {
		mr, mrErr := bd.Show(report.ActiveMR)
		switch {
		case mrErr != nil && errors.Is(mrErr, beads.ErrNotFound):
			report.ActiveMRStatus = "missing"
		case mrErr != nil:
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("active_mr=%s status=lookup_error: %v", report.ActiveMR, mrErr))
		case mr == nil:
			report.ActiveMRStatus = "missing"
		default:
			report.ActiveMRStatus = mr.Status
			if !beads.IssueStatus(mr.Status).IsTerminal() {
				report.Refusals = append(report.Refusals,
					fmt.Sprintf("active_mr=%s status=%s is still live; an open or held MR must be resolved first",
						report.ActiveMR, mr.Status))
			}
		}
	}

	// --- preconditions 6 and 7: worktree clean and work preserved ------------
	gitState, gitErr := getGitState(p.ClonePath)
	switch {
	case gitErr != nil:
		report.GitState = "unknown"
		report.Refusals = append(report.Refusals,
			fmt.Sprintf("git_state=lookup_error path=%s: %v", p.ClonePath, gitErr))
	default:
		report.GitState = describeReconcileGitState(gitState)
		if len(gitState.UncommittedFiles) > 0 {
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("git_state=has_uncommitted files=%d", len(gitState.UncommittedFiles)))
		}
		if gitState.StashCount > 0 {
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("git_state=has_stash count=%d", gitState.StashCount))
		}
		if gitState.UnpreservedPatchCount > 0 {
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("git_state=unpreserved_commits count=%d base=%s",
					gitState.UnpreservedPatchCount, gitState.ComparisonBase))
		}
	}

	// --- plan ----------------------------------------------------------------
	if report.ActiveMR != "" {
		report.Planned = append(report.Planned,
			fmt.Sprintf("clear active_mr if it still equals %s", report.ActiveMR))
	}
	if selfCleanID != "" {
		report.Planned = append(report.Planned,
			fmt.Sprintf("close %s as terminal lifecycle cleanup", selfCleanID))
	}
	if strings.TrimSpace(fields.CleanupStatus) != string(polecat.CleanupClean) ||
		strings.TrimSpace(fields.AgentState) != string(beads.AgentStateIdle) {
		report.Planned = append(report.Planned, "set agent_state=idle and cleanup_status=clean")
	}

	if len(report.Refusals) > 0 {
		report.Verdict = reconcileVerdictRefused
		return emitReconcileReport(report)
	}
	if len(report.Planned) == 0 {
		report.Verdict = reconcileVerdictNoop
		return emitReconcileReport(report)
	}
	report.Verdict = reconcileVerdictSafe
	if polecatReconcileDryRun {
		return emitReconcileReport(report)
	}

	// --- apply ---------------------------------------------------------------
	// Ordered so that a failure part-way leaves the remaining steps retryable.
	if report.ActiveMR != "" {
		cleared, clearErr := bd.ClearAgentActiveMRIfMatches(agentBeadID, report.ActiveMR)
		switch {
		case clearErr != nil:
			report.Verdict = reconcileVerdictRefused
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("clearing active_mr=%s failed: %v", report.ActiveMR, clearErr))
			return emitReconcileReport(report)
		case cleared:
			report.Applied = append(report.Applied,
				fmt.Sprintf("cleared active_mr=%s", report.ActiveMR))
		default:
			// Compare-and-set declined: a replacement MR was attached concurrently.
			report.Verdict = reconcileVerdictRefused
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("active_mr no longer equals %s; a replacement was attached concurrently",
					report.ActiveMR))
			return emitReconcileReport(report)
		}
	}

	if selfCleanID != "" {
		reason := fmt.Sprintf("Terminal lifecycle cleanup: source %s is terminal and MR %s is %s (gt polecat reconcile)",
			report.SourceIssue, report.ActiveMR, orUnset(report.ActiveMRStatus))
		// Force is required because the step is assigned to the dormant polecat,
		// not to the operator running this command. Every precondition above has
		// already proven that polecat has no live session and no active work.
		if closeErr := bd.CloseWithReasonForce(reason, selfCleanID); closeErr != nil {
			report.Verdict = reconcileVerdictRefused
			report.Refusals = append(report.Refusals,
				fmt.Sprintf("closing self-clean step %s failed: %v", selfCleanID, closeErr))
			return emitReconcileReport(report)
		}
		report.Applied = append(report.Applied, fmt.Sprintf("closed %s", selfCleanID))
	}

	idle := string(beads.AgentStateIdle)
	clean := string(polecat.CleanupClean)
	if updErr := bd.UpdateAgentDescriptionFields(agentBeadID, beads.AgentFieldUpdates{
		AgentState:    &idle,
		CleanupStatus: &clean,
	}); updErr != nil {
		report.Verdict = reconcileVerdictRefused
		report.Refusals = append(report.Refusals,
			fmt.Sprintf("setting agent_state/cleanup_status failed: %v", updErr))
		return emitReconcileReport(report)
	}
	report.Applied = append(report.Applied, "set agent_state=idle cleanup_status=clean")

	return emitReconcileReport(report)
}

// reconcileInspectAssignedWork returns the id of the single recognised self-clean
// step, plus refusals for any OTHER active work. Arbitrary hooked product work is
// never auto-closed.
func reconcileInspectAssignedWork(bd *beads.Beads, rigName, polecatName string,
	fields *beads.AgentFields, report *polecatReconcileReport) (string, []string) {

	assignee := fmt.Sprintf("%s/polecats/%s", rigName, polecatName)
	var refusals []string
	selfCleanID := ""

	for _, ephemeral := range []bool{true, false} {
		// Status "all" then filter: the orphaned self-clean step sits at
		// in_progress, and a status="open" filter excludes it silently, which
		// would leave the step behind and make reconciliation incomplete.
		issues, err := bd.List(beads.ListOptions{
			Status:    "all",
			Assignee:  assignee,
			Limit:     0,
			Priority:  -1,
			Ephemeral: ephemeral,
		})
		if err != nil {
			refusals = append(refusals,
				fmt.Sprintf("assigned_work lookup_error (ephemeral=%v): %v", ephemeral, err))
			continue
		}
		for _, issue := range issues {
			if issue == nil {
				continue
			}
			if beads.IssueStatus(issue.Status).IsTerminal() {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(issue.Title), selfCleanStepTitle) {
				if selfCleanID != "" && selfCleanID != issue.ID {
					refusals = append(refusals,
						fmt.Sprintf("multiple self-clean steps open (%s, %s); resolve by hand",
							selfCleanID, issue.ID))
					continue
				}
				selfCleanID = issue.ID
				report.SelfCleanStep = issue.ID
				report.SelfCleanStepStatus = issue.Status
				continue
			}
			refusals = append(refusals,
				fmt.Sprintf("assigned_work=%s status=%s is active and is not the internal self-clean step",
					issue.ID, issue.Status))
		}
	}

	// A hook pointing at anything other than the recognised self-clean step blocks.
	if hook := strings.TrimSpace(fields.HookBead); hook != "" && hook != selfCleanID {
		refusals = append(refusals,
			fmt.Sprintf("hook_bead=%s is attached and is not the internal self-clean step", hook))
	}
	return selfCleanID, refusals
}

func describeReconcileGitState(g *GitState) string {
	if g == nil {
		return "unknown"
	}
	parts := []string{}
	if len(g.UncommittedFiles) > 0 {
		parts = append(parts, fmt.Sprintf("uncommitted=%d", len(g.UncommittedFiles)))
	}
	if g.StashCount > 0 {
		parts = append(parts, fmt.Sprintf("stash=%d", g.StashCount))
	}
	if g.UnpreservedPatchCount > 0 {
		parts = append(parts, fmt.Sprintf("unpreserved=%d", g.UnpreservedPatchCount))
	}
	if len(parts) == 0 {
		return "clean, branch preserved"
	}
	return strings.Join(parts, " ")
}

func firstNonEmptyReconcile(values ...string) string {
	for _, v := range values {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func orUnset(s string) string {
	if strings.TrimSpace(s) == "" {
		return "unset"
	}
	return s
}

func emitReconcileReport(report *polecatReconcileReport) error {
	if report.Verdict == "" {
		report.Verdict = reconcileVerdictRefused
	}
	if polecatReconcileJSON {
		out, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	} else {
		fmt.Printf("Polecat: %s/%s\n", report.Rig, report.Polecat)
		fmt.Printf("Agent DB: %s (%s)\n", report.AgentDB, report.AgentBead)
		if report.SourceIssue != "" {
			fmt.Printf("Source issue: %s (%s)\n", report.SourceIssue, orUnset(report.SourceIssueStatus))
		}
		if report.ActiveMR != "" {
			fmt.Printf("Active MR: %s (%s)\n", report.ActiveMR, orUnset(report.ActiveMRStatus))
		}
		fmt.Printf("Git state: %s\n", orUnset(report.GitState))
		if report.SessionName != "" {
			fmt.Printf("Session: %s\n", report.SessionName)
		} else {
			fmt.Printf("Session: none\n")
		}
		if report.SelfCleanStep != "" {
			fmt.Printf("Orphan self-clean step: %s (%s)\n", report.SelfCleanStep, report.SelfCleanStepStatus)
		}
		if len(report.Planned) > 0 {
			fmt.Printf("\nPlanned actions:\n")
			for _, a := range report.Planned {
				fmt.Printf("  - %s\n", a)
			}
		}
		if len(report.Applied) > 0 {
			fmt.Printf("\nApplied:\n")
			for _, a := range report.Applied {
				fmt.Printf("  - %s\n", a)
			}
		}
		if len(report.Refusals) > 0 {
			fmt.Printf("\nRefused because:\n")
			for _, rr := range report.Refusals {
				fmt.Printf("  - %s\n", rr)
			}
		}
		fmt.Printf("\nVerdict: %s", report.Verdict)
		if report.DryRun && report.Verdict == reconcileVerdictSafe {
			fmt.Printf(" (dry run: nothing was changed)")
		}
		fmt.Println()
	}
	if report.Verdict == reconcileVerdictRefused {
		return errPolecatReconcileRefused
	}
	return nil
}
