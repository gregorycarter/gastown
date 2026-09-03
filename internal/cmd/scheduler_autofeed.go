package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/constants"
	"github.com/steveyegge/gastown/internal/convoy"
	"github.com/steveyegge/gastown/internal/scheduler/capacity"
	"github.com/steveyegge/gastown/internal/style"
	"github.com/steveyegge/gastown/internal/workspace"
)

// autoFeedReadyLimit is how many `bd ready` rows are considered per rig.
// The floor is small (single digits) and rows are priority-sorted by bd, so
// 50 is generous headroom without paying for a full backlog scan.
const autoFeedReadyLimit = 50

// autoFeedCandidate is one `bd ready` row reduced to the fields the feeder
// needs. Kept separate from beads.Issue so the selection logic is pure and
// testable without a database.
type autoFeedCandidate struct {
	ID        string
	Title     string
	Status    string
	Assignee  string
	Type      string
	Priority  int
	CreatedAt string
	Labels    []string
	Rig       string
}

// autoFeedRejection records why a ready bead was not enqueued. Surfaced by
// `gt scheduler feed --dry-run` so an operator can see what the feeder
// refuses and why, instead of guessing from an empty queue.
type autoFeedRejection struct {
	ID     string
	Rig    string
	Reason string
}

// autoFeedPolicy is the label/ops-slot policy applied to candidates.
type autoFeedPolicy struct {
	// AllowLabels, when non-empty, requires each candidate to carry at least
	// one of these labels.
	AllowLabels []string
	// ExcludeLabels rejects any candidate carrying one of these labels.
	ExcludeLabels []string
	// MaxOpsSlots caps concurrently-working ops-labelled beads.
	MaxOpsSlots int
	// OpsWorking is how many ops-labelled beads polecats are already on.
	OpsWorking int
}

// isOpsBead reports whether a bead carries a label that marks it as
// operations (rather than product) work.
func isOpsBead(labels []string) bool {
	for _, l := range labels {
		for _, ops := range capacity.OpsLabels {
			if strings.EqualFold(strings.TrimSpace(l), ops) {
				return true
			}
		}
	}
	return false
}

func hasAnyLabel(labels, want []string) (string, bool) {
	for _, l := range labels {
		for _, w := range want {
			if strings.EqualFold(strings.TrimSpace(l), strings.TrimSpace(w)) {
				return w, true
			}
		}
	}
	return "", false
}

// sortAutoFeedCandidates orders candidates the way the scheduler dispatches
// them: priority ascending (P0 first), then oldest first, then by ID so the
// order is total and stable across ticks.
func sortAutoFeedCandidates(candidates []autoFeedCandidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if a.CreatedAt != b.CreatedAt {
			return a.CreatedAt < b.CreatedAt
		}
		return a.ID < b.ID
	})
}

// selectAutoFeedCandidates picks up to limit beads to enqueue, returning the
// selection and a rejection reason for every candidate it passed over.
//
// A candidate qualifies when it is open, unassigned, of a slingable type, not
// already scheduled, carries no excluded label, and (when an allow-list is
// configured) carries at least one allowed label. Ops-labelled work is capped
// at policy.MaxOpsSlots concurrently working, unless the bead is P0/P1.
func selectAutoFeedCandidates(candidates []autoFeedCandidate, policy autoFeedPolicy, scheduled map[string]bool, limit int) ([]autoFeedCandidate, []autoFeedRejection) {
	ordered := make([]autoFeedCandidate, len(candidates))
	copy(ordered, candidates)
	sortAutoFeedCandidates(ordered)

	var selected []autoFeedCandidate
	var rejected []autoFeedRejection
	opsUsed := policy.OpsWorking

	reject := func(c autoFeedCandidate, reason string) {
		rejected = append(rejected, autoFeedRejection{ID: c.ID, Rig: c.Rig, Reason: reason})
	}

	for _, c := range ordered {
		// Intrinsic disqualifications are evaluated before the floor check so
		// a --dry-run tells an operator *why* a bead is not dispatchable
		// ("exclude-label=needs-operator") rather than hiding it behind
		// "floor-met".
		if c.Status != "open" {
			reject(c, fmt.Sprintf("status=%s", c.Status))
			continue
		}
		if strings.TrimSpace(c.Assignee) != "" {
			reject(c, fmt.Sprintf("assignee=%s", c.Assignee))
			continue
		}
		if c.Type != "" && !convoy.IsSlingableType(c.Type) {
			reject(c, fmt.Sprintf("type=%s", c.Type))
			continue
		}
		if capacity.IsMessagingBead(c.Labels) {
			reject(c, "messaging-bead")
			continue
		}
		if scheduled[c.ID] {
			reject(c, "already-scheduled")
			continue
		}
		if label, found := hasAnyLabel(c.Labels, policy.ExcludeLabels); found {
			reject(c, "exclude-label="+label)
			continue
		}
		if len(policy.AllowLabels) > 0 {
			if _, found := hasAnyLabel(c.Labels, policy.AllowLabels); !found {
				reject(c, "not-in-allow-list")
				continue
			}
		}
		if isOpsBead(c.Labels) && c.Priority > 1 {
			if opsUsed >= policy.MaxOpsSlots {
				reject(c, fmt.Sprintf("ops-slot-cap=%d", policy.MaxOpsSlots))
				continue
			}
			if len(selected) >= limit {
				reject(c, "floor-met")
				continue
			}
			opsUsed++
		} else if len(selected) >= limit {
			reject(c, "floor-met")
			continue
		}
		selected = append(selected, c)
	}

	return selected, rejected
}

// autoFeedCandidatesForRig runs `bd ready` in one rig and converts the rows.
func autoFeedCandidatesForRig(townRoot, rigName string) ([]autoFeedCandidate, error) {
	rigBeadsDir, ok := beads.ResolveRepoAliasBeadsDir(townRoot, rigName)
	if !ok {
		return nil, fmt.Errorf("cannot resolve beads database for rig %q", rigName)
	}
	workDir := filepath.Dir(rigBeadsDir)
	b := beads.NewWithBeadsDir(workDir, rigBeadsDir)

	issues, err := b.ReadyLimited(autoFeedReadyLimit)
	if err != nil {
		return nil, fmt.Errorf("bd ready in %s: %w", workDir, err)
	}

	// Same hygiene `gt ready` applies: formula scaffolds, wisps, identity
	// beads and rows that do not route back to this rig are not work.
	issues = filterFormulaScaffolds(issues, getFormulaNames(workDir))
	issues = filterWisps(issues, getWispIDs(workDir))
	issues = filterIdentityBeads(issues)
	issues = filterReadyIssuesByRoute(townRoot, rigName, issues)

	candidates := make([]autoFeedCandidate, 0, len(issues))
	for _, issue := range issues {
		if issue == nil {
			continue
		}
		candidates = append(candidates, autoFeedCandidate{
			ID:        issue.ID,
			Title:     issue.Title,
			Status:    issue.Status,
			Assignee:  issue.Assignee,
			Type:      issue.Type,
			Priority:  issue.Priority,
			CreatedAt: issue.CreatedAt,
			Labels:    issue.Labels,
			Rig:       rigName,
		})
	}
	return candidates, nil
}

// countWorkingOpsBeads counts polecats currently working an ops-labelled bead.
// Best effort: a rig whose beads are unreadable contributes 0 rather than
// blocking the feed.
func countWorkingOpsBeads(townRoot string) int {
	rigsConfig, err := config.LoadRigsConfig(constants.MayorRigsPath(townRoot))
	if err != nil {
		return 0
	}
	count := 0
	for rigName := range rigsConfig.Rigs {
		rigPath := filepath.Join(townRoot, rigName)
		if _, err := os.Stat(rigPath); err != nil {
			continue
		}
		active, err := listActivePolecatWorkByName(beads.New(rigPath), rigName)
		if err != nil {
			continue
		}
		for _, issue := range active {
			if issue != nil && isOpsBead(issue.Labels) {
				count++
			}
		}
	}
	return count
}

// autoFeedResult reports what one feed pass did (or would do).
type autoFeedResult struct {
	Floor      int
	ReadyNow   int
	Selected   []autoFeedCandidate
	Rejected   []autoFeedRejection
	Enqueued   int
	OpsWorking int
	// DirectMode is set when scheduler.max_polecats <= 0, where contexts are
	// never consumed and feeding the queue would only accumulate them.
	DirectMode bool
}

// autoFeedScheduler tops the dispatch queue up to the configured floor from
// `bd ready`. Returns the number of beads enqueued.
//
// This is the supply side of the scheduler. Without it the queue is fed only
// by `gt sling` (a Mayor LLM turn) and the convoy stranded scan, so dispatch
// stalls whenever nobody is prompting the Mayor.
func autoFeedScheduler(townRoot string, floorOverride int, dryRun bool) (*autoFeedResult, error) {
	settings, err := config.LoadOrCreateTownSettings(config.TownSettingsPath(townRoot))
	if err != nil {
		return nil, fmt.Errorf("loading town settings: %w", err)
	}
	schedulerCfg := settings.Scheduler
	if schedulerCfg == nil {
		schedulerCfg = capacity.DefaultSchedulerConfig()
	}

	floor := schedulerCfg.GetQueueFloor()
	if floorOverride > 0 {
		floor = floorOverride
	}
	result := &autoFeedResult{Floor: floor}
	if floor <= 0 {
		return result, nil
	}
	if schedulerCfg.GetMaxPolecats() <= 0 {
		// Direct dispatch: contexts are never consumed, so feeding the queue
		// would just accumulate open context beads.
		result.DirectMode = true
		return result, nil
	}

	assessments, assessErr := assessScheduledContexts(townRoot)
	if assessErr != nil {
		return nil, fmt.Errorf("assessing scheduled contexts: %w", assessErr)
	}
	result.ReadyNow = len(readySlingContextsFromAssessments(assessments))
	need := floor - result.ReadyNow
	if need <= 0 {
		return result, nil
	}

	rigsConfig, err := config.LoadRigsConfig(constants.MayorRigsPath(townRoot))
	if err != nil {
		return nil, fmt.Errorf("loading rigs config: %w", err)
	}
	rigNames := make([]string, 0, len(rigsConfig.Rigs))
	for name := range rigsConfig.Rigs {
		rigNames = append(rigNames, name)
	}
	sort.Strings(rigNames)

	var candidates []autoFeedCandidate
	var candidateIDs []string
	for _, rigName := range rigNames {
		rigCandidates, err := autoFeedCandidatesForRig(townRoot, rigName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s autofeed: %v\n", style.Dim.Render("○"), err)
			continue
		}
		candidates = append(candidates, rigCandidates...)
		for _, c := range rigCandidates {
			candidateIDs = append(candidateIDs, c.ID)
		}
	}
	if len(candidates) == 0 {
		return result, nil
	}

	result.OpsWorking = countWorkingOpsBeads(townRoot)
	policy := autoFeedPolicy{
		AllowLabels:   schedulerCfg.GetAutoFeedLabels(),
		ExcludeLabels: schedulerCfg.GetAutoFeedExcludeLabels(),
		MaxOpsSlots:   schedulerCfg.GetAutoFeedMaxOpsSlots(),
		OpsWorking:    result.OpsWorking,
	}

	selected, rejected := selectAutoFeedCandidates(candidates, policy, areScheduled(candidateIDs), need)
	result.Selected = selected
	result.Rejected = rejected
	if dryRun {
		return result, nil
	}

	for _, c := range selected {
		opts := ScheduleOptions{
			Formula:  resolveFormula("", false, townRoot, c.Rig),
			NoConvoy: true,
		}
		if err := scheduleBead(c.ID, c.Rig, opts); err != nil {
			fmt.Fprintf(os.Stderr, "%s autofeed: could not schedule %s → %s: %v\n",
				style.Warning.Render("⚠"), c.ID, c.Rig, err)
			continue
		}
		result.Enqueued++
		fmt.Printf("%s autofeed enqueued %s (P%d, %s) → %s\n",
			style.Bold.Render("→"), c.ID, c.Priority, c.Title, c.Rig)
	}
	return result, nil
}

var (
	schedulerFeedFloor  int
	schedulerFeedDryRun bool
)

var schedulerFeedCmd = &cobra.Command{
	Use:   "feed",
	Short: "Top the dispatch queue up from bd ready",
	Long: `Top the dispatch queue up to scheduler.queue_floor from bd ready.

The daemon runs this automatically at the top of every dispatch tick when
scheduler.queue_floor > 0. Run it by hand to inspect or force a top-up.

Candidates must be open, unassigned, of a slingable type, not already
scheduled, and must pass the label policy:

  scheduler.queue_floor             minimum ready contexts to maintain (0 = off)
  scheduler.autofeed_labels         allow-list (empty = any label)
  scheduler.autofeed_exclude_labels deny-list (default: operator/control-plane labels)
  scheduler.autofeed_max_ops_slots  max concurrently-working ops beads (default 1)

  gt scheduler feed                    # top up to the configured floor
  gt scheduler feed --floor 6          # override the floor for this run
  gt scheduler feed --dry-run --floor 6  # print what it would enqueue, touch nothing`,
	RunE: runSchedulerFeed,
}

func runSchedulerFeed(cmd *cobra.Command, args []string) error {
	townRoot, err := workspace.FindFromCwdOrError()
	if err != nil {
		return err
	}

	result, err := autoFeedScheduler(townRoot, schedulerFeedFloor, schedulerFeedDryRun)
	if err != nil {
		return err
	}

	if result.Floor <= 0 {
		fmt.Println("Auto-feed is off (scheduler.queue_floor=0). Enable with: gt config set scheduler.queue_floor 6")
		return nil
	}

	if result.DirectMode {
		fmt.Println("Auto-feed does nothing in direct dispatch mode (scheduler.max_polecats <= 0).")
		return nil
	}

	fmt.Printf("%s (floor %d, ready now %d, ops working %d)\n",
		style.Bold.Render("Scheduler auto-feed"), result.Floor, result.ReadyNow, result.OpsWorking)

	if len(result.Selected) == 0 {
		fmt.Println("  Nothing to enqueue.")
	}
	for _, c := range result.Selected {
		verb := "Enqueued"
		if schedulerFeedDryRun {
			verb = "Would enqueue"
		}
		fmt.Printf("  %s %s → %s (P%d) %s\n", verb, c.ID, c.Rig, c.Priority, c.Title)
	}
	if len(result.Rejected) > 0 {
		fmt.Printf("\n  Excluded (%d):\n", len(result.Rejected))
		for _, r := range result.Rejected {
			fmt.Printf("    %s %s: %s\n", style.Dim.Render("○"), r.ID, r.Reason)
		}
	}
	return nil
}
