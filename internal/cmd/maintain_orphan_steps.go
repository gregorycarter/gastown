package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/daemon"
	"github.com/steveyegge/gastown/internal/doltserver"
	"github.com/steveyegge/gastown/internal/formula"
	"github.com/steveyegge/gastown/internal/util"
)

// Orphan step reap: kasb accumulated thousands of open formula step wisps
// (mol-polecat-work's eight steps) whose molecule root had been deleted along
// with its parent-child edges. The nightly purge removes only CLOSED wisps, so
// these stayed open forever, inflating every wisp scan and `bd blocked`
// (which walks blocked wisps one query at a time). This step closes them —
// it never deletes — so the ordinary closed-wisp purge removes them a week
// later.
//
// A wisp is selected only when all of these hold:
//   - status open, not a molecule, idle (updated_at) for at least MinAge;
//   - its title is a literal step title of a known formula;
//   - it has no parent-child edge to a wisp or issue that still exists;
//   - its source bead, read from the formula text ("bd show <id>",
//     "Issue: <id>") of the step or of a sibling step linked to it by blocks
//     edges, is exactly one issue in the same database, and that issue is
//     closed.

const (
	orphanStepModeOff    = "off"
	orphanStepModeDryRun = "dry-run"
	orphanStepModeClose  = "close"

	defaultOrphanStepCap    = 2000
	defaultOrphanStepMinAge = 48 * time.Hour

	orphanStepPageSize   = 500
	orphanStepScanFactor = 5 // scan at most cap*factor candidate rows per database
	// bd close recomputes blocked state per wisp; it has been measured at
	// ~40 wisps/min on kasb, so batches are small and time-boxed, and a run
	// stops closing after orphanStepCloseBudget (the rest wait a night).
	orphanStepCloseBatch   = 25
	orphanStepCloseTimeout = 3 * time.Minute
	orphanStepCloseBudget  = 30 * time.Minute
	orphanStepCloseNote    = "Orphan formula step: parent molecule gone and source bead closed (gt maintain)"
)

// orphanStepSettings is the resolved configuration for one run.
type orphanStepSettings struct {
	Mode   string
	Cap    int
	MinAge time.Duration
}

// resolveOrphanStepSettings merges daemon.json
// (patrols.scheduled_maintenance.orphan_step_reap) with command-line overrides.
// An empty flagMode or a flagCap <= 0 means "use config".
func resolveOrphanStepSettings(cfg *daemon.DaemonPatrolConfig, flagMode string, flagCap int) (orphanStepSettings, error) {
	s := orphanStepSettings{Mode: orphanStepModeDryRun, Cap: defaultOrphanStepCap, MinAge: defaultOrphanStepMinAge}
	if cfg != nil && cfg.Patrols != nil && cfg.Patrols.ScheduledMaintenance != nil {
		if r := cfg.Patrols.ScheduledMaintenance.OrphanStepReap; r != nil {
			if r.Mode != "" {
				s.Mode = r.Mode
			}
			if r.Cap > 0 {
				s.Cap = r.Cap
			}
			if r.MinAge != "" {
				d, err := time.ParseDuration(r.MinAge)
				if err != nil || d <= 0 {
					return s, fmt.Errorf("orphan_step_reap.min_age %q: want a positive duration", r.MinAge)
				}
				s.MinAge = d
			}
		}
	}
	if flagMode != "" {
		s.Mode = flagMode
	}
	if flagCap > 0 {
		s.Cap = flagCap
	}
	switch s.Mode {
	case orphanStepModeOff, orphanStepModeDryRun, orphanStepModeClose:
	default:
		return s, fmt.Errorf("orphan step reap mode %q: want off, dry-run or close", s.Mode)
	}
	return s, nil
}

// orphanStepSourceRe finds the source bead in rendered formula text.
var orphanStepSourceRe = regexp.MustCompile(`(?:\bbd show|\bIssue:)[ \t]+([A-Za-z0-9][A-Za-z0-9_]*-[A-Za-z0-9][A-Za-z0-9._-]*)`)

// orphanStepSourceRefs returns the distinct bead IDs a step description
// names as its source, in order of appearance.
func orphanStepSourceRefs(description string) []string {
	var refs []string
	seen := map[string]bool{}
	for _, m := range orphanStepSourceRe.FindAllStringSubmatch(description, -1) {
		id := strings.TrimRight(m[1], ".-_")
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		refs = append(refs, id)
	}
	return refs
}

// orphanStepCandidate is one selected-by-SQL step wisp with the source refs
// read from its own description.
type orphanStepCandidate struct {
	ID   string
	Refs []string
}

// orphanStepPick is a step chosen for closing.
type orphanStepPick struct {
	ID     string
	Source string
}

// orphanStepPlan is the outcome of selection for one database.
type orphanStepPlan struct {
	Candidates    int
	EligibleTotal int
	Picks         []orphanStepPick // first Cap eligible, in candidate order
	NoSource      int              // no source named in the step or its siblings
	SourceUnknown int              // source named but not an issue in this database
	Ambiguous     int              // more than one existing source issue
	SourceOpen    int              // source issue not closed
}

// componentRefs groups candidates and neighbours by blocks edges and returns,
// for each candidate, the union of source refs across its component.
func componentRefs(cands []orphanStepCandidate, neighborRefs map[string][]string, edges [][2]string) map[string][]string {
	parent := map[string]string{}
	var find func(string) string
	find = func(x string) string {
		p, ok := parent[x]
		if !ok {
			parent[x] = x
			return x
		}
		if p == x {
			return x
		}
		r := find(p)
		parent[x] = r
		return r
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[ra] = rb
		}
	}
	refsOf := map[string][]string{}
	for _, c := range cands {
		find(c.ID)
		refsOf[c.ID] = c.Refs
	}
	for id, refs := range neighborRefs {
		find(id)
		if _, isCand := refsOf[id]; !isCand {
			refsOf[id] = refs
		}
	}
	for _, e := range edges {
		union(e[0], e[1])
	}

	byRoot := map[string]map[string]bool{}
	for id, refs := range refsOf {
		root := find(id)
		if byRoot[root] == nil {
			byRoot[root] = map[string]bool{}
		}
		for _, r := range refs {
			byRoot[root][r] = true
		}
	}
	out := make(map[string][]string, len(cands))
	for _, c := range cands {
		set := byRoot[find(c.ID)]
		refs := make([]string, 0, len(set))
		for r := range set {
			refs = append(refs, r)
		}
		sort.Strings(refs)
		out[c.ID] = refs
	}
	return out
}

// planOrphanStepReap decides which candidates to close. sourceStatus maps
// issue IDs that exist in this database to their status.
func planOrphanStepReap(cands []orphanStepCandidate, compRefs map[string][]string, sourceStatus map[string]string, limit int) orphanStepPlan {
	plan := orphanStepPlan{Candidates: len(cands)}
	for _, c := range cands {
		refs := compRefs[c.ID]
		var existing []string
		for _, r := range refs {
			if _, ok := sourceStatus[r]; ok {
				existing = append(existing, r)
			}
		}
		switch {
		case len(refs) == 0:
			plan.NoSource++
		case len(existing) == 0:
			plan.SourceUnknown++
		case len(existing) > 1:
			plan.Ambiguous++
		case sourceStatus[existing[0]] != "closed":
			plan.SourceOpen++
		default:
			plan.EligibleTotal++
			if len(plan.Picks) < limit {
				plan.Picks = append(plan.Picks, orphanStepPick{ID: c.ID, Source: existing[0]})
			}
		}
	}
	return plan
}

// orphanStepCandidateQuery selects open, idle, non-molecule wisps with a known
// step title and no live parent, after a (created_at, id) keyset position.
func orphanStepCandidateQuery(titleCount int) string {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", titleCount), ",")
	return `SELECT w.id, w.created_at, COALESCE(w.description, '') FROM wisps w
WHERE w.status = 'open'
  AND w.issue_type <> 'molecule'
  AND w.updated_at < ?
  AND w.title IN (` + placeholders + `)
  AND NOT EXISTS (
    SELECT 1 FROM wisp_dependencies d
    WHERE d.issue_id = w.id AND d.type = 'parent-child'
      AND (d.depends_on_external IS NOT NULL
           OR EXISTS (SELECT 1 FROM wisps p WHERE p.id = d.depends_on_wisp_id)
           OR EXISTS (SELECT 1 FROM issues p WHERE p.id = d.depends_on_issue_id)))
  AND (w.created_at > ? OR (w.created_at = ? AND w.id > ?))
ORDER BY w.created_at, w.id
LIMIT ?`
}

// orphanStepReport summarises one database's reap.
type orphanStepReport struct {
	DB     string
	Plan   orphanStepPlan
	Closed int
	Errors []string
}

// reapOrphanSteps selects (and in close mode closes) orphan step wisps in one
// database.
func reapOrphanSteps(config *doltserver.Config, townRoot, dbName string, s orphanStepSettings, titles []string) (orphanStepReport, error) {
	rep := orphanStepReport{DB: dbName}
	if len(titles) == 0 {
		return rep, nil
	}
	db, err := maintainOpenDB(config, dbName)
	if err != nil {
		return rep, err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if ok, err := orphanStepTablesExist(ctx, db); err != nil || !ok {
		return rep, err
	}

	cands, err := orphanStepScan(ctx, db, s, titles)
	if err != nil {
		return rep, err
	}
	if len(cands) == 0 {
		return rep, nil
	}

	candIDs := make([]string, len(cands))
	isCand := make(map[string]bool, len(cands))
	for i, c := range cands {
		candIDs[i] = c.ID
		isCand[c.ID] = true
	}
	edges, err := orphanStepBlocksEdges(ctx, db, candIDs)
	if err != nil {
		return rep, err
	}
	var neighborIDs []string
	seenNeighbor := map[string]bool{}
	for _, e := range edges {
		for _, id := range e {
			if !isCand[id] && !seenNeighbor[id] {
				seenNeighbor[id] = true
				neighborIDs = append(neighborIDs, id)
			}
		}
	}
	neighborRefs, err := orphanStepNeighborRefs(ctx, db, neighborIDs)
	if err != nil {
		return rep, err
	}
	compRefs := componentRefs(cands, neighborRefs, edges)

	var allRefs []string
	seenRef := map[string]bool{}
	for _, refs := range compRefs {
		for _, r := range refs {
			if !seenRef[r] {
				seenRef[r] = true
				allRefs = append(allRefs, r)
			}
		}
	}
	sourceStatus, err := orphanStepIssueStatuses(ctx, db, allRefs)
	if err != nil {
		return rep, err
	}
	rep.Plan = planOrphanStepReap(cands, compRefs, sourceStatus, s.Cap)

	if s.Mode != orphanStepModeClose || len(rep.Plan.Picks) == 0 {
		return rep, nil
	}
	beadsDir := doltserver.FindRigBeadsDir(townRoot, dbName)
	if _, err := os.Stat(filepath.Join(beadsDir, "metadata.json")); err != nil {
		return rep, fmt.Errorf("no initialised beads dir for %s: %w", dbName, err)
	}
	started := time.Now()
	for i := 0; i < len(rep.Plan.Picks); i += orphanStepCloseBatch {
		if time.Since(started) > orphanStepCloseBudget {
			rep.Errors = append(rep.Errors, fmt.Sprintf("close time budget %s used; %d left for the next run", orphanStepCloseBudget, len(rep.Plan.Picks)-i))
			break
		}
		end := i + orphanStepCloseBatch
		if end > len(rep.Plan.Picks) {
			end = len(rep.Plan.Picks)
		}
		ids := make([]string, 0, end-i)
		for _, p := range rep.Plan.Picks[i:end] {
			ids = append(ids, p.ID)
		}
		if err := closeOrphanStepBatch(beadsDir, ids); err != nil {
			rep.Errors = append(rep.Errors, err.Error())
			continue
		}
		rep.Closed += len(ids)
	}
	return rep, nil
}

// closeOrphanStepBatch runs `bd close` for one batch in the database's beads
// directory, pinned to that database like the closed-wisp purge.
func closeOrphanStepBatch(beadsDir string, ids []string) error {
	env := beads.BuildMutationPinnedBDEnv(os.Environ(), beadsDir)
	args := append([]string{"close", "--reason", orphanStepCloseNote}, ids...)
	args = beads.MaybePrependAllowStaleWithEnv(env, args)

	ctx, cancel := context.WithTimeout(context.Background(), orphanStepCloseTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bd", args...)
	cmd.Dir = filepath.Dir(beadsDir)
	cmd.Env = env
	util.SetProcessGroup(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("bd close (%d wisps): timed out after %s", len(ids), orphanStepCloseTimeout)
	}
	if err != nil {
		return fmt.Errorf("bd close (%d wisps): %w: %s", len(ids), err, firstLineOf(strings.TrimSpace(stderr.String())))
	}
	return nil
}

func orphanStepTablesExist(ctx context.Context, db *sql.DB) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name IN ('wisps', 'wisp_dependencies', 'issues')`).Scan(&n)
	if err != nil {
		return false, err
	}
	return n == 3, nil
}

func orphanStepScan(ctx context.Context, db *sql.DB, s orphanStepSettings, titles []string) ([]orphanStepCandidate, error) {
	query := orphanStepCandidateQuery(len(titles))
	cutoff := time.Now().UTC().Add(-s.MinAge)
	maxScan := s.Cap * orphanStepScanFactor
	var out []orphanStepCandidate
	lastCreated := time.Unix(0, 0).UTC()
	lastID := ""
	for len(out) < maxScan {
		args := make([]any, 0, len(titles)+5)
		args = append(args, cutoff)
		for _, t := range titles {
			args = append(args, t)
		}
		args = append(args, lastCreated, lastCreated, lastID, orphanStepPageSize)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("selecting orphan steps: %w", err)
		}
		n := 0
		for rows.Next() {
			var id, desc string
			var created time.Time
			if err := rows.Scan(&id, &created, &desc); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out = append(out, orphanStepCandidate{ID: id, Refs: orphanStepSourceRefs(desc)})
			lastCreated, lastID = created, id
			n++
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return nil, err
		}
		if n < orphanStepPageSize {
			break
		}
	}
	return out, nil
}

// orphanStepInQuery runs query once per chunk of ids; query has one %s for
// the placeholder list (or two, for queries that match ids on two columns).
func orphanStepInQuery(ctx context.Context, db *sql.DB, query string, slots int, ids []string, scan func(*sql.Rows) error) error {
	for i := 0; i < len(ids); i += orphanStepPageSize {
		end := i + orphanStepPageSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]
		ph := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		phs := make([]any, slots)
		args := make([]any, 0, len(chunk)*slots)
		for s := 0; s < slots; s++ {
			phs[s] = ph
			for _, id := range chunk {
				args = append(args, id)
			}
		}
		rows, err := db.QueryContext(ctx, fmt.Sprintf(query, phs...), args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			if err := scan(rows); err != nil {
				_ = rows.Close()
				return err
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func orphanStepBlocksEdges(ctx context.Context, db *sql.DB, ids []string) ([][2]string, error) {
	var edges [][2]string
	err := orphanStepInQuery(ctx, db, `SELECT issue_id, depends_on_wisp_id FROM wisp_dependencies
WHERE type = 'blocks' AND depends_on_wisp_id IS NOT NULL
  AND (issue_id IN (%s) OR depends_on_wisp_id IN (%s))`, 2, ids, func(r *sql.Rows) error {
		var a, b string
		if err := r.Scan(&a, &b); err != nil {
			return err
		}
		edges = append(edges, [2]string{a, b})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading step edges: %w", err)
	}
	return edges, nil
}

func orphanStepNeighborRefs(ctx context.Context, db *sql.DB, ids []string) (map[string][]string, error) {
	out := map[string][]string{}
	err := orphanStepInQuery(ctx, db, `SELECT id, COALESCE(description, '') FROM wisps WHERE id IN (%s)`, 1, ids, func(r *sql.Rows) error {
		var id, desc string
		if err := r.Scan(&id, &desc); err != nil {
			return err
		}
		out[id] = orphanStepSourceRefs(desc)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading sibling steps: %w", err)
	}
	return out, nil
}

func orphanStepIssueStatuses(ctx context.Context, db *sql.DB, ids []string) (map[string]string, error) {
	out := map[string]string{}
	err := orphanStepInQuery(ctx, db, `SELECT id, status FROM issues WHERE id IN (%s)`, 1, ids, func(r *sql.Rows) error {
		var id, status string
		if err := r.Scan(&id, &status); err != nil {
			return err
		}
		out[id] = status
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading source beads: %w", err)
	}
	return out, nil
}

// formatOrphanStepReport is the one-line log entry for a database.
func formatOrphanStepReport(r orphanStepReport, s orphanStepSettings) string {
	p := r.Plan
	verb := fmt.Sprintf("would close %d", len(p.Picks))
	if s.Mode == orphanStepModeClose {
		verb = fmt.Sprintf("closed %d", r.Closed)
	}
	line := fmt.Sprintf("orphan-steps %s [%s]: %s of %d eligible (cap %d, idle >= %s); scanned %d; skipped no-source=%d source-unknown=%d ambiguous=%d source-open=%d",
		r.DB, s.Mode, verb, p.EligibleTotal, s.Cap, s.MinAge, p.Candidates, p.NoSource, p.SourceUnknown, p.Ambiguous, p.SourceOpen)
	if n := len(p.Picks); n > 0 {
		sample := p.Picks
		if n > 3 {
			sample = sample[:3]
		}
		parts := make([]string, len(sample))
		for i, pk := range sample {
			parts[i] = pk.ID + "<-" + pk.Source
		}
		line += "; e.g. " + strings.Join(parts, ", ")
	}
	if len(r.Errors) > 0 {
		line += fmt.Sprintf("; %d close batch error(s), first: %s", len(r.Errors), firstLineOf(r.Errors[0]))
	}
	return line
}

func firstLineOf(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// countOpenWisps returns the number of non-closed wisps in a database, or -1
// when the database has no wisps table or cannot be read.
func countOpenWisps(config *doltserver.Config, dbName string) int {
	db, err := maintainOpenDB(config, dbName)
	if err != nil {
		return -1
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), maintainQueryTimeout)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM wisps WHERE status <> 'closed'").Scan(&n); err != nil {
		return -1
	}
	return n
}

// maintainMetricsLine reports open wisps per database and the convoy
// manager's last hourly convoy-check count.
func maintainMetricsLine(townRoot string, openWisps map[string]int, dbs []string) string {
	parts := make([]string, 0, len(dbs))
	for _, db := range dbs {
		if n, ok := openWisps[db]; ok && n >= 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", db, n))
		}
	}
	convoy := "unknown"
	if data, err := os.ReadFile(daemon.ConvoyMetricsFile(townRoot)); err == nil {
		var snap daemon.ConvoyMetricsSnapshot
		if json.Unmarshal(data, &snap) == nil && !snap.WrittenAt.IsZero() {
			convoy = fmt.Sprintf("%d (window ending %s)", snap.ConvoyChecks, snap.WrittenAt.UTC().Format("15:04Z"))
		}
	}
	return fmt.Sprintf("maintain metrics: open wisps %s; convoy checks last hour=%s", strings.Join(parts, " "), convoy)
}

// orphanStepTitles is the step-title allowlist (overridable in tests).
var orphanStepTitles = formula.StepTitles
