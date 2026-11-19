package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	beadsdk "github.com/steveyegge/beads"
	"github.com/steveyegge/gastown/internal/atomicfile"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/convoy"
	"github.com/steveyegge/gastown/internal/util"
)

const (
	// defaultStrandedScanInterval is the pause between the end of one
	// stranded scan and the start of the next. It was 30s on a ticker; a scan
	// takes ~14s plus one `gt convoy check` per waiting convoy, so scans ran
	// back to back and produced ~60% of all bd/gt spawns against Dolt.
	// Completion is event-driven (5s close poll) and feeding is reactive on
	// close events, so the stranded scan is only a backstop.
	defaultStrandedScanInterval = 5 * time.Minute

	// defaultCompletionSweepInterval is how often a stranded convoy whose
	// tracked issues are still open gets a fallback completion check.
	defaultCompletionSweepInterval = 10 * time.Minute

	// convoyMetricsInterval is how often the manager logs its counters.
	convoyMetricsInterval = time.Hour

	// recoveryScanInterval is the scan pause while Dolt is recovering.
	recoveryScanInterval = 5 * time.Second
	eventPollInterval    = 5 * time.Second
	eventPollMaxBackoff  = 60 * time.Second
	// Beads lifecycle events use CURRENT_TIMESTAMP in Dolt, which is second
	// precision. Poll with a 1s overlap so transitions that happen in the same
	// second as the previous high-water mark are still visible next cycle.
	eventPollLookback = 1 * time.Second

	// convoyGracePeriod is how long after creation a convoy is immune from
	// auto-close. This prevents a race where the daemon's stranded scan
	// fires before the sling's bd dep add is visible in Dolt. See GH#2303.
	convoyGracePeriod = 5 * time.Minute

	// convoyFeedBackoffBase and convoyFeedBackoffMax bound the per-convoy
	// feed backoff. Without it the scan retried `gt sling` for the same
	// unslingable issue every 30 seconds forever: one day's daemon log held
	// 3,282 feed attempts, 3,282 of them failing "already hooked".
	convoyFeedBackoffBase = 30 * time.Second
	convoyFeedBackoffMax  = 15 * time.Minute
)

// strandedConvoyInfo matches the JSON output of `gt convoy stranded --json`.
type strandedConvoyInfo struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	TrackedCount int       `json:"tracked_count"`
	ReadyCount   int       `json:"ready_count"`
	ReadyIssues  []string  `json:"ready_issues"`
	CreatedAt    time.Time `json:"created_at"`
	BaseBranch   string    `json:"base_branch,omitempty"`
	// OpenIssues lists tracked issues not yet closed (unknown status counts
	// as open). nil means the producing gt predates the field; an empty,
	// non-nil slice means every tracked issue is closed.
	OpenIssues []string `json:"open_issues"`
}

// ConvoyManager monitors beads events for issue closes and periodically scans for stranded convoys.
// It handles both event-driven completion checks (via convoy.CheckConvoysForIssue) and periodic
// stranded convoy feeding/cleanup.
//
// Event polling watches ALL beads stores (town-level hq + per-rig) so that close events from
// any rig are detected. Convoys live in the hq store, so convoy lookups always use hqStore.
// Parked rigs are skipped during event polling.
type ConvoyManager struct {
	townRoot     string
	scanInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	logger       func(format string, args ...interface{})

	// stores maps store names to beads stores for event polling.
	// Key "hq" is the town-level store (used for convoy lookups).
	// Other keys are rig names (e.g., "gastown", "beads", "shippercrm").
	// Populated lazily via openStores if nil at startup (e.g., Dolt not ready).
	// Protected by storesMu.
	stores   map[string]beadsdk.Storage
	storesMu sync.Mutex

	// openStores is called lazily to open beads stores when stores is nil.
	// This handles the case where Dolt isn't ready at daemon startup.
	// Once stores are successfully opened, this is not called again.
	// May be nil to disable lazy opening (stores must be provided upfront).
	openStores func() map[string]beadsdk.Storage

	// isRigParked reports whether a rig is currently parked/docked.
	// Parked rigs are skipped during event polling. May be nil (never parked).
	isRigParked func(string) bool

	gtPath string

	// started guards against double-call of Start() which would spawn duplicate goroutines.
	started atomic.Bool

	// recoveryMode is set true when an event-poll failure is detected (indicating
	// Dolt is down). While set, runStrandedScan uses a shorter 5s interval so it
	// retries quickly once Dolt comes back. Cleared after the first successful scan.
	recoveryMode atomic.Bool

	// scanMu serializes calls to scan() from runStrandedScan, runStartupSweep,
	// and the Dolt recovery callback. Without this, concurrent scans can spawn
	// duplicate convoy checks for the same stranded convoy.
	scanMu sync.Mutex

	// lastEventIDs tracks per-store high-water marks for event polling.
	// Key matches stores map keys ("hq", "gastown", etc.).
	lastEventIDs sync.Map // map[string]time.Time

	// seeded is true once the first poll cycle has run (warm-up).
	// The first cycle advances high-water marks without processing events,
	// preventing a burst of historical event replay on daemon restart.
	seeded atomic.Bool

	// processedCloses tracks issue IDs whose current closed state has already
	// been processed. This prevents duplicate convoy checks when the same close
	// event is seen from multiple stores or across poll cycles where high-water
	// marks don't perfectly deduplicate (e.g., event replication). The entry is
	// cleared when the issue is reopened so a later close is processed again.
	// See GH #1798.
	processedCloses sync.Map // map[string]bool

	// processedLifecycleEvents tracks close/reopen event IDs that have already
	// been handled. This allows the 1s overlap window above without replaying
	// the same lifecycle events on every poll.
	processedLifecycleEvents sync.Map // map[string]bool

	// feedBackoff holds per-convoy feed backoff state, keyed by convoy ID.
	// A convoy whose feed fails is retried on an exponential schedule
	// instead of every scan, and logs only when its state changes.
	feedBackoff sync.Map // map[string]*convoyFeedBackoff

	// completionSweep is the fallback completion-check interval for stranded
	// convoys with open tracked issues. <= 0 checks on every scan (legacy).
	completionSweep time.Duration
	// skipInactiveRigs skips completion checks for convoys with an open
	// tracked issue on a parked, docked or removed rig.
	skipInactiveRigs bool

	// completionMu guards lastCompletionCheck and inactiveSkip.
	completionMu sync.Mutex
	// lastCompletionCheck records when each convoy was last checked, by the
	// scan or by the event poll.
	lastCompletionCheck map[string]time.Time
	// inactiveSkip holds the reason a convoy is being skipped, so the skip is
	// logged once per state change instead of every scan.
	inactiveSkip map[string]string

	// now is the clock (overridable in tests).
	now func() time.Time

	metrics convoyMetrics
}

// convoyMetrics counts manager work between hourly metric log lines.
type convoyMetrics struct {
	checksEvent     atomic.Int64
	checksAllClosed atomic.Int64
	checksSweep     atomic.Int64
	checksLegacy    atomic.Int64
	checksEmpty     atomic.Int64
	skipNotDue      atomic.Int64
	skipInactive    atomic.Int64
	scans           atomic.Int64
	scanNanos       atomic.Int64

	mu    sync.Mutex
	since time.Time
}

// convoyFeedBackoff is the retry state for one convoy's feed attempts.
type convoyFeedBackoff struct {
	until time.Time
	delay time.Duration
}

// feedBackoffActive reports whether this convoy is still in its backoff
// window. Silent by design: the reason was logged when the backoff started.
func (m *ConvoyManager) feedBackoffActive(convoyID string) bool {
	v, ok := m.feedBackoff.Load(convoyID)
	if !ok {
		return false
	}
	state, ok := v.(*convoyFeedBackoff)
	return ok && time.Now().Before(state.until)
}

// recordFeedFailure doubles this convoy's backoff (capped) and logs a single
// line for the new state. reason describes why nothing could be fed.
func (m *ConvoyManager) recordFeedFailure(convoyID, reason string) {
	delay := convoyFeedBackoffBase
	if v, ok := m.feedBackoff.Load(convoyID); ok {
		if state, ok := v.(*convoyFeedBackoff); ok && state.delay > 0 {
			delay = state.delay * 2
		}
	}
	if delay > convoyFeedBackoffMax {
		delay = convoyFeedBackoffMax
	}
	m.feedBackoff.Store(convoyID, &convoyFeedBackoff{until: time.Now().Add(delay), delay: delay})
	m.logger("Convoy %s: %s — backing off %s", convoyID, reason, delay)
}

// clearFeedBackoff resets a convoy's backoff after a successful feed.
func (m *ConvoyManager) clearFeedBackoff(convoyID string) {
	m.feedBackoff.Delete(convoyID)
}

// issueBlocksFeeding reports whether an issue is already claimed by a worker
// and must not be slung again. Reads the rig's own store; an unreadable issue
// is not blocked, so an unavailable store cannot stall the feeder.
func (m *ConvoyManager) issueBlocksFeeding(rigName, issueID string) bool {
	m.storesMu.Lock()
	store := m.stores[rigName]
	m.storesMu.Unlock()
	if store == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()
	issue, err := store.GetIssue(ctx, issueID)
	if err != nil || issue == nil {
		return false
	}
	switch string(issue.Status) {
	case "hooked", "in_progress":
		return true
	}
	return false
}

// NewConvoyManager creates a new convoy manager.
// scanInterval controls the periodic stranded scan; 0 uses default (30s).
// stores maps store names ("hq", rig names) to beads stores for event polling.
// nil stores disables event-driven convoy checks (stranded scan still runs),
// unless openStores is provided for lazy initialization.
// openStores is called lazily if stores is nil (e.g., Dolt not ready at startup).
// isRigParked reports whether a rig should be skipped during polling (nil = never parked).
// gtPath is the resolved path to the gt binary for subprocess calls.
func NewConvoyManager(townRoot string, logger func(format string, args ...interface{}), gtPath string, scanInterval time.Duration, stores map[string]beadsdk.Storage, openStores func() map[string]beadsdk.Storage, isRigParked func(string) bool) *ConvoyManager {
	if scanInterval <= 0 {
		scanInterval = defaultStrandedScanInterval
	}
	if isRigParked == nil {
		isRigParked = func(string) bool { return false }
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &ConvoyManager{
		townRoot:            townRoot,
		scanInterval:        scanInterval,
		ctx:                 ctx,
		cancel:              cancel,
		logger:              logger,
		stores:              stores,
		openStores:          openStores,
		isRigParked:         isRigParked,
		gtPath:              gtPath,
		completionSweep:     defaultCompletionSweepInterval,
		skipInactiveRigs:    true,
		lastCompletionCheck: make(map[string]time.Time),
		inactiveSkip:        make(map[string]string),
		now:                 time.Now,
	}
	m.metrics.since = time.Now()
	return m
}

// Configure applies daemon.json patrols.convoy_manager settings. Call it
// before Start. A nil config keeps the defaults; invalid durations are logged
// and ignored.
func (m *ConvoyManager) Configure(cfg *ConvoyManagerConfig) {
	if cfg == nil {
		return
	}
	if cfg.StrandedInterval != "" {
		if d, err := time.ParseDuration(cfg.StrandedInterval); err == nil && d > 0 {
			m.scanInterval = d
		} else {
			m.logger("Convoy: ignoring invalid convoy_manager.stranded_interval %q", cfg.StrandedInterval)
		}
	}
	if cfg.CompletionSweepInterval != "" {
		if d, err := time.ParseDuration(cfg.CompletionSweepInterval); err == nil && d >= 0 {
			m.completionSweep = d
		} else {
			m.logger("Convoy: ignoring invalid convoy_manager.completion_sweep_interval %q", cfg.CompletionSweepInterval)
		}
	}
	if cfg.SkipInactiveRigs != nil {
		m.skipInactiveRigs = *cfg.SkipInactiveRigs
	}
}

// Start begins the convoy manager goroutines (event poll + stranded scan).
// It is safe to call multiple times; subsequent calls are no-ops.
func (m *ConvoyManager) Start() error {
	if !m.started.CompareAndSwap(false, true) {
		m.logger("Convoy: Start() already called, ignoring duplicate")
		return nil
	}
	m.wg.Add(2)
	go m.runEventPoll()
	go m.runStrandedScan()
	// Run a one-shot sweep to catch convoys that completed during any previous
	// outage or while the daemon was stopped.
	go m.runStartupSweep()
	return nil
}

// Stop gracefully stops the convoy manager and closes any beads stores it owns.
func (m *ConvoyManager) Stop() {
	m.cancel()
	m.wg.Wait()

	// Close stores (whether eagerly passed or lazily opened)
	m.storesMu.Lock()
	stores := m.stores
	m.stores = nil
	m.storesMu.Unlock()
	for name, store := range stores {
		if store != nil {
			if err := store.Close(); err != nil {
				m.logger("Convoy: error closing beads store (%s): %v", name, err)
			} else {
				m.logger("Convoy: closed beads store (%s)", name)
			}
		}
	}
}

// runEventPoll polls GetAllEventsSince every 5s and processes close events.
// If stores aren't available at startup (e.g., Dolt not ready), retries
// lazily via the openStores callback until stores become available.
func (m *ConvoyManager) runEventPoll() {
	defer m.wg.Done()

	m.storesMu.Lock()
	hasStores := len(m.stores) > 0
	hasOpener := m.openStores != nil
	m.storesMu.Unlock()

	if !hasStores && !hasOpener {
		m.logger("Convoy: no beads stores and no opener, event polling disabled")
		return
	}

	currentInterval := eventPollInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.storesMu.Lock()
			// Lazy store initialization: retry if stores not yet available
			if len(m.stores) == 0 {
				if m.openStores != nil {
					m.stores = m.openStores()
				}
				if len(m.stores) == 0 {
					m.storesMu.Unlock()
					continue // still not ready, try next tick
				}
			}
			// Take a snapshot of stores for this tick to avoid holding the
			// lock across potentially slow network/Dolt calls.
			snapshot := make(map[string]beadsdk.Storage, len(m.stores))
			for k, v := range m.stores {
				snapshot[k] = v
			}
			m.storesMu.Unlock()

			hadError := m.pollStoresSnapshot(snapshot)
			// Exponential backoff on consecutive errors to avoid hammering
			// a recovering Dolt server. Reset on success. (GH#2686)
			if hadError {
				newInterval := currentInterval * 2
				if newInterval > eventPollMaxBackoff {
					newInterval = eventPollMaxBackoff
				}
				if newInterval != currentInterval {
					currentInterval = newInterval
					ticker.Reset(currentInterval)
					m.logger("Convoy: poll backoff → %s", currentInterval)
				}
			} else if currentInterval != eventPollInterval {
				currentInterval = eventPollInterval
				ticker.Reset(currentInterval)
				m.logger("Convoy: poll recovered, interval reset to %s", currentInterval)
			}
		}
	}
}

// pollStoresSnapshot polls events from all non-parked stores in the snapshot.
// The first call is a warm-up: it advances high-water marks without
// processing events, preventing a burst of historical replay on restart.
// A per-cycle seen set deduplicates close events across stores so each
// issueID is processed at most once per poll cycle.
// Returns true if any store poll encountered an error.
func (m *ConvoyManager) pollStoresSnapshot(stores map[string]beadsdk.Storage) bool {
	seen := make(map[string]bool)
	hadError := false
	for name, store := range stores {
		if name != "hq" && m.isRigParked(name) {
			continue
		}
		if err := m.pollStore(name, store, stores, seen); err != nil {
			hadError = true
		}
	}
	m.seeded.CompareAndSwap(false, true)
	return hadError
}

// pollStore fetches new events from a single store and processes close events.
// Convoy lookups always use the hq store since convoys are hq-* prefixed.
// The stores snapshot is passed to avoid accessing m.stores without the lock.
// The seen set deduplicates issueIDs across stores within a poll cycle.
// Returns an error if the poll failed (used by caller for backoff decisions).
func (m *ConvoyManager) pollStore(name string, store beadsdk.Storage, stores map[string]beadsdk.Storage, seen map[string]bool) error {
	// Load per-store high-water mark.
	// Default to Unix epoch (not zero time) because Go's zero time.Time
	// (0001-01-01) causes Dolt's SQL driver to produce +Inf when converting
	// to a float parameter, triggering "Error 1366: +Inf is not a valid
	// value for double". Unix epoch is safe for all SQL backends.
	highWater := time.Unix(0, 0).UTC()
	if v, ok := m.lastEventIDs.Load(name); ok {
		highWater = v.(time.Time)
	}
	querySince := highWater
	if !highWater.Equal(time.Unix(0, 0).UTC()) {
		querySince = highWater.Add(-eventPollLookback)
		if querySince.Before(time.Unix(0, 0).UTC()) {
			querySince = time.Unix(0, 0).UTC()
		}
	}

	events, err := store.GetAllEventsSince(m.ctx, querySince)
	if err != nil {
		if isInfNaNError(err) {
			// A corrupted row in the events table has +Inf/-Inf/NaN stored in a
			// double column (e.g. created_at serialized from Go's zero time.Time).
			// Advance the high-water mark to now so future polls skip past the
			// bad row entirely. Events before now are missed, but the stranded
			// convoy scanner will catch any completions that were lost.
			now := time.Now().UTC()
			m.lastEventIDs.Store(name, now)
			m.logger("Convoy: event poll (%s): +Inf/NaN row detected, advancing HWM to %s to skip corrupt data", name, now.Format(time.RFC3339))
			return nil
		}
		m.logger("Convoy: event poll error (%s): %v", name, err)
		// Signal recovery mode so the stranded scan shortens its interval and
		// retries quickly once Dolt comes back.
		m.recoveryMode.Store(true)
		return err
	}

	// Advance high-water mark from all events
	for _, e := range events {
		if e.CreatedAt.After(highWater) {
			highWater = e.CreatedAt
		}
	}
	m.lastEventIDs.Store(name, highWater)

	// First poll cycle is warm-up only: advance marks, skip processing.
	// This prevents replaying the entire event history on daemon restart.
	if !m.seeded.Load() {
		for _, e := range events {
			if e.ID == "" {
				continue
			}
			if isCloseEvent(e) || isReopenEvent(e) {
				m.processedLifecycleEvents.Store(e.ID, true)
			}
		}
		return nil
	}

	// Use hq store for convoy lookups (convoys are hq-* prefixed)
	hqStore := stores["hq"]
	if hqStore == nil {
		m.logger("Convoy: hq store unavailable, skipping convoy lookups for %s events", name)
		return nil
	}

	for _, e := range events {
		issueID := e.IssueID
		if issueID == "" {
			continue
		}

		if isCloseEvent(e) || isReopenEvent(e) {
			if _, alreadyHandled := m.processedLifecycleEvents.LoadOrStore(e.ID, true); alreadyHandled {
				continue
			}
		}

		if isReopenEvent(e) {
			// Reopening starts a new close epoch for this issue. Clear both the
			// per-cycle and cross-cycle dedup so a later close is processed again.
			delete(seen, issueID)
			m.processedCloses.Delete(issueID)
			continue
		}

		if !isCloseEvent(e) {
			continue
		}

		// Deduplicate: skip if already processed this issueID in this poll cycle
		// (same close may appear in multiple stores or as multiple event types).
		// Reopen events clear this marker so close→reopen→close can be processed
		// twice even when all three events land in the same poll cycle.
		if seen[issueID] {
			continue
		}
		seen[issueID] = true

		// Cross-cycle dedup: skip if this issue's close was already processed
		// in a previous poll cycle. The same close event can appear from
		// multiple stores (replication) or across poll cycles when high-water
		// marks don't perfectly filter. See GH #1798.
		if _, alreadyProcessed := m.processedCloses.LoadOrStore(issueID, true); alreadyProcessed {
			continue
		}

		m.logger("Convoy: close detected: %s (from %s)", issueID, name)
		resolver := convoy.NewStoreResolver(m.townRoot, stores)
		checked := convoy.CheckConvoysForIssue(m.ctx, hqStore, m.townRoot, issueID, "Convoy", m.logger, m.gtPath, m.isRigParked, resolver)
		m.recordEventChecks(checked)
		convoy.FireCrossRigDepNotifications(m.ctx, issueID, m.townRoot, stores, m.logger)
	}
	return nil
}

// isInfNaNError reports whether err is a Dolt/SQL error about an invalid float
// value (+Inf, -Inf, NaN) in a double column. These errors arise when a
// corrupted row (e.g. created_at written from Go's zero time.Time via an old
// driver path) is encountered during a query. The caller should advance the
// high-water mark to skip past the offending row rather than entering
// permanent backoff.
func isInfNaNError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Dolt wraps values in single quotes: "'+Inf' is not a valid value for 'double'"
	// Match both quoted and unquoted forms.
	return strings.Contains(msg, "+Inf is not a valid value") ||
		strings.Contains(msg, "'+Inf' is not a valid value") ||
		strings.Contains(msg, "-Inf is not a valid value") ||
		strings.Contains(msg, "'-Inf' is not a valid value") ||
		strings.Contains(msg, "NaN is not a valid value") ||
		strings.Contains(msg, "'NaN' is not a valid value")
}

func isCloseEvent(e *beadsdk.Event) bool {
	if e == nil {
		return false
	}
	if e.EventType == beadsdk.EventClosed {
		return true
	}
	return e.EventType == beadsdk.EventStatusChanged &&
		e.NewValue != nil &&
		*e.NewValue == "closed"
}

func isReopenEvent(e *beadsdk.Event) bool {
	if e == nil {
		return false
	}
	if e.EventType == beadsdk.EventReopened {
		return true
	}
	return e.EventType == beadsdk.EventStatusChanged &&
		e.OldValue != nil &&
		*e.OldValue == "closed" &&
		(e.NewValue == nil || *e.NewValue != "closed")
}

// runStrandedScan is the periodic stranded convoy scan loop.
// During recovery mode (after Dolt poll errors) the interval shrinks to 5s
// so a successful scan fires promptly once Dolt comes back. Recovery mode is
// cleared after the first successful scan.
//
// The pause is measured from the end of a scan, so a slow scan can never make
// the next one start immediately (a 30s ticker with a 14s+ scan ran them back
// to back).
func (m *ConvoyManager) runStrandedScan() {
	defer m.wg.Done()

	// Run once immediately, then after each pause
	m.scan()

	timer := time.NewTimer(m.nextScanDelay())
	defer timer.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			m.scan()
			m.maybeLogMetrics()
			timer.Reset(m.nextScanDelay())
		}
	}
}

// nextScanDelay is the pause before the next stranded scan. While in
// recovery mode it is short so we retry quickly after a Dolt outage.
func (m *ConvoyManager) nextScanDelay() time.Duration {
	if m.recoveryMode.Load() && m.scanInterval > recoveryScanInterval {
		return recoveryScanInterval
	}
	return m.scanInterval
}

// scan runs one stranded scan cycle: find stranded convoys, feed or close each.
// Serialized by scanMu to prevent concurrent scans from spawning duplicate checks.
func (m *ConvoyManager) scan() {
	m.scanMu.Lock()
	defer m.scanMu.Unlock()

	scanStart := time.Now()
	defer func() {
		m.metrics.scans.Add(1)
		m.metrics.scanNanos.Add(int64(time.Since(scanStart)))
	}()

	stranded, err := m.findStranded()
	if err != nil {
		m.logger("Convoy: stranded scan failed: %s", util.FirstLine(err.Error()))
		return
	}
	// Successful scan: clear recovery mode so the ticker returns to normal interval.
	m.recoveryMode.Store(false)

	m.pruneCompletionState(stranded)
	var rigs *rigActivity
	if m.skipInactiveRigs {
		rigs = newRigActivity(m.townRoot, m.isRigParked)
	}

	for _, c := range stranded {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		if c.ReadyCount > 0 {
			m.feedFirstReady(c)
		} else if c.TrackedCount == 0 {
			// Empty convoy — but skip if it was just created (GH#2303).
			// The sling's bd dep add may not be visible in Dolt yet.
			if !c.CreatedAt.IsZero() && time.Since(c.CreatedAt) < convoyGracePeriod {
				m.logger("Convoy %s: empty but within grace period (created %s ago) — skipping", c.ID, time.Since(c.CreatedAt).Round(time.Second))
				continue
			}
			m.metrics.checksEmpty.Add(1)
			m.closeEmptyConvoy(c.ID)
		} else {
			// Tracked issues exist but none are ready. This could mean:
			// (a) all tracked issues are closed → convoy should auto-close
			// (b) issues are blocked/in-progress → needs agent review
			// Run convoy check to handle case (a); it's a no-op for (b).
			// The close-event poll already checks a convoy when one of its
			// tracked issues closes, so here we check only when the scan
			// sees everything closed, or as a slow sweep.
			check, reason := m.completionAction(c, m.now(), rigs)
			if !check {
				continue
			}
			m.logger("Convoy %s: %d tracked issues, 0 ready — checking completion (%s)", c.ID, c.TrackedCount, reason)
			m.checkConvoyCompletion(c.ID)
			m.markCompletionChecked(c.ID, m.now())
		}
	}
}

// Completion-check reasons returned by completionAction.
const (
	completionReasonLegacy    = "every-scan"
	completionReasonAllClosed = "all tracked closed"
	completionReasonSweep     = "sweep"
	completionReasonNotDue    = "not due"
	completionReasonInactive  = "inactive rig"
)

// completionAction decides whether the scan runs `gt convoy check` for a
// convoy that has tracked issues but none ready, and why.
func (m *ConvoyManager) completionAction(c strandedConvoyInfo, now time.Time, rigs *rigActivity) (bool, string) {
	if m.completionSweep <= 0 {
		m.metrics.checksLegacy.Add(1)
		return true, completionReasonLegacy
	}

	// The scan itself saw every tracked issue closed: a close event was
	// missed (restart, Dolt outage). Check now.
	if c.OpenIssues != nil && len(c.OpenIssues) == 0 {
		m.clearInactiveSkip(c.ID)
		m.metrics.checksAllClosed.Add(1)
		return true, completionReasonAllClosed
	}

	// An open tracked issue on a parked, docked or removed rig cannot
	// progress, so the convoy cannot complete until that rig is back.
	if rigs != nil {
		for _, id := range c.OpenIssues {
			if why := rigs.inactiveReason(id); why != "" {
				m.noteInactiveSkip(c.ID, fmt.Sprintf("tracked issue %s: %s", id, why))
				m.metrics.skipInactive.Add(1)
				return false, completionReasonInactive
			}
		}
	}
	m.clearInactiveSkip(c.ID)

	m.completionMu.Lock()
	last, seen := m.lastCompletionCheck[c.ID]
	if !seen && c.OpenIssues != nil {
		// First sighting with known-open issues: the check would be a
		// no-op, so spread first sweeps across the interval instead of
		// checking every convoy in the same scan.
		last = now.Add(-completionStagger(c.ID, m.completionSweep))
		m.lastCompletionCheck[c.ID] = last
		seen = true
	}
	m.completionMu.Unlock()

	if !seen || now.Sub(last) >= m.completionSweep {
		m.metrics.checksSweep.Add(1)
		return true, completionReasonSweep
	}
	m.metrics.skipNotDue.Add(1)
	return false, completionReasonNotDue
}

// completionStagger maps a convoy ID to a stable offset in [0, interval).
func completionStagger(convoyID string, interval time.Duration) time.Duration {
	secs := int64(interval / time.Second)
	if secs <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(convoyID))
	return time.Duration(int64(h.Sum32())%secs) * time.Second
}

// markCompletionChecked records that a convoy's completion was just checked.
func (m *ConvoyManager) markCompletionChecked(convoyID string, at time.Time) {
	m.completionMu.Lock()
	m.lastCompletionCheck[convoyID] = at
	m.completionMu.Unlock()
}

// recordEventChecks notes convoys the close-event poll just checked, so the
// sweep does not repeat the work.
func (m *ConvoyManager) recordEventChecks(convoyIDs []string) {
	if len(convoyIDs) == 0 {
		return
	}
	now := m.now()
	m.completionMu.Lock()
	for _, id := range convoyIDs {
		m.lastCompletionCheck[id] = now
	}
	m.completionMu.Unlock()
	m.metrics.checksEvent.Add(int64(len(convoyIDs)))
}

// pruneCompletionState drops state for convoys no longer open.
func (m *ConvoyManager) pruneCompletionState(stranded []strandedConvoyInfo) {
	open := make(map[string]bool, len(stranded))
	for _, c := range stranded {
		open[c.ID] = true
	}
	m.completionMu.Lock()
	defer m.completionMu.Unlock()
	for id := range m.lastCompletionCheck {
		if !open[id] {
			delete(m.lastCompletionCheck, id)
		}
	}
	for id := range m.inactiveSkip {
		if !open[id] {
			delete(m.inactiveSkip, id)
		}
	}
}

// noteInactiveSkip logs a skip once per reason change.
func (m *ConvoyManager) noteInactiveSkip(convoyID, reason string) {
	m.completionMu.Lock()
	prev := m.inactiveSkip[convoyID]
	m.inactiveSkip[convoyID] = reason
	m.completionMu.Unlock()
	if prev != reason {
		m.logger("Convoy %s: skipping completion checks — %s", convoyID, reason)
	}
}

func (m *ConvoyManager) clearInactiveSkip(convoyID string) {
	m.completionMu.Lock()
	_, had := m.inactiveSkip[convoyID]
	delete(m.inactiveSkip, convoyID)
	m.completionMu.Unlock()
	if had {
		m.logger("Convoy %s: resuming completion checks", convoyID)
	}
}

// maybeLogMetrics logs and resets the counters once per convoyMetricsInterval.
func (m *ConvoyManager) maybeLogMetrics() {
	m.metrics.mu.Lock()
	defer m.metrics.mu.Unlock()
	elapsed := time.Since(m.metrics.since)
	if elapsed < convoyMetricsInterval {
		return
	}
	m.logger("%s", m.metrics.snapshotLine(elapsed))
	snap := m.metrics.snapshot(elapsed)
	if err := atomicfile.EnsureDirAndWriteJSON(ConvoyMetricsFile(m.townRoot), snap); err != nil {
		m.logger("Convoy: writing metrics snapshot: %v", err)
	}
	m.metrics.reset()
}

// ConvoyMetricsSnapshot is the last hourly convoy-manager metrics window,
// written to ConvoyMetricsFile so `gt maintain` can report it.
type ConvoyMetricsSnapshot struct {
	WrittenAt        time.Time `json:"written_at"`
	WindowSeconds    int64     `json:"window_seconds"`
	ConvoyChecks     int64     `json:"convoy_checks"`
	EventChecks      int64     `json:"event_checks"`
	AllClosedChecks  int64     `json:"all_closed_checks"`
	SweepChecks      int64     `json:"sweep_checks"`
	EveryScanChecks  int64     `json:"every_scan_checks"`
	EmptyChecks      int64     `json:"empty_checks"`
	SkippedNotDue    int64     `json:"skipped_not_due"`
	SkippedInactive  int64     `json:"skipped_inactive_rig"`
	StrandedScans    int64     `json:"stranded_scans"`
	StrandedScanAvgS float64   `json:"stranded_scan_avg_seconds"`
}

// ConvoyMetricsFile is where the convoy manager writes its hourly snapshot.
func ConvoyMetricsFile(townRoot string) string {
	return filepath.Join(townRoot, "daemon", "convoy_metrics.json")
}

func (cm *convoyMetrics) snapshot(elapsed time.Duration) ConvoyMetricsSnapshot {
	s := ConvoyMetricsSnapshot{
		WrittenAt:       time.Now().UTC(),
		WindowSeconds:   int64(elapsed / time.Second),
		EventChecks:     cm.checksEvent.Load(),
		AllClosedChecks: cm.checksAllClosed.Load(),
		SweepChecks:     cm.checksSweep.Load(),
		EveryScanChecks: cm.checksLegacy.Load(),
		EmptyChecks:     cm.checksEmpty.Load(),
		SkippedNotDue:   cm.skipNotDue.Load(),
		SkippedInactive: cm.skipInactive.Load(),
		StrandedScans:   cm.scans.Load(),
	}
	s.ConvoyChecks = s.EventChecks + s.AllClosedChecks + s.SweepChecks + s.EveryScanChecks + s.EmptyChecks
	if s.StrandedScans > 0 {
		s.StrandedScanAvgS = time.Duration(cm.scanNanos.Load() / s.StrandedScans).Seconds()
	}
	return s
}

// snapshotLine formats the counters as one log line.
func (cm *convoyMetrics) snapshotLine(elapsed time.Duration) string {
	event, allClosed := cm.checksEvent.Load(), cm.checksAllClosed.Load()
	sweep, legacy, empty := cm.checksSweep.Load(), cm.checksLegacy.Load(), cm.checksEmpty.Load()
	scans := cm.scans.Load()
	avg := time.Duration(0)
	if scans > 0 {
		avg = time.Duration(cm.scanNanos.Load() / scans)
	}
	return fmt.Sprintf("Convoy metrics (last %s): convoy checks=%d (event=%d all-closed=%d sweep=%d every-scan=%d empty=%d); skipped not-due=%d inactive-rig=%d; stranded scans=%d avg=%s",
		elapsed.Round(time.Minute), event+allClosed+sweep+legacy+empty, event, allClosed, sweep, legacy, empty,
		cm.skipNotDue.Load(), cm.skipInactive.Load(), scans, avg.Round(100*time.Millisecond))
}

func (cm *convoyMetrics) reset() {
	for _, c := range []*atomic.Int64{&cm.checksEvent, &cm.checksAllClosed, &cm.checksSweep, &cm.checksLegacy,
		&cm.checksEmpty, &cm.skipNotDue, &cm.skipInactive, &cm.scans, &cm.scanNanos} {
		c.Store(0)
	}
	cm.since = time.Now()
}

// rigActivity answers, for one scan, whether a tracked issue routes to a rig
// that cannot make progress: no route for its prefix (rig removed), a route
// to a rig directory that no longer exists, or a parked/docked rig.
type rigActivity struct {
	townRoot string
	routes   map[string]string // prefix -> route path; nil = unknown, judge nothing
	parked   func(string) bool
	rigCache map[string]string
}

func newRigActivity(townRoot string, parked func(string) bool) *rigActivity {
	ra := &rigActivity{townRoot: townRoot, parked: parked, rigCache: make(map[string]string)}
	routes, err := beads.LoadRoutes(filepath.Join(townRoot, ".beads"))
	if err != nil || len(routes) == 0 {
		return ra
	}
	ra.routes = make(map[string]string, len(routes))
	for _, r := range routes {
		ra.routes[r.Prefix] = r.Path
	}
	return ra
}

// inactiveReason returns why issueID's rig is inactive, or "" if it is active
// or cannot be judged.
func (r *rigActivity) inactiveReason(issueID string) string {
	if r == nil || r.routes == nil {
		return ""
	}
	id := issueID
	if strings.HasPrefix(id, "external:") {
		// external:<rig>:<id>
		if parts := strings.SplitN(id, ":", 3); len(parts) == 3 {
			id = parts[2]
		}
	}
	prefix := beads.ExtractPrefix(id)
	if prefix == "" {
		return ""
	}
	path, ok := r.routes[prefix]
	if !ok {
		return fmt.Sprintf("no route for prefix %s (rig removed)", prefix)
	}
	if path == "" || path == "." {
		return "" // town-level bead
	}
	rig := strings.SplitN(path, "/", 2)[0]
	if why, ok := r.rigCache[rig]; ok {
		return why
	}
	why := ""
	if _, err := os.Stat(filepath.Join(r.townRoot, rig)); os.IsNotExist(err) {
		why = fmt.Sprintf("rig %s removed", rig)
	} else if r.parked != nil && r.parked(rig) {
		why = fmt.Sprintf("rig %s parked or docked", rig)
	}
	r.rigCache[rig] = why
	return why
}

// findStranded runs `gt convoy stranded --json` and parses the output.
func (m *ConvoyManager) findStranded() ([]strandedConvoyInfo, error) {
	cmd := exec.CommandContext(m.ctx, m.gtPath, "convoy", "stranded", "--json")
	cmd.Dir = m.townRoot
	cmd.Env = bdReadOnlyRoutingEnv(m.townRoot)
	util.SetProcessGroup(cmd)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s", util.FirstLine(stderr.String()))
	}

	var stranded []strandedConvoyInfo
	if err := json.Unmarshal(stdout.Bytes(), &stranded); err != nil {
		// Include first line of raw output for debugging (e.g., non-JSON warnings on stdout)
		raw := util.FirstLine(stdout.String())
		return nil, fmt.Errorf("parsing stranded JSON: %w (raw: %q)", err, raw)
	}

	return stranded, nil
}

// feedFirstReady iterates through all ready issues in a stranded convoy and
// dispatches the first one that can be successfully slung. Issues are skipped
// (with logging) when the prefix is unresolvable, the rig has no route, the
// rig is parked, or the sling command fails. This ensures convoys progress
// even when some issues target unavailable rigs.
func (m *ConvoyManager) feedFirstReady(c strandedConvoyInfo) {
	if len(c.ReadyIssues) == 0 {
		return
	}
	if m.feedBackoffActive(c.ID) {
		return
	}

	for _, issueID := range c.ReadyIssues {
		prefix := beads.ExtractPrefix(issueID)
		if prefix == "" {
			m.logger("Convoy %s: no prefix for %s, skipping", c.ID, issueID)
			continue
		}

		rig := beads.GetRigNameForPrefix(m.townRoot, prefix)
		if rig == "" {
			m.logger("Convoy %s: no rig for %s (prefix %s), skipping", c.ID, issueID, prefix)
			continue
		}

		if m.isRigParked(rig) {
			m.logger("Convoy %s: rig %s is parked, skipping %s", c.ID, rig, issueID)
			continue
		}

		// A bead already hooked or in progress has a worker. `gt sling`
		// refuses it, so attempting one is pure noise and Dolt load.
		if m.issueBlocksFeeding(rig, issueID) {
			continue
		}

		m.logger("Convoy %s: feeding %s to %s", c.ID, issueID, rig)

		slingArgs := []string{"sling", issueID, rig, "--no-boot"}
		if c.BaseBranch != "" {
			slingArgs = append(slingArgs, "--base-branch="+c.BaseBranch)
		}
		cmd := exec.CommandContext(m.ctx, m.gtPath, slingArgs...)
		cmd.Dir = m.townRoot
		cmd.Env = bdMutationRoutingEnv(m.townRoot)
		util.SetProcessGroup(cmd)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			m.logger("Convoy %s: sling %s failed: %s", c.ID, issueID, util.FirstLine(stderr.String()))
			continue
		}
		m.clearFeedBackoff(c.ID)
		return // Successfully dispatched one issue
	}

	m.recordFeedFailure(c.ID, fmt.Sprintf("no dispatchable issues (all %d skipped)", len(c.ReadyIssues)))
}

// checkConvoyCompletion runs gt convoy check to auto-close a convoy whose
// tracked issues may all be closed. This handles the case where the event poll
// missed the close events (e.g., daemon restart, Dolt latency).
func (m *ConvoyManager) checkConvoyCompletion(convoyID string) {
	cmd := exec.CommandContext(m.ctx, m.gtPath, "convoy", "check", convoyID)
	cmd.Dir = m.townRoot
	cmd.Env = bdMutationRoutingEnv(m.townRoot)
	util.SetProcessGroup(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		m.logger("Convoy %s: completion check failed: %s", convoyID, util.FirstLine(stderr.String()))
	}
}

// closeEmptyConvoy runs gt convoy check to auto-close an empty convoy.
func (m *ConvoyManager) closeEmptyConvoy(convoyID string) {
	m.logger("Convoy %s: auto-closing (empty)", convoyID)

	cmd := exec.CommandContext(m.ctx, m.gtPath, "convoy", "check", convoyID)
	cmd.Dir = m.townRoot
	cmd.Env = bdMutationRoutingEnv(m.townRoot)
	util.SetProcessGroup(cmd)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		m.logger("Convoy %s: check failed: %s", convoyID, util.FirstLine(stderr.String()))
	}
}

// runStartupSweep runs one convoy check pass after a brief delay to catch
// convoys that completed while the daemon was stopped or Dolt was unavailable.
// It waits 10 seconds so Dolt has time to stabilize before the first query.
// This goroutine is not tracked in wg because it is short-lived (exits after
// a single scan) and does not need to participate in the Stop() shutdown.
func (m *ConvoyManager) runStartupSweep() {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-m.ctx.Done():
		return
	case <-timer.C:
	}
	m.logger("Convoy: running startup sweep for stranded convoys")
	m.scan()
}
