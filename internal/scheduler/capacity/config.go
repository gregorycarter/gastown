// Package capacity provides types and pure functions for the capacity-controlled
// dispatch scheduler. The impure orchestration (dispatch loop, enqueue, epic/convoy
// resolution) stays in cmd but uses types and pure functions from this package.
package capacity

import "time"

// SchedulerConfig configures the capacity scheduler for polecat dispatch.
// This is a town-wide setting (not per-rig) because capacity control is host-wide:
// API rate limits, memory, and CPU are shared resources across all rigs.
//
// Behavior is driven entirely by MaxPolecats:
//
//	-1 (default): direct dispatch — gt sling works as before, near-zero overhead
//	 0:           direct dispatch (same as -1)
//	 N > 0:       deferred dispatch — labels/metadata applied, daemon dispatches
type SchedulerConfig struct {
	// MaxPolecats is the max concurrent polecats across ALL rigs.
	// Includes both scheduler-dispatched and directly-slung polecats.
	// nil/absent = default (-1, direct dispatch). 0 = direct dispatch (same as -1).
	// N > 0 = deferred dispatch with capacity control.
	MaxPolecats *int `json:"max_polecats,omitempty"`

	// BatchSize is the number of beads to dispatch per heartbeat tick.
	// Limits spawn rate per 3-minute cycle.
	// nil/absent = default (1). Explicit 0 is rejected by config setter.
	BatchSize *int `json:"batch_size,omitempty"`

	// SpawnDelay is the delay between spawns to prevent Dolt lock contention.
	// Default: "0s".
	SpawnDelay string `json:"spawn_delay,omitempty"`

	// QueueFloor is the minimum number of ready sling contexts the auto-feeder
	// keeps in the queue. 0 (default) disables auto-feed entirely, preserving
	// the historical behaviour where only `gt sling` and the convoy feeder
	// enqueue work. N > 0 makes the daemon top the queue up from `bd ready`
	// on every dispatch tick.
	QueueFloor *int `json:"queue_floor,omitempty"`

	// AutoFeedLabels is an allow-list of labels for auto-feed candidates.
	// Empty (default) means any label is acceptable.
	AutoFeedLabels []string `json:"autofeed_labels,omitempty"`

	// AutoFeedExcludeLabels is a deny-list of labels. A ready bead carrying
	// any of these is never auto-fed. nil means "use the defaults"; an
	// explicitly empty JSON array disables exclusion.
	AutoFeedExcludeLabels []string `json:"autofeed_exclude_labels,omitempty"`

	// AutoFeedMaxOpsSlots caps how many currently-working polecats may be on
	// ops-labelled beads (the exclude list plus ci-train-failure, watchdog,
	// ci). Product work is what the factory exists for; ops work must not
	// take every slot. P0/P1 beads bypass the cap. Default 1.
	AutoFeedMaxOpsSlots *int `json:"autofeed_max_ops_slots,omitempty"`

	// MaxPendingMRs is the per-rig work-in-progress cap: when a rig already
	// has this many polecats holding an unlanded merge request, the
	// scheduler defers dispatch of NEW work for that rig so a landing outage
	// turns into an idle rig instead of a pile of MRs (each holding a
	// worktree, disk and a polecat directory slot). Recovery contexts, P0
	// beads and WIPCapExemptLabels still dispatch. nil/absent = default (8);
	// 0 disables the cap.
	MaxPendingMRs *int `json:"max_pending_mrs,omitempty"`

	// MaxPendingMRsByRig overrides MaxPendingMRs for individual rigs
	// (rig name -> limit; 0 disables the cap for that rig).
	MaxPendingMRsByRig map[string]int `json:"max_pending_mrs_by_rig,omitempty"`

	// WIPCapExemptLabels are work-bead labels that bypass the WIP cap because
	// the work unblocks landing. nil means "use the defaults"; an explicitly
	// empty JSON array exempts nothing by label.
	WIPCapExemptLabels []string `json:"wip_cap_exempt_labels,omitempty"`
}

// DefaultMaxPendingMRs is the default per-rig WIP cap on unlanded MRs.
const DefaultMaxPendingMRs = 8

// DefaultWIPCapExemptLabels mark work that unblocks landing (tracked release
// remediation, CI train failures, fixes to the landing machinery itself) and
// therefore must not wait behind the cap.
var DefaultWIPCapExemptLabels = []string{
	"release-remediation",
	"ci-train-failure",
	"landing-unblock",
}

// GetMaxPendingMRs returns the WIP cap for rig: the per-rig override when
// present, else MaxPendingMRs, else DefaultMaxPendingMRs. Negative values are
// clamped to 0 (disabled).
func (c *SchedulerConfig) GetMaxPendingMRs(rig string) int {
	limit := DefaultMaxPendingMRs
	if c != nil {
		if override, ok := c.MaxPendingMRsByRig[rig]; ok && rig != "" {
			limit = override
		} else if c.MaxPendingMRs != nil {
			limit = *c.MaxPendingMRs
		}
	}
	if limit < 0 {
		return 0
	}
	return limit
}

// GetWIPCapExemptLabels returns the WIP-cap label exemptions. An unset field
// yields the defaults; an explicitly empty list exempts nothing by label.
func (c *SchedulerConfig) GetWIPCapExemptLabels() []string {
	if c == nil || c.WIPCapExemptLabels == nil {
		return DefaultWIPCapExemptLabels
	}
	return c.WIPCapExemptLabels
}

// DefaultAutoFeedExcludeLabels are labels that mark work the auto-feeder must
// never dispatch on its own: operator dispositions, explicit dispatch holds,
// policy changes, and the agent-lifecycle roles. A control-plane topic label
// alone does not hold a bead or make it consume an operations slot.
var DefaultAutoFeedExcludeLabels = []string{
	"needs-operator",
	"needs-operator-rollout",
	"live-validation",
	"gt-fork",
	"dispatch:hold",
	"policy",
	"patrol",
	"refinery",
	"witness",
}

// OpsLabels are labels that mark a bead as operations work rather than
// product work. The auto-feeder limits how many slots these may occupy
// concurrently (AutoFeedMaxOpsSlots).
var OpsLabels = func() []string {
	labels := make([]string, 0, len(DefaultAutoFeedExcludeLabels)+3)
	labels = append(labels, DefaultAutoFeedExcludeLabels...)
	return append(labels, "ci-train-failure", "watchdog", "ci")
}()

// DefaultQueueFloor is the auto-feed floor when unset: 0 = feature off.
const DefaultQueueFloor = 0

// DefaultAutoFeedMaxOpsSlots is the default cap on concurrently-working
// ops-labelled beads chosen by the auto-feeder.
const DefaultAutoFeedMaxOpsSlots = 1

// DefaultSchedulerConfig returns a SchedulerConfig with sensible defaults.
// MaxPolecats=-1 means direct dispatch (no scheduler overhead).
func DefaultSchedulerConfig() *SchedulerConfig {
	defaultMax := -1
	defaultBatch := 1
	return &SchedulerConfig{
		MaxPolecats: &defaultMax,
		BatchSize:   &defaultBatch,
		SpawnDelay:  "0s",
	}
}

// GetMaxPolecats returns MaxPolecats or the default (-1, direct dispatch) if unset.
func (c *SchedulerConfig) GetMaxPolecats() int {
	if c == nil || c.MaxPolecats == nil {
		return -1
	}
	return *c.MaxPolecats
}

// GetBatchSize returns BatchSize or the default (1) if unset.
func (c *SchedulerConfig) GetBatchSize() int {
	if c == nil || c.BatchSize == nil {
		return 1
	}
	return *c.BatchSize
}

// GetQueueFloor returns QueueFloor or the default (0, auto-feed off).
// Negative values are clamped to 0.
func (c *SchedulerConfig) GetQueueFloor() int {
	if c == nil || c.QueueFloor == nil {
		return DefaultQueueFloor
	}
	if *c.QueueFloor < 0 {
		return 0
	}
	return *c.QueueFloor
}

// GetAutoFeedLabels returns the auto-feed allow-list (nil = allow any label).
func (c *SchedulerConfig) GetAutoFeedLabels() []string {
	if c == nil {
		return nil
	}
	return c.AutoFeedLabels
}

// GetAutoFeedExcludeLabels returns the auto-feed deny-list. An unset field
// yields the defaults; an explicitly empty list disables exclusion.
func (c *SchedulerConfig) GetAutoFeedExcludeLabels() []string {
	if c == nil || c.AutoFeedExcludeLabels == nil {
		return DefaultAutoFeedExcludeLabels
	}
	return c.AutoFeedExcludeLabels
}

// GetAutoFeedMaxOpsSlots returns the ops-slot cap or the default (1).
// Negative values are clamped to 0 (no ops work auto-fed).
func (c *SchedulerConfig) GetAutoFeedMaxOpsSlots() int {
	if c == nil || c.AutoFeedMaxOpsSlots == nil {
		return DefaultAutoFeedMaxOpsSlots
	}
	if *c.AutoFeedMaxOpsSlots < 0 {
		return 0
	}
	return *c.AutoFeedMaxOpsSlots
}

// GetSpawnDelay returns SpawnDelay as a duration, defaulting to 0s.
func (c *SchedulerConfig) GetSpawnDelay() time.Duration {
	if c == nil || c.SpawnDelay == "" {
		return 0
	}
	return ParseDurationOrDefault(c.SpawnDelay, 0)
}

// IsDeferred returns true when the scheduler is configured for deferred dispatch
// (max_polecats > 0). Returns false for direct dispatch (-1) and disabled (0).
func (c *SchedulerConfig) IsDeferred() bool {
	return c.GetMaxPolecats() > 0
}

// ParseDurationOrDefault parses a Go duration string, returning fallback on error or empty input.
func ParseDurationOrDefault(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fallback
	}
	return d
}
