package config

import "sync"

// DefaultCloseOnMerge is the fork default for workflow.close_on_merge.
//
// true  = a source bead submitted to the merge queue is held open
//
//	(in_progress + awaiting-merge:<mr>) until the Refinery's post-merge
//	receipt closes it, and merge rejections reopen/re-dispatch it.
//
// false = every path behaves exactly as upstream: gt done closes the source
//
//	bead the moment the MR bead exists, and gt mq reject leaves it alone.
//
// See mayor/GAS_TOWN_DEV_CYCLE_REVIEW_2026-09-02.md item 6 (hq-19pxc, hq-tx4md).
const DefaultCloseOnMerge = true

// TownWorkflowConfig is the town-level workflow settings block; it is the same
// type as WorkflowConfig (which also carries formulas_dir) so one JSON object
// serves both fork additions.
type TownWorkflowConfig = WorkflowConfig

// GetCloseOnMerge returns the configured value or DefaultCloseOnMerge.
func (c *TownWorkflowConfig) GetCloseOnMerge() bool {
	if c == nil || c.CloseOnMerge == nil {
		return DefaultCloseOnMerge
	}
	return *c.CloseOnMerge
}

// CloseOnMergeEnabled reports whether the close-on-landed lifecycle is active
// for these settings.
func (s *TownSettings) CloseOnMergeEnabled() bool {
	if s == nil {
		return DefaultCloseOnMerge
	}
	return s.Workflow.GetCloseOnMerge()
}

var (
	closeOnMergeMu    sync.Mutex
	closeOnMergeCache = map[string]bool{}
)

// CloseOnMergeEnabledForTown loads town settings and reports whether the
// close-on-landed lifecycle is active. It is deliberately failure-tolerant:
// an unreadable settings file falls back to the compiled-in default rather
// than blocking a completion or a rejection.
//
// The answer is cached per town root for the life of the process. Every caller
// is a short-lived CLI invocation or a single daemon tick, so a stale value
// cannot outlive an operator's edit by more than one command.
func CloseOnMergeEnabledForTown(townRoot string) bool {
	if townRoot == "" {
		return DefaultCloseOnMerge
	}
	closeOnMergeMu.Lock()
	defer closeOnMergeMu.Unlock()
	if v, ok := closeOnMergeCache[townRoot]; ok {
		return v
	}
	settings, err := LoadOrCreateTownSettings(TownSettingsPath(townRoot))
	v := DefaultCloseOnMerge
	if err == nil {
		v = settings.CloseOnMergeEnabled()
	}
	closeOnMergeCache[townRoot] = v
	return v
}

// ResetCloseOnMergeCache clears the per-town cache. Tests use it after
// rewriting a settings file.
func ResetCloseOnMergeCache() {
	closeOnMergeMu.Lock()
	defer closeOnMergeMu.Unlock()
	closeOnMergeCache = map[string]bool{}
}
