package daemon

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// dogMol records the step outcomes of one daemon dog run in memory.
//
// Daemon dogs (doctor, checkpoint, jsonl, compactor, reaper fallback) run
// deterministic work in-process. They used to pour a persistent wisp molecule
// (`bd mol wisp <formula>`: a root plus one wisp per formula step, with
// parent-child and blocks dependencies) on every run purely for observability,
// then try to close it with a single unordered `bd close` pass. Formula steps
// are chained with `needs`, so closing them in listing order hit "cannot close
// blocked issue", the root then failed with "N open child issue(s)", and every
// run leaked part of its molecule. At doctor_dog's 5-minute cadence that was
// ~2,500 wisps a day (plus events and dependency rows) left open in hq.
//
// Nothing reads those molecules: the work happens here, and the daemon log is
// the run record. So a dog run no longer creates beads at all. The step API is
// kept so call sites still document their phases, and failed steps are logged
// once when the run ends.
type dogMol struct {
	formula string
	started time.Time
	logger  interface{ Printf(string, ...interface{}) }

	mu     sync.Mutex
	steps  map[string]string // step slug -> "" (ok) or failure reason
	closed bool
}

// pourDogMolecule starts in-memory step tracking for a dog run. It never
// creates wisps; see dogMol. vars are accepted for call-site compatibility.
func (d *Daemon) pourDogMolecule(formulaName string, _ map[string]string) *dogMol {
	dm := &dogMol{
		formula: formulaName,
		started: time.Now(),
		steps:   make(map[string]string),
	}
	if d != nil && d.logger != nil {
		dm.logger = d.logger
	}
	return dm
}

// closeStep records a step as completed. A recorded failure is kept.
func (dm *dogMol) closeStep(stepSlug string) {
	if dm == nil {
		return
	}
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if reason, ok := dm.steps[stepSlug]; ok && reason != "" {
		return
	}
	dm.steps[stepSlug] = ""
}

// failStep records a step as failed with a reason.
func (dm *dogMol) failStep(stepSlug, reason string) {
	if dm == nil {
		return
	}
	if reason == "" {
		reason = "failed"
	}
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.steps[stepSlug] = reason
}

// failedSteps returns sorted "slug: reason" entries for failed steps.
func (dm *dogMol) failedSteps() []string {
	if dm == nil {
		return nil
	}
	dm.mu.Lock()
	defer dm.mu.Unlock()
	var failed []string
	for slug, reason := range dm.steps {
		if reason != "" {
			failed = append(failed, slug+": "+reason)
		}
	}
	sort.Strings(failed)
	return failed
}

// close ends the run. It is idempotent and safe on every exit path (callers
// defer it). Failed steps are logged once; successful runs stay quiet because
// each dog already logs its own summary.
func (dm *dogMol) close() {
	if dm == nil {
		return
	}
	dm.mu.Lock()
	if dm.closed {
		dm.mu.Unlock()
		return
	}
	dm.closed = true
	dm.mu.Unlock()

	failed := dm.failedSteps()
	if len(failed) == 0 || dm.logger == nil {
		return
	}
	dm.logger.Printf("dog_run: %s finished in %s with %d failed step(s): %s",
		dm.formula, time.Since(dm.started).Round(time.Millisecond), len(failed), strings.Join(failed, "; "))
}
