package daemon

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/steveyegge/gastown/internal/constants"
	"github.com/steveyegge/gastown/internal/tmux"
)

// PressureResult holds the outcome of a pressure check.
type PressureResult struct {
	// OK is true if spawning should proceed.
	OK bool

	// Reason describes why spawning was blocked (empty if OK).
	Reason string

	// LoadAvg5 is the 5-minute load average at check time. Load-5 is used in
	// preference to load-1 because a CI train's 20-minute burst otherwise
	// toggles the gate several times per train.
	LoadAvg5 float64

	// MemAvailableGB is approximate available memory in GB.
	MemAvailableGB float64

	// ActiveSessions is the count of active Claude agent sessions.
	ActiveSessions int
}

// cpuExemptKinds are the spawn kinds never deferred on CPU pressure. The
// Refinery and Witness are the merge path: deferring their respawn while a CI
// train saturates the host stalls exactly the work that would end the load.
// They remain subject to the memory and session-count tiers.
var cpuExemptKinds = map[string]bool{
	constants.RoleRefinery: true,
	constants.RoleWitness:  true,
}

// cpuPressureDeferred applies enter/exit hysteresis to the CPU tier.
//
// Without it a host oscillating around the threshold flips between deferring
// and dispatching on every 3-minute tick. Deferral engages above threshold and
// clears only once load/core falls below threshold*exitRatio.
func cpuPressureDeferred(wasDeferred bool, loadPerCore, threshold, exitRatio float64) bool {
	if threshold <= 0 {
		return false
	}
	if wasDeferred {
		if exitRatio <= 0 || exitRatio > 1 {
			exitRatio = 1
		}
		return loadPerCore >= threshold*exitRatio
	}
	return loadPerCore > threshold
}

// checkPressure evaluates system load and session concurrency to decide
// whether spawning a new agent session is safe. It checks:
//
//  1. CPU pressure: 5-minute load average vs threshold (per-core), with
//     enter/exit hysteresis. Skipped for the Refinery and Witness.
//  2. Memory pressure: available memory vs minimum threshold.
//  3. Session concurrency: active tmux sessions vs maximum cap.
//
// Infrastructure agents (deacon, witness, mayor) should NOT be gated by
// pressure—they are the monitoring/recovery layer. Only gate:
//   - Polecats (dispatchQueuedWork, crash restarts)
//   - Refineries (memory/session tiers only)
//   - Dogs
//
// Called only from the heartbeat goroutine, so the hysteresis flag needs no
// synchronisation.
func (d *Daemon) checkPressure(kind string) PressureResult {
	cfg := d.loadOperationalConfig().GetDaemonConfig()

	cpuThreshold := cfg.PressureCPUThresholdV()
	memThreshold := cfg.PressureMemThresholdGBV()
	maxSessions := cfg.PressureMaxSessionsV()

	if cpuExemptKinds[kind] {
		cpuThreshold = 0
	}

	// All checks disabled (default) — skip entirely, no subprocess calls.
	if cpuThreshold <= 0 && memThreshold <= 0 && maxSessions <= 0 {
		return PressureResult{OK: true}
	}

	var result PressureResult
	result.OK = true

	// Tier 1: CPU pressure (load average per core, with hysteresis)
	if cpuThreshold > 0 {
		result.LoadAvg5 = loadAverage5()
		numCPU := float64(runtime.NumCPU())
		loadPerCore := result.LoadAvg5 / numCPU
		exitRatio := cfg.PressureCPUExitRatioV()
		d.pressureCPUDeferred = cpuPressureDeferred(d.pressureCPUDeferred, loadPerCore, cpuThreshold, exitRatio)
		if d.pressureCPUDeferred {
			result.OK = false
			result.Reason = fmt.Sprintf("cpu pressure: load5/core %.2f vs threshold %.2f (exit %.2f, load5=%.1f, cores=%d)",
				loadPerCore, cpuThreshold, cpuThreshold*exitRatio, result.LoadAvg5, int(numCPU))
			return result
		}
	}

	// Tier 1: Memory pressure
	if memThreshold > 0 {
		result.MemAvailableGB = availableMemoryGB()
		if result.MemAvailableGB > 0 && result.MemAvailableGB < memThreshold {
			result.OK = false
			result.Reason = fmt.Sprintf("memory pressure: %.1fGB available, minimum %.1fGB", result.MemAvailableGB, memThreshold)
			return result
		}
	}

	// Tier 2: Session concurrency cap
	if maxSessions > 0 {
		result.ActiveSessions = d.countAgentSessions()
		if result.ActiveSessions >= maxSessions {
			result.OK = false
			result.Reason = fmt.Sprintf("session cap: %d active sessions, max %d", result.ActiveSessions, maxSessions)
			return result
		}
	}

	return result
}

// countAgentSessions counts active tmux sessions that belong to Gas Town agents.
// Uses the town's tmux socket so it only counts sessions for this town.
func (d *Daemon) countAgentSessions() int {
	t := tmux.NewTmux()
	sessions, err := t.ListSessions()
	if err != nil {
		return 0
	}

	count := 0
	for _, name := range sessions {
		if isAgentSession(name) {
			count++
		}
	}
	return count
}

// isAgentSession returns true if the tmux session name looks like a Gas Town agent.
// Agent sessions use prefixed names (e.g., "hq-mayor", "rig-witness", "rig-polecat-foo").
func isAgentSession(name string) bool {
	// Agent sessions contain role markers
	for _, marker := range []string{
		constants.RoleMayor,
		constants.RoleWitness,
		constants.RoleRefinery,
		constants.RolePolecat,
		constants.RoleDeacon,
		constants.RoleCrew,
		"boot",
		"dog",
	} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

// loadAverage5 returns the 5-minute load average.
// Falls back to 0 if unavailable (effectively disabling the check).
//
// Load-5 rather than load-1: host load here is dominated by CI trains that run
// for ~20 minutes, and the 1-minute average swings far enough inside one train
// to flip the gate repeatedly. Load-5 is a free, well-understood smoother.
func loadAverage5() float64 {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		// macOS: use sysctl
		return loadAverage5Sysctl()
	}
	// /proc/loadavg: "<load1> <load5> <load15> ..."
	var load1, load5 float64
	if n, _ := fmt.Sscanf(string(data), "%f %f", &load1, &load5); n < 2 {
		return load1
	}
	return load5
}
