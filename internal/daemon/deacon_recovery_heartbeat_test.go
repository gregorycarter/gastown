package daemon

import (
	"github.com/steveyegge/gastown/internal/deacon"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDeaconRecoveryNeedsSustainedHeartbeats(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "daemon"), 0700); err != nil {
		t.Fatal(err)
	}
	rt := NewRestartTracker(root, RestartTrackerConfig{StabilityPeriod: 30 * time.Minute})
	rt.state.Agents["deacon"] = &AgentRestartInfo{LastRestart: now.Add(-48 * time.Hour), CrashLoopSince: now.Add(-48 * time.Hour), RestartCount: 5}
	gap := 20 * time.Minute
	// One post-restart heartbeat, even days after the fault, is insufficient.
	if !rt.RecordHealthyHeartbeat("deacon", now, now, gap) || !rt.IsInCrashLoop("deacon") {
		t.Fatal("startup alone must not clear crash loop")
	}
	// A repeated observation of the same heartbeat cannot advance the window.
	rt.RecordHealthyHeartbeat("deacon", now, now.Add(15*time.Minute), gap)
	if !rt.IsInCrashLoop("deacon") {
		t.Fatal("same heartbeat counted as progress")
	}
	// Persist observation state and recreate the tracker as on daemon restart.
	if err := rt.Save(); err != nil {
		t.Fatal(err)
	}
	rt = NewRestartTracker(root, RestartTrackerConfig{StabilityPeriod: 30 * time.Minute})
	if err := rt.Load(); err != nil {
		t.Fatal(err)
	}
	for _, delta := range []time.Duration{15 * time.Minute, 30 * time.Minute} {
		rt.RecordHealthyHeartbeat("deacon", now.Add(delta), now.Add(delta), gap)
	}
	if rt.IsInCrashLoop("deacon") {
		t.Fatal("sustained real heartbeat progress did not recover")
	}
	if err := rt.Save(); err != nil {
		t.Fatal(err)
	}
	restored := NewRestartTracker(root, RestartTrackerConfig{})
	if err := restored.Load(); err != nil {
		t.Fatal(err)
	}
	if restored.IsInCrashLoop("deacon") || !restored.CanRestart("deacon") {
		t.Fatal("recovery not persisted")
	}
}

func TestDeaconRecoveryRejectsStaleFutureAndDiscontinuousHeartbeats(t *testing.T) {
	now := time.Now().UTC()
	gap := 20 * time.Minute
	for _, tc := range []struct {
		name string
		at   time.Time
	}{
		{"missing", time.Time{}}, {"stale", now.Add(-gap)}, {"future", now.Add(time.Minute)}, {"pre-restart", now.Add(-2 * time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := NewRestartTracker(t.TempDir(), RestartTrackerConfig{})
			rt.state.Agents["deacon"] = &AgentRestartInfo{LastRestart: now.Add(-time.Hour), CrashLoopSince: now.Add(-time.Hour), HealthySince: now.Add(-30 * time.Minute), LastHeartbeat: now.Add(-15 * time.Minute)}
			rt.RecordHealthyHeartbeat("deacon", tc.at, now, gap)
			if !rt.IsInCrashLoop("deacon") || !rt.state.Agents["deacon"].HealthySince.IsZero() {
				t.Fatal("invalid evidence retained recovery window")
			}
		})
	}
	rt := NewRestartTracker(t.TempDir(), RestartTrackerConfig{})
	rt.state.Agents["deacon"] = &AgentRestartInfo{LastRestart: now.Add(-2 * time.Hour), CrashLoopSince: now.Add(-2 * time.Hour), HealthySince: now.Add(-time.Hour), LastHeartbeat: now.Add(-gap)}
	rt.RecordHealthyHeartbeat("deacon", now, now, gap)
	if !rt.IsInCrashLoop("deacon") || !rt.state.Agents["deacon"].HealthySince.Equal(now) {
		t.Fatal("long gap should restart stability observation")
	}
}

func TestDeaconRecoveryRunsBeforeCrashLoopGuard(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "daemon"), 0700); err != nil {
		t.Fatal(err)
	}
	rt := NewRestartTracker(root, RestartTrackerConfig{})
	rt.state.Agents["deacon"] = &AgentRestartInfo{LastRestart: now.Add(-48 * time.Hour), CrashLoopSince: now.Add(-48 * time.Hour), RestartCount: 5, HealthySince: now.Add(-31 * time.Minute), LastHeartbeat: now.Add(-15 * time.Minute)}
	if err := deacon.WriteHeartbeat(root, &deacon.Heartbeat{Timestamp: now, Cycle: 42}); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{config: &Config{TownRoot: root}, logger: log.New(io.Discard, "", 0), restartTracker: rt}
	d.checkDeaconHeartbeat()
	if rt.IsInCrashLoop("deacon") {
		t.Fatal("crash-loop guard hid sustained heartbeat progress")
	}
	restored := NewRestartTracker(root, RestartTrackerConfig{})
	if err := restored.Load(); err != nil {
		t.Fatal(err)
	}
	if restored.IsInCrashLoop("deacon") {
		t.Fatal("daemon did not persist recovered state")
	}
}
