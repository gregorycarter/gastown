package daemon

import (
	"io"
	"log"
	"testing"
	"time"
)

func TestGetPatrolInterval(t *testing.T) {
	cfg := &DaemonPatrolConfig{Patrols: &PatrolsConfig{
		Witness:  &PatrolConfig{Enabled: true, Interval: "5m"},
		Deacon:   &PatrolConfig{Enabled: true},
		Refinery: &PatrolConfig{Enabled: true, Interval: "not-a-duration"},
		Handler:  &PatrolConfig{Enabled: true, Interval: "0s"},
	}}

	if got := GetPatrolInterval(cfg, "witness"); got != 5*time.Minute {
		t.Errorf("witness interval = %v, want 5m", got)
	}
	if got := GetPatrolInterval(cfg, "deacon"); got != 0 {
		t.Errorf("unset interval = %v, want 0", got)
	}
	if got := GetPatrolInterval(cfg, "refinery"); got != 0 {
		t.Errorf("unparsable interval = %v, want 0", got)
	}
	if got := GetPatrolInterval(cfg, "handler"); got != 0 {
		t.Errorf("zero interval = %v, want 0", got)
	}
	if got := GetPatrolInterval(nil, "witness"); got != 0 {
		t.Errorf("nil config = %v, want 0", got)
	}
	if got := GetPatrolInterval(cfg, "doctor_dog"); got != 0 {
		t.Errorf("patrol without a PatrolConfig = %v, want 0", got)
	}
}

func newIntervalTestDaemon(cfg *DaemonPatrolConfig) *Daemon {
	return &Daemon{
		patrolConfig: cfg,
		logger:       log.New(io.Discard, "", 0),
	}
}

func TestPatrolDueHonoursInterval(t *testing.T) {
	d := newIntervalTestDaemon(&DaemonPatrolConfig{Patrols: &PatrolsConfig{
		Witness: &PatrolConfig{Enabled: true, Interval: "5m"},
	}})

	if !d.patrolDue("witness") {
		t.Fatal("first check should always be due")
	}
	if d.patrolDue("witness") {
		t.Error("second check within the interval should not be due")
	}

	// Pretend the last run was long ago.
	d.patrolLastRun["witness"] = time.Now().Add(-10 * time.Minute)
	if !d.patrolDue("witness") {
		t.Error("check should be due once the interval has elapsed")
	}
}

func TestPatrolDueWithoutIntervalRunsEveryTick(t *testing.T) {
	d := newIntervalTestDaemon(&DaemonPatrolConfig{Patrols: &PatrolsConfig{
		Deacon: &PatrolConfig{Enabled: true},
	}})
	for i := 0; i < 3; i++ {
		if !d.patrolDue("deacon") {
			t.Fatalf("tick %d: an unset interval must run every heartbeat", i)
		}
	}
}

func TestPatrolDueIsPerPatrol(t *testing.T) {
	d := newIntervalTestDaemon(&DaemonPatrolConfig{Patrols: &PatrolsConfig{
		Witness:  &PatrolConfig{Enabled: true, Interval: "5m"},
		Refinery: &PatrolConfig{Enabled: true, Interval: "5m"},
	}})
	if !d.patrolDue("witness") || !d.patrolDue("refinery") {
		t.Fatal("both patrols should be due on the first tick")
	}
	if d.patrolDue("witness") || d.patrolDue("refinery") {
		t.Error("neither patrol should be due again within the interval")
	}
}
