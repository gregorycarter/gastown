package daemon

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/config"
)

func TestLabelValueInt(t *testing.T) {
	labels := []string{"gt:agent", "idle:4", "heartbeat:1756900000", "backoff-until:1756900600"}

	if got := labelValueInt(labels, heartbeatLabelPrefix); got != 1756900000 {
		t.Errorf("heartbeat = %d, want 1756900000", got)
	}
	if got := labelValueInt(labels, backoffUntilLabelPrefix); got != 1756900600 {
		t.Errorf("backoff-until = %d, want 1756900600", got)
	}
	if got := labelValueInt(labels, "missing:"); got != 0 {
		t.Errorf("missing label = %d, want 0", got)
	}
	if got := labelValueInt([]string{"heartbeat:garbage"}, heartbeatLabelPrefix); got != 0 {
		t.Errorf("unparsable label = %d, want 0", got)
	}
	if got := labelValueInt([]string{"heartbeat:10", "heartbeat:99"}, heartbeatLabelPrefix); got != 99 {
		t.Errorf("duplicate labels should yield the newest, got %d", got)
	}
}

func TestDecideParkingAction(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	const threshold = 15 * time.Minute
	parked := func(hbAgo time.Duration) parkingObservation {
		return parkingObservation{
			SessionExists: true,
			AgentAlive:    true,
			Idle:          true,
			HeartbeatUnix: now.Add(-hbAgo).Unix(),
			Now:           now,
		}
	}

	tests := []struct {
		name string
		obs  parkingObservation
		prev *PatrolParkingRecord
		want parkingAction
	}{
		{"healthy: fresh heartbeat", parked(2 * time.Minute), nil, parkingActionNone},
		{"parked: stale heartbeat, first sighting", parked(30 * time.Minute), nil, parkingActionNudge},
		{"busy agent is never parked", func() parkingObservation {
			o := parked(30 * time.Minute)
			o.Idle = false
			return o
		}(), nil, parkingActionNone},
		{"dead agent is not parked", func() parkingObservation {
			o := parked(30 * time.Minute)
			o.AgentAlive = false
			return o
		}(), nil, parkingActionNone},
		{"no session is not parked", func() parkingObservation {
			o := parked(30 * time.Minute)
			o.SessionExists = false
			return o
		}(), nil, parkingActionNone},
		{"no heartbeat label is unobservable", parkingObservation{
			SessionExists: true, AgentAlive: true, Idle: true, Now: now,
		}, nil, parkingActionNone},
		{"future backoff-until means sleeping, not parked", func() parkingObservation {
			o := parked(30 * time.Minute)
			o.BackoffUntilUnix = now.Add(10 * time.Minute).Unix()
			return o
		}(), nil, parkingActionNone},
		{"expired backoff-until does not protect", func() parkingObservation {
			o := parked(30 * time.Minute)
			o.BackoffUntilUnix = now.Add(-5 * time.Minute).Unix()
			return o
		}(), nil, parkingActionNudge},
		{"age exactly at threshold is parked", parked(threshold), nil, parkingActionNudge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := decideParkingAction(tc.obs, threshold, tc.prev); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecideParkingActionEscalatesToRestart(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	const threshold = 15 * time.Minute
	hb := now.Add(-40 * time.Minute).Unix()

	obs := parkingObservation{
		SessionExists: true, AgentAlive: true, Idle: true,
		HeartbeatUnix: hb, Now: now,
	}
	prev := &PatrolParkingRecord{NudgedAt: now.Add(-3 * time.Minute), HeartbeatUnix: hb}

	if got := decideParkingAction(obs, threshold, prev); got != parkingActionRestart {
		t.Errorf("unchanged heartbeat after nudge should restart, got %v", got)
	}

	// The nudge worked: the heartbeat advanced but is still older than the
	// threshold — the agent is running again, so back off rather than restart.
	advanced := obs
	advanced.HeartbeatUnix = now.Add(-20 * time.Minute).Unix()
	if got := decideParkingAction(advanced, threshold, prev); got != parkingActionNudge {
		t.Errorf("advanced heartbeat should re-nudge, not restart, got %v", got)
	}
}

func TestDecideParkingActionDisabledThreshold(t *testing.T) {
	now := time.Now()
	obs := parkingObservation{
		SessionExists: true, AgentAlive: true, Idle: true,
		HeartbeatUnix: now.Add(-24 * time.Hour).Unix(), Now: now,
	}
	if got := decideParkingAction(obs, 0, nil); got != parkingActionNone {
		t.Errorf("zero threshold must disable the detector, got %v", got)
	}
}

func TestParkedNudgeMessage(t *testing.T) {
	msg := parkedNudgeMessage(42 * time.Minute)
	for _, want := range []string{"PARKED:", "heartbeat 42m old", "gt prime --hook", "re-enter your await step"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}

func TestParkedAfterForDefaults(t *testing.T) {
	cfg := &config.DaemonThresholds{}
	if got := parkedAfterFor("witness", cfg); got != config.DefaultPatrolParkedAfterWitness {
		t.Errorf("witness = %v, want %v", got, config.DefaultPatrolParkedAfterWitness)
	}
	if got := parkedAfterFor("deacon", cfg); got != config.DefaultPatrolParkedAfterDeacon {
		t.Errorf("deacon = %v, want %v", got, config.DefaultPatrolParkedAfterDeacon)
	}
	if got := parkedAfterFor("refinery", cfg); got != config.DefaultPatrolParkedAfterRefinery {
		t.Errorf("refinery = %v, want %v", got, config.DefaultPatrolParkedAfterRefinery)
	}
	if got := parkedAfterFor("polecat", cfg); got != 0 {
		t.Errorf("non-patrol role = %v, want 0", got)
	}

	overridden := &config.DaemonThresholds{PatrolParkedAfter: map[string]string{
		"witness":  "5m",
		"refinery": "0s",
	}}
	if got := parkedAfterFor("witness", overridden); got != 5*time.Minute {
		t.Errorf("configured witness = %v, want 5m", got)
	}
	if got := parkedAfterFor("refinery", overridden); got != 0 {
		t.Errorf("refinery disabled by 0s should be 0, got %v", got)
	}
	if got := parkedAfterFor("deacon", overridden); got != config.DefaultPatrolParkedAfterDeacon {
		t.Errorf("unset deacon should fall back to default, got %v", got)
	}
}

// fakeParking builds parkingDeps backed by in-memory maps.
type fakeParking struct {
	labels    map[string][]string
	labelsErr error
	sessions  map[string]bool
	alive     map[string]bool
	idle      map[string]bool
	nudged    []string
	killed    []string
	events    []string
	nudgeErr  error
	now       time.Time
	logs      []string
}

func (f *fakeParking) deps() parkingDeps {
	return parkingDeps{
		AgentLabels: func() (map[string][]string, error) {
			if f.labelsErr != nil {
				return nil, f.labelsErr
			}
			return f.labels, nil
		},
		HasSession:   func(s string) (bool, error) { return f.sessions[s], nil },
		IsAgentAlive: func(s string) bool { return f.alive[s] },
		IsIdle:       func(s string) bool { return f.idle[s] },
		Nudge: func(s, msg string) error {
			if f.nudgeErr != nil {
				return f.nudgeErr
			}
			f.nudged = append(f.nudged, s)
			return nil
		},
		Kill: func(s string) error {
			f.killed = append(f.killed, s)
			return nil
		},
		LogEvent: func(identity string, _ map[string]interface{}) {
			f.events = append(f.events, identity)
		},
		Now: func() time.Time { return f.now },
		Log: func(format string, args ...interface{}) {
			f.logs = append(f.logs, fmt.Sprintf(format, args...))
		},
	}
}

func newFakeParking(now time.Time, hbAgo time.Duration) *fakeParking {
	return &fakeParking{
		labels: map[string][]string{
			"gt-bt-witness": {"gt:agent", fmt.Sprintf("heartbeat:%d", now.Add(-hbAgo).Unix())},
		},
		sessions: map[string]bool{"bt-witness": true},
		alive:    map[string]bool{"bt-witness": true},
		idle:     map[string]bool{"bt-witness": true},
		now:      now,
	}
}

func witnessRole() patrolRole {
	return patrolRole{
		Identity:  "bridge_town_core/witness",
		Session:   "bt-witness",
		AgentBead: "gt-bt-witness",
		Threshold: 15 * time.Minute,
	}
}

func TestRunPatrolParkingNudgesThenRestarts(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	f := newFakeParking(now, 40*time.Minute)
	state := &State{}
	role := witnessRole()

	// Tick 1: parked → nudge, record kept.
	actions := runPatrolParking([]patrolRole{role}, state, f.deps())
	if actions[role.Identity] != parkingActionNudge {
		t.Fatalf("tick 1 action = %v, want nudge", actions[role.Identity])
	}
	if len(f.nudged) != 1 || f.nudged[0] != "bt-witness" {
		t.Fatalf("expected one nudge to bt-witness, got %v", f.nudged)
	}
	rec := state.PatrolParking[role.Identity]
	if rec == nil || rec.NudgedAt != now {
		t.Fatalf("parked_nudged_at not recorded: %+v", rec)
	}
	if hb, ok := state.PatrolHeartbeats[role.Identity]; !ok || !hb.Equal(now.Add(-40*time.Minute)) {
		t.Errorf("heartbeat not recorded in state: %v", hb)
	}

	// Tick 2: same stale heartbeat → restart + event.
	f.now = now.Add(3 * time.Minute)
	actions = runPatrolParking([]patrolRole{role}, state, f.deps())
	if actions[role.Identity] != parkingActionRestart {
		t.Fatalf("tick 2 action = %v, want restart", actions[role.Identity])
	}
	if len(f.killed) != 1 || f.killed[0] != "bt-witness" {
		t.Fatalf("expected session kill, got %v", f.killed)
	}
	if len(f.events) != 1 || f.events[0] != role.Identity {
		t.Fatalf("expected patrol_parked_restart event, got %v", f.events)
	}
	if _, still := state.PatrolParking[role.Identity]; still {
		t.Error("parking record should be cleared after restart")
	}
}

func TestRunPatrolParkingClearsRecordWhenHeartbeatResumes(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	f := newFakeParking(now, 40*time.Minute)
	state := &State{}
	role := witnessRole()

	runPatrolParking([]patrolRole{role}, state, f.deps())
	if state.PatrolParking[role.Identity] == nil {
		t.Fatal("expected a parking record after the first nudge")
	}

	// The agent resumed: fresh heartbeat.
	f.now = now.Add(3 * time.Minute)
	f.labels["gt-bt-witness"] = []string{"gt:agent", fmt.Sprintf("heartbeat:%d", f.now.Unix())}
	actions := runPatrolParking([]patrolRole{role}, state, f.deps())

	if actions[role.Identity] != parkingActionNone {
		t.Errorf("resumed patrol action = %v, want none", actions[role.Identity])
	}
	if _, still := state.PatrolParking[role.Identity]; still {
		t.Error("parking record should be cleared once the loop resumes")
	}
	if len(f.killed) != 0 {
		t.Errorf("a resumed patrol must not be restarted, got %v", f.killed)
	}
}

func TestRunPatrolParkingSkipsMissingSessionAndBead(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	role := witnessRole()

	// Session gone.
	f := newFakeParking(now, 40*time.Minute)
	f.sessions["bt-witness"] = false
	state := &State{PatrolParking: map[string]*PatrolParkingRecord{role.Identity: {Identity: role.Identity}}}
	runPatrolParking([]patrolRole{role}, state, f.deps())
	if len(f.nudged) != 0 || len(f.killed) != 0 {
		t.Error("a missing session must not be nudged or killed")
	}
	if _, still := state.PatrolParking[role.Identity]; still {
		t.Error("parking record should be cleared when the session is gone")
	}

	// Agent bead missing entirely.
	f2 := newFakeParking(now, 40*time.Minute)
	delete(f2.labels, "gt-bt-witness")
	state2 := &State{}
	runPatrolParking([]patrolRole{role}, state2, f2.deps())
	if len(f2.nudged) != 0 || len(f2.killed) != 0 {
		t.Error("a role with no agent bead must be skipped")
	}
}

func TestRunPatrolParkingBeadListFailureIsInert(t *testing.T) {
	f := newFakeParking(time.Now(), 40*time.Minute)
	f.labelsErr = errors.New("dolt unreachable")
	state := &State{}
	actions := runPatrolParking([]patrolRole{witnessRole()}, state, f.deps())
	if len(actions) != 0 || len(f.nudged) != 0 || len(f.killed) != 0 {
		t.Error("a beads failure must not produce nudges or kills")
	}
}

func TestRunPatrolParkingNudgeFailureDoesNotRecord(t *testing.T) {
	now := time.Now()
	f := newFakeParking(now, 40*time.Minute)
	f.nudgeErr = errors.New("tmux gone")
	state := &State{}
	role := witnessRole()

	runPatrolParking([]patrolRole{role}, state, f.deps())
	if _, recorded := state.PatrolParking[role.Identity]; recorded {
		t.Error("a failed nudge must not arm the restart escalation")
	}
}
