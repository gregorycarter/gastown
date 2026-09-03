package daemon

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/constants"
	"github.com/steveyegge/gastown/internal/events"
	"github.com/steveyegge/gastown/internal/nudge"
	"github.com/steveyegge/gastown/internal/session"
)

// Patrol parking detection.
//
// A patrol role (deacon, witness, refinery) is "parked" when its tmux session
// and agent process are alive and the agent is sitting at its prompt, but the
// patrol loop is not running: the `heartbeat:<unix>` label its await-* step
// maintains has stopped advancing. Nothing in the base daemon reads that label
// (liveness is "process exists"), so a parked patrol stayed parked until a
// human noticed — hours, in the hq-5uqry cases.
//
// The detector is deliberately conservative: the conjunction of "idle at the
// prompt" AND "heartbeat has not advanced" is what distinguishes a parked agent
// from a busy one. A patrol inside a legitimate backoff sleep advertises it
// with a future `backoff-until:` label and is left alone.

const (
	// backoffUntilLabelPrefix marks a scheduled wake-up written by await-*.
	backoffUntilLabelPrefix = "backoff-until:"
	// heartbeatLabelPrefix is the liveness label written by await-*.
	heartbeatLabelPrefix = "heartbeat:"

	// mayorQueueDeliverAfter is how long the Mayor's nudge queue may sit
	// non-empty while the Mayor is idle at its prompt before the daemon
	// delivers it immediately. The Mayor is never auto-restarted.
	mayorQueueDeliverAfter = 10 * time.Minute
)

// parkingAction is what the detector decided to do about one patrol role.
type parkingAction int

const (
	// parkingActionNone means the role is healthy (or unobservable).
	parkingActionNone parkingAction = iota
	// parkingActionNudge means send an immediate un-park nudge.
	parkingActionNudge
	// parkingActionRestart means the previous nudge did not take: kill the
	// session and let the existing manager respawn it.
	parkingActionRestart
)

func (a parkingAction) String() string {
	switch a {
	case parkingActionNudge:
		return "nudge"
	case parkingActionRestart:
		return "restart"
	default:
		return "none"
	}
}

// parkingObservation is everything the decision needs about one patrol role.
// Every field is sampled by the caller so the decision itself stays pure.
type parkingObservation struct {
	SessionExists bool
	AgentAlive    bool
	Idle          bool

	// HeartbeatUnix is the value of the heartbeat: label (0 when absent).
	HeartbeatUnix int64

	// BackoffUntilUnix is the value of the backoff-until: label (0 when absent).
	BackoffUntilUnix int64

	// Now is the evaluation time.
	Now time.Time
}

// heartbeatAge returns how long ago the heartbeat label was written.
// Returns ok=false when the role has no heartbeat label at all.
func (o parkingObservation) heartbeatAge() (time.Duration, bool) {
	if o.HeartbeatUnix <= 0 {
		return 0, false
	}
	return o.Now.Sub(time.Unix(o.HeartbeatUnix, 0)), true
}

// decideParkingAction is the whole parking policy, expressed as a pure function.
//
// prev is the record from the previous heartbeat (nil when the role has never
// been flagged). A role is nudged the first time it looks parked; if the next
// tick finds the same condition AND the heartbeat has not advanced past the
// value seen when the nudge was sent, the session is restarted.
func decideParkingAction(obs parkingObservation, threshold time.Duration, prev *PatrolParkingRecord) parkingAction {
	if threshold <= 0 {
		return parkingActionNone
	}
	if !obs.SessionExists || !obs.AgentAlive || !obs.Idle {
		return parkingActionNone
	}
	// A role sleeping in a declared backoff window is not parked.
	if obs.BackoffUntilUnix > 0 && time.Unix(obs.BackoffUntilUnix, 0).After(obs.Now) {
		return parkingActionNone
	}
	age, ok := obs.heartbeatAge()
	if !ok {
		// No heartbeat label at all: the role has never entered an await step
		// under this scheme. Treat as unobservable rather than parked — the
		// existing session-liveness checks own that case.
		return parkingActionNone
	}
	if age < threshold {
		return parkingActionNone
	}
	if prev != nil && !prev.NudgedAt.IsZero() && obs.HeartbeatUnix <= prev.HeartbeatUnix {
		// Already nudged and the heartbeat has not advanced since.
		return parkingActionRestart
	}
	return parkingActionNudge
}

// parkedNudgeMessage is the text delivered to a parked patrol role.
func parkedNudgeMessage(age time.Duration) string {
	return fmt.Sprintf("PARKED: your patrol loop is not running (heartbeat %dm old). Run gt prime --hook and re-enter your await step.",
		int(age.Minutes()))
}

// patrolRole describes one role the detector watches.
type patrolRole struct {
	Identity  string // "deacon", "<rig>/witness", "<rig>/refinery"
	Session   string
	AgentBead string
	Threshold time.Duration
}

// parkedAfterFor returns the configured parked threshold for a role.
// Defaults: witness 15m, deacon 45m, refinery 60m.
func parkedAfterFor(role string, cfg *config.DaemonThresholds) time.Duration {
	switch role {
	case constants.RoleWitness:
		return cfg.PatrolParkedAfterWitnessD()
	case constants.RoleRefinery:
		return cfg.PatrolParkedAfterRefineryD()
	case constants.RoleDeacon:
		return cfg.PatrolParkedAfterDeaconD()
	}
	return 0
}

// labelValueInt reads an integer-valued label such as "heartbeat:1756900000".
// Returns 0 when the label is absent or unparsable.
func labelValueInt(labels []string, prefix string) int64 {
	var newest int64
	for _, label := range labels {
		if !strings.HasPrefix(label, prefix) {
			continue
		}
		v, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(label, prefix)), 10, 64)
		if err != nil || v <= 0 {
			continue
		}
		if v > newest {
			newest = v
		}
	}
	return newest
}

// patrolRolesToWatch enumerates the deacon plus each operational rig's witness
// and refinery, with their agent bead IDs and per-role thresholds.
func (d *Daemon) patrolRolesToWatch() []patrolRole {
	cfg := d.loadOperationalConfig().GetDaemonConfig()
	var roles []patrolRole

	if d.isPatrolActive("deacon") {
		roles = append(roles, patrolRole{
			Identity:  constants.RoleDeacon,
			Session:   d.getDeaconSessionName(),
			AgentBead: beads.DeaconBeadIDTown(),
			Threshold: parkedAfterFor(constants.RoleDeacon, cfg),
		})
	}

	if d.isPatrolActive("witness") {
		for _, rigName := range d.getPatrolRigs("witness") {
			prefix := config.GetRigPrefix(d.config.TownRoot, rigName)
			roles = append(roles, patrolRole{
				Identity:  rigName + "/" + constants.RoleWitness,
				Session:   session.WitnessSessionName(session.PrefixFor(rigName)),
				AgentBead: beads.WitnessBeadIDWithPrefix(prefix, rigName),
				Threshold: parkedAfterFor(constants.RoleWitness, cfg),
			})
		}
	}

	if d.isPatrolActive("refinery") {
		for _, rigName := range d.getPatrolRigs("refinery") {
			prefix := config.GetRigPrefix(d.config.TownRoot, rigName)
			roles = append(roles, patrolRole{
				Identity:  rigName + "/" + constants.RoleRefinery,
				Session:   session.RefinerySessionName(session.PrefixFor(rigName)),
				AgentBead: beads.RefineryBeadIDWithPrefix(prefix, rigName),
				Threshold: parkedAfterFor(constants.RoleRefinery, cfg),
			})
		}
	}

	return roles
}

// parkingDeps are the side-effecting operations the detector needs. They are
// injected so the loop can be tested with fakes instead of a live tmux server
// and Dolt database.
type parkingDeps struct {
	// AgentLabels returns the labels of every agent bead, keyed by bead ID.
	AgentLabels func() (map[string][]string, error)

	HasSession   func(session string) (bool, error)
	IsAgentAlive func(session string) bool
	IsIdle       func(session string) bool
	Nudge        func(session, message string) error
	Kill         func(session string) error

	// LogEvent records a patrol_parked_restart event.
	LogEvent func(identity string, payload map[string]interface{})

	Now func() time.Time
	Log func(format string, args ...interface{})
}

// runPatrolParking evaluates every watched role and applies the decision.
// It mutates state.PatrolParking and state.PatrolHeartbeats and returns the
// action taken per identity (for tests and logging).
func runPatrolParking(roles []patrolRole, state *State, deps parkingDeps) map[string]parkingAction {
	actions := make(map[string]parkingAction, len(roles))
	if len(roles) == 0 {
		return actions
	}

	labelsByBead, err := deps.AgentLabels()
	if err != nil {
		deps.Log("Parking detector: could not list agent beads: %v", err)
		return actions
	}

	if state.PatrolParking == nil {
		state.PatrolParking = make(map[string]*PatrolParkingRecord)
	}
	if state.PatrolHeartbeats == nil {
		state.PatrolHeartbeats = make(map[string]time.Time)
	}

	now := deps.Now()
	for _, role := range roles {
		labels, ok := labelsByBead[role.AgentBead]
		if !ok {
			continue // No agent bead — nothing to read a heartbeat from.
		}

		obs := parkingObservation{
			HeartbeatUnix:    labelValueInt(labels, heartbeatLabelPrefix),
			BackoffUntilUnix: labelValueInt(labels, backoffUntilLabelPrefix),
			Now:              now,
		}
		if obs.HeartbeatUnix > 0 {
			state.PatrolHeartbeats[role.Identity] = time.Unix(obs.HeartbeatUnix, 0)
		}

		exists, err := deps.HasSession(role.Session)
		if err != nil || !exists {
			// Session gone: the ensure*Running steps own that case. Clear any
			// stale parking record so a fresh session starts from scratch.
			delete(state.PatrolParking, role.Identity)
			actions[role.Identity] = parkingActionNone
			continue
		}
		obs.SessionExists = true
		obs.AgentAlive = deps.IsAgentAlive(role.Session)
		if obs.AgentAlive {
			obs.Idle = deps.IsIdle(role.Session)
		}

		action := decideParkingAction(obs, role.Threshold, state.PatrolParking[role.Identity])
		actions[role.Identity] = action
		age, _ := obs.heartbeatAge()

		switch action {
		case parkingActionNone:
			// Healthy (or busy): forget any earlier parked flag.
			delete(state.PatrolParking, role.Identity)

		case parkingActionNudge:
			deps.Log("PARKED: %s idle at prompt with heartbeat %s old (threshold %s) — nudging",
				role.Identity, age.Round(time.Minute), role.Threshold)
			if err := deps.Nudge(role.Session, parkedNudgeMessage(age)); err != nil {
				deps.Log("Parking detector: nudge to %s failed: %v", role.Identity, err)
				continue
			}
			state.PatrolParking[role.Identity] = &PatrolParkingRecord{
				Identity:      role.Identity,
				Session:       role.Session,
				NudgedAt:      now,
				HeartbeatUnix: obs.HeartbeatUnix,
			}

		case parkingActionRestart:
			deps.Log("PARKED: %s did not resume after nudge (heartbeat %s old) — restarting session %s",
				role.Identity, age.Round(time.Minute), role.Session)
			if err := deps.Kill(role.Session); err != nil {
				deps.Log("Parking detector: killing %s failed: %v", role.Session, err)
				continue
			}
			deps.LogEvent(role.Identity, map[string]interface{}{
				"session":            role.Session,
				"heartbeat_age_secs": int64(age.Seconds()),
				"threshold_secs":     int64(role.Threshold.Seconds()),
			})
			// The manager respawns on the next heartbeat's ensure*Running step.
			delete(state.PatrolParking, role.Identity)
		}
	}

	return actions
}

// checkPatrolParking runs the parking detector for every watched patrol role
// and handles the Mayor's stalled nudge queue. Called from the heartbeat after
// the existing patrol health checks.
func (d *Daemon) checkPatrolParking(state *State) {
	roles := d.patrolRolesToWatch()
	if len(roles) > 0 {
		runPatrolParking(roles, state, d.parkingDeps())
	}
	d.checkMayorParking(state)
}

// parkingDeps binds the detector to the live tmux server and town beads DB.
func (d *Daemon) parkingDeps() parkingDeps {
	return parkingDeps{
		AgentLabels: func() (map[string][]string, error) {
			townBd := beads.New(beads.GetTownBeadsPath(d.config.TownRoot)).ForAgentBead()
			agentBeads, err := townBd.ListAgentBeads()
			if err != nil {
				return nil, err
			}
			labels := make(map[string][]string, len(agentBeads))
			for id, issue := range agentBeads {
				if issue == nil {
					continue
				}
				labels[id] = issue.Labels
			}
			return labels, nil
		},
		HasSession:   d.tmux.HasSession,
		IsAgentAlive: d.tmux.IsAgentAlive,
		IsIdle:       d.tmux.IsIdle,
		Nudge:        d.tmux.NudgeSession,
		Kill:         d.tmux.KillSessionWithProcesses,
		LogEvent: func(identity string, payload map[string]interface{}) {
			_ = events.LogAudit(events.TypePatrolParkedRestart, identity, payload)
		},
		Now: time.Now,
		Log: d.logger.Printf,
	}
}

// checkMayorParking handles the Mayor, which has no await loop and is never
// auto-restarted. The only failure it can recover is a queued nudge that never
// drained: if the Mayor is idle at its prompt with a non-empty nudge queue for
// more than mayorQueueDeliverAfter, deliver the queue immediately.
func (d *Daemon) checkMayorParking(state *State) {
	mayorSession := session.MayorSessionName()

	exists, err := d.tmux.HasSession(mayorSession)
	if err != nil || !exists {
		state.MayorNudgeQueueSince = time.Time{}
		return
	}

	// Record the Mayor's heartbeat (if it keeps one) for `gt daemon status`.
	townBd := beads.New(beads.GetTownBeadsPath(d.config.TownRoot)).ForAgentBead()
	if issue, _, err := townBd.GetAgentBead(beads.MayorBeadIDTown()); err == nil && issue != nil {
		if hb := labelValueInt(issue.Labels, heartbeatLabelPrefix); hb > 0 {
			if state.PatrolHeartbeats == nil {
				state.PatrolHeartbeats = make(map[string]time.Time)
			}
			state.PatrolHeartbeats[constants.RoleMayor] = time.Unix(hb, 0)
		}
	}

	pending := nudge.QueueLen(d.config.TownRoot, mayorSession)
	if pending == 0 {
		state.MayorNudgeQueueSince = time.Time{}
		return
	}
	if state.MayorNudgeQueueSince.IsZero() {
		state.MayorNudgeQueueSince = time.Now()
		return
	}
	if time.Since(state.MayorNudgeQueueSince) < mayorQueueDeliverAfter {
		return
	}
	if !d.tmux.IsAgentAlive(mayorSession) || !d.tmux.IsIdle(mayorSession) {
		return
	}

	drained, err := nudge.Drain(d.config.TownRoot, mayorSession)
	if err != nil || len(drained) == 0 {
		return
	}
	formatted := nudge.FormatForInjectionForSession(drained, mayorSession)
	d.logger.Printf("Mayor nudge queue stalled for %s with %d message(s) — delivering immediately",
		time.Since(state.MayorNudgeQueueSince).Round(time.Minute), len(drained))
	if err := d.tmux.NudgeSession(mayorSession, formatted); err != nil {
		d.logger.Printf("Parking detector: delivering Mayor nudge queue failed: %v", err)
		return
	}
	state.MayorNudgeQueueSince = time.Time{}
}
