package cmd

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/events"
)

// patrolWakePolicy filters the model wake path, not the durable event stream.
// Periodic timeouts still discover conditions without a matching event.
type patrolWakePolicy struct {
	role     Role
	rig      string
	actor    string
	debounce time.Duration
}

func (p patrolWakePolicy) classify(line string) (wake, urgent bool) {
	if p.role == RoleUnknown || p.role == "" {
		return true, false // Preserve non-patrol subscribers' original behavior.
	}
	var e events.Event
	if json.Unmarshal([]byte(line), &e) != nil {
		return false, false
	}
	actor := strings.Trim(e.Actor, "/")
	if actor == p.actor {
		return false, false
	}
	str := func(key string) string { v, _ := e.Payload[key].(string); return strings.Trim(v, "/") }
	directed := str("to") == p.actor || str("target") == p.actor ||
		str("to") == string(p.role) || str("target") == string(p.role)
	// Mail and nudges explicitly addressed to this role, and failures, never
	// wait for the coalescing interval. Terminal receipts are already in beads.
	if e.Type == events.TypeMail || e.Type == events.TypeNudge {
		if !directed {
			return false, false
		}
		subject := strings.ToUpper(str("subject"))
		for _, prefix := range []string{"ACK", "RE: ACK", "MERGED ", "SYSTEM-MANDATED ACK"} {
			if strings.HasPrefix(subject, prefix) {
				return false, false
			}
		}
		return true, true
	}
	if e.Type == events.TypeEscalationSent {
		return directed, directed
	}
	if e.Type == events.TypeMassDeath {
		return true, true
	}
	inRig := p.rig == "" || str("rig") == p.rig || strings.HasPrefix(actor, p.rig+"/") ||
		strings.HasPrefix(str("agent"), p.rig+"/") || strings.HasPrefix(str("target"), p.rig+"/")
	if !inRig {
		return false, false
	}
	switch e.Type {
	case events.TypeSessionDeath, events.TypeMassDeath, events.TypeKill, events.TypeHalt,
		events.TypeSchedulerDispatchFailed, events.TypeMergeFailed:
		return true, true
	case events.TypeDone, events.TypeMerged:
		return true, false
	case events.TypeSpawn, events.TypeSling, events.TypeSchedulerDispatch:
		return p.role == RoleWitness, false
	default:
		// Ignore session_start, handoffs, patrol reports, bookkeeping, queue
		// stocking and unrelated activity. They used to wake the whole town.
		return false, false
	}
}
