package cmd

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/constants"
	"github.com/steveyegge/gastown/internal/mail"
)

// heartbeatLabelPrefix is the label written on an agent bead by the await-*
// molecule steps (molecule_await_signal.go / molecule_await_event.go) to prove
// the patrol loop is alive: "heartbeat:<unix seconds>".
const heartbeatLabelPrefix = "heartbeat:"

// parseHeartbeatLabels extracts the newest heartbeat timestamp from a set of
// agent-bead labels. Returns ok=false when no heartbeat label is present.
func parseHeartbeatLabels(labels []string) (time.Time, bool) {
	var newest time.Time
	found := false
	for _, label := range labels {
		if !strings.HasPrefix(label, heartbeatLabelPrefix) {
			continue
		}
		secs, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(label, heartbeatLabelPrefix)), 10, 64)
		if err != nil || secs <= 0 {
			continue
		}
		ts := time.Unix(secs, 0)
		if ts.After(newest) {
			newest = ts
			found = true
		}
	}
	return newest, found
}

// nudgeTargetToIdentity normalizes a nudge target address into the identity
// form buildAgentBeadID understands ("mayor", "deacon", "<rig>/witness",
// "<rig>/refinery", "<rig>/<polecat>", "<rig>/crew/<name>").
//
// Bare "witness"/"refinery" shortcuts are resolved against the caller's rig.
// Returns "" when the target has no agent bead (channels, dogs, raw session
// names).
func nudgeTargetToIdentity(target string) string {
	target = strings.TrimSuffix(target, "/")
	if target == "" || strings.HasPrefix(target, "channel:") {
		return ""
	}
	if _, isDog := mail.DogAddressName(target); isDog {
		return ""
	}

	switch target {
	case constants.RoleMayor, constants.RoleDeacon:
		return target
	case constants.RoleWitness, constants.RoleRefinery:
		roleInfo, err := GetRole()
		if err != nil || roleInfo.Rig == "" {
			return ""
		}
		return roleInfo.Rig + "/" + target
	}

	if !strings.Contains(target, "/") {
		// Raw session name — no reliable identity mapping.
		return ""
	}
	return target
}

// agentHeartbeatAge returns how long ago the target agent bead's heartbeat
// label was written. The agent bead is read through the town-routed path
// (ForAgentBead), because agent beads live in the town DB even though their
// IDs carry a rig prefix.
func agentHeartbeatAge(townRoot, target string) (age time.Duration, beadID string, err error) {
	identity := nudgeTargetToIdentity(target)
	if identity == "" {
		return 0, "", fmt.Errorf("no agent bead for target %q", target)
	}

	beadID = buildAgentBeadID(identity, RoleUnknown, townRoot)
	if beadID == "" {
		return 0, "", fmt.Errorf("could not resolve agent bead for %q", identity)
	}

	bd := beads.New(beads.GetTownBeadsPath(townRoot)).ForAgentBead()
	issue, _, err := bd.GetAgentBead(beadID)
	if err != nil {
		return 0, beadID, fmt.Errorf("reading agent bead %s: %w", beadID, err)
	}
	if issue == nil {
		return 0, beadID, fmt.Errorf("agent bead %s not found", beadID)
	}

	ts, ok := parseHeartbeatLabels(issue.Labels)
	if !ok {
		return 0, beadID, fmt.Errorf("agent bead %s has no %s label", beadID, heartbeatLabelPrefix)
	}
	return time.Since(ts), beadID, nil
}

// ifStaleDecision is the outcome of the --if-stale gate.
type ifStaleDecision struct {
	Skip   bool
	Reason string
}

// evaluateIfStale decides whether a nudge should be suppressed because the
// target's heartbeat is fresher than the threshold.
//
// A missing or unreadable heartbeat never suppresses the nudge: the gate exists
// to avoid pestering a demonstrably live patrol loop, not to swallow nudges
// when the evidence is absent.
func evaluateIfStale(threshold, age time.Duration, beadID string, lookupErr error) ifStaleDecision {
	if lookupErr != nil {
		return ifStaleDecision{Skip: false, Reason: fmt.Sprintf("heartbeat unavailable (%v) — sending anyway", lookupErr)}
	}
	if age < threshold {
		return ifStaleDecision{
			Skip:   true,
			Reason: fmt.Sprintf("heartbeat on %s is %s old (< %s) — nudge skipped", beadID, age.Round(time.Second), threshold),
		}
	}
	return ifStaleDecision{
		Skip:   false,
		Reason: fmt.Sprintf("heartbeat on %s is %s old (>= %s) — sending", beadID, age.Round(time.Second), threshold),
	}
}
