package cmd

import (
	"strings"

	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/session"
)

func canonicalAgentAssignee(agentID string) string {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return ""
	}

	trimmed := strings.TrimSuffix(agentID, "/")
	if isTownLevelRole(trimmed) {
		return trimmed + "/"
	}

	return agentID
}

// hookAssigneeCandidates returns equivalent assignee identities that may refer
// to the same agent. Town-level roles historically appear both with and
// without a trailing slash (for example "deacon" vs "deacon/"), so hook
// lookups must tolerate both forms.
func hookAssigneeCandidates(agentID string) []string {
	agentID = canonicalAgentAssignee(agentID)
	if agentID == "" {
		return nil
	}

	var candidates []string
	seen := make(map[string]struct{})
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		candidates = append(candidates, id)
	}

	add(agentID)

	trimmed := strings.TrimSuffix(agentID, "/")
	add(trimmed)
	if isTownLevelRole(trimmed) {
		add(trimmed + "/")
	}

	if identity, err := session.ParseAddress(agentID); err == nil {
		add(identity.Address())
		if isTownLevelRole(identity.Address()) {
			add(identity.Address() + "/")
		}
	}

	// Mail uses shorthand polecat identities like "rig/name"; tolerate those
	// when querying hook state by assignee.
	parts := strings.Split(trimmed, "/")
	if len(parts) == 3 && (parts[1] == "crew" || parts[1] == "polecats") {
		add(parts[0] + "/" + parts[2])
	}

	return candidates
}

func listAssignedBeadsByAliases(b *beads.Beads, status, agentID string) ([]*beads.Issue, error) {
	var lastErr error
	for _, candidate := range hookAssigneeCandidates(agentID) {
		found, err := b.List(beads.ListOptions{
			Status:   status,
			Assignee: candidate,
			Priority: -1,
		})
		if err != nil {
			if lastErr == nil {
				lastErr = err
			}
			continue
		}
		if len(found) > 0 {
			return found, nil
		}
	}
	return nil, lastErr
}
