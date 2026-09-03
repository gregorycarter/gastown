package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/steveyegge/gastown/internal/config"
	"github.com/steveyegge/gastown/internal/tmux"
	"github.com/steveyegge/gastown/internal/workspace"
)

// patrolHandoffSample holds the observable state a patrol handoff decision uses.
type patrolHandoffSample struct {
	RSSMB int
	Age   time.Duration
}

// shouldSkipPatrolHandoff decides whether a patrol role's handoff is
// unnecessary. A patrol agent that still has a small resident set and a young
// session has plenty of context left; restarting it costs a full formula
// re-render, a handoff mail bead and several Dolt commits for nothing.
//
// Both conditions must hold: RSS under rssLimitMB AND session age under maxAge.
func shouldSkipPatrolHandoff(sample patrolHandoffSample, rssLimitMB int, maxAge time.Duration) bool {
	if rssLimitMB <= 0 || maxAge <= 0 {
		return false
	}
	return sample.RSSMB < rssLimitMB && sample.Age < maxAge
}

// formatPatrolHandoffSkip renders the message printed when a handoff is skipped.
func formatPatrolHandoffSkip(sample patrolHandoffSample) string {
	return fmt.Sprintf("handoff skipped: context OK (rss %dMB, age %.1fh) — re-enter your patrol loop",
		sample.RSSMB, sample.Age.Hours())
}

// currentRoleName returns the simple role name of the calling session
// ("deacon", "witness", "refinery", "mayor", …) from GT_ROLE. Empty when unset.
func currentRoleName() string {
	role := os.Getenv("GT_ROLE")
	if role == "" {
		return ""
	}
	parsed, _, _ := parseRoleString(role)
	return string(parsed)
}

// patrolHandoffThresholds reads the RSS and age ceilings from town settings.
func patrolHandoffThresholds() (rssLimitMB int, maxAge time.Duration) {
	townRoot, _ := workspace.FindFromCwd()
	cfg := config.LoadOperationalConfig(townRoot).GetDaemonConfig()
	return cfg.PatrolHandoffRSSMBV(), cfg.PatrolHandoffMaxAgeD()
}

// samplePatrolHandoffState measures the caller's agent RSS and session age.
// Returns an error when either cannot be observed — callers must then proceed
// with the handoff rather than guessing.
func samplePatrolHandoffState(session string) (patrolHandoffSample, error) {
	if session == "" {
		return patrolHandoffSample{}, fmt.Errorf("no tmux session")
	}
	t := tmux.NewTmuxWithSocket(tmux.SocketFromEnv())

	rssMB, err := t.AgentRSSMB(session)
	if err != nil {
		return patrolHandoffSample{}, err
	}

	created, err := t.GetSessionCreatedUnix(session)
	if err != nil {
		return patrolHandoffSample{}, err
	}
	if created <= 0 {
		return patrolHandoffSample{}, fmt.Errorf("session %s has no creation time", session)
	}

	return patrolHandoffSample{
		RSSMB: rssMB,
		Age:   time.Since(time.Unix(created, 0)),
	}, nil
}

// maybeSkipPatrolHandoff implements the patrol handoff guard for runHandoff.
// It returns true when the caller should exit 0 without handing off.
//
// The guard applies only when the caller is a patrol role (deacon, witness,
// refinery) and --force was not given. Any sampling failure falls through to a
// normal handoff: a guard that cannot observe the agent must not block it.
func maybeSkipPatrolHandoff(force bool) bool {
	if force {
		return false
	}
	if !isPatrolRole(currentRoleName()) {
		return false
	}

	session, err := getCurrentTmuxSession()
	if err != nil || session == "" {
		return false
	}

	sample, err := samplePatrolHandoffState(session)
	if err != nil {
		return false
	}

	rssLimitMB, maxAge := patrolHandoffThresholds()
	if !shouldSkipPatrolHandoff(sample, rssLimitMB, maxAge) {
		return false
	}

	fmt.Println(formatPatrolHandoffSkip(sample))
	return true
}
