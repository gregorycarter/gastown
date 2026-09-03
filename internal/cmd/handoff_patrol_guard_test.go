package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/config"
)

func TestShouldSkipPatrolHandoff(t *testing.T) {
	const limitMB = 1024
	const maxAge = 8 * time.Hour

	tests := []struct {
		name   string
		sample patrolHandoffSample
		want   bool
	}{
		{"small rss, young session", patrolHandoffSample{RSSMB: 300, Age: 20 * time.Minute}, true},
		{"just under both ceilings", patrolHandoffSample{RSSMB: 1023, Age: maxAge - time.Minute}, true},
		{"rss over ceiling", patrolHandoffSample{RSSMB: 2048, Age: 20 * time.Minute}, false},
		{"rss exactly at ceiling", patrolHandoffSample{RSSMB: limitMB, Age: time.Minute}, false},
		{"session too old", patrolHandoffSample{RSSMB: 100, Age: 9 * time.Hour}, false},
		{"age exactly at ceiling", patrolHandoffSample{RSSMB: 100, Age: maxAge}, false},
		{"both over", patrolHandoffSample{RSSMB: 4096, Age: 12 * time.Hour}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldSkipPatrolHandoff(tc.sample, limitMB, maxAge); got != tc.want {
				t.Errorf("shouldSkipPatrolHandoff(%+v) = %v, want %v", tc.sample, got, tc.want)
			}
		})
	}
}

func TestShouldSkipPatrolHandoffDisabledThresholds(t *testing.T) {
	sample := patrolHandoffSample{RSSMB: 1, Age: time.Second}
	if shouldSkipPatrolHandoff(sample, 0, 8*time.Hour) {
		t.Error("rssLimitMB=0 must disable the guard")
	}
	if shouldSkipPatrolHandoff(sample, 1024, 0) {
		t.Error("maxAge=0 must disable the guard")
	}
}

func TestFormatPatrolHandoffSkip(t *testing.T) {
	msg := formatPatrolHandoffSkip(patrolHandoffSample{RSSMB: 412, Age: 90 * time.Minute})
	for _, want := range []string{"handoff skipped: context OK", "rss 412MB", "age 1.5h", "re-enter your patrol loop"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
}

func TestHandoffCooldownForRole(t *testing.T) {
	empty := &config.OperationalConfig{}

	for _, role := range []string{"deacon", "witness", "refinery"} {
		if got := handoffCooldownForRole(role, empty); got != config.DefaultPatrolHandoffMinInterval {
			t.Errorf("%s cooldown = %v, want %v", role, got, config.DefaultPatrolHandoffMinInterval)
		}
	}
	if got := handoffCooldownForRole("polecat", empty); got != config.DefaultMinHandoffCooldown {
		t.Errorf("polecat cooldown = %v, want %v", got, config.DefaultMinHandoffCooldown)
	}

	custom := &config.OperationalConfig{
		Daemon:  &config.DaemonThresholds{PatrolHandoffMinInterval: "45m"},
		Session: &config.SessionThresholds{MinHandoffCooldown: "10s"},
	}
	if got := handoffCooldownForRole("witness", custom); got != 45*time.Minute {
		t.Errorf("configured patrol cooldown = %v, want 45m", got)
	}
	if got := handoffCooldownForRole("crew", custom); got != 10*time.Second {
		t.Errorf("configured session cooldown = %v, want 10s", got)
	}
}

func TestMaybeSkipPatrolHandoffForceAndNonPatrol(t *testing.T) {
	t.Setenv("GT_ROLE", "witness")
	if maybeSkipPatrolHandoff(true) {
		t.Error("--force must never skip the handoff")
	}
	t.Setenv("GT_ROLE", "mayor")
	if maybeSkipPatrolHandoff(false) {
		t.Error("non-patrol roles must never be skipped")
	}
	t.Setenv("GT_ROLE", "")
	if maybeSkipPatrolHandoff(false) {
		t.Error("unset GT_ROLE must never be skipped")
	}
}
