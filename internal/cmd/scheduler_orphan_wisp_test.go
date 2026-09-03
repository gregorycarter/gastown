package cmd

import (
	"testing"
	"time"
)

func TestIsWispID(t *testing.T) {
	for _, id := range []string{"bt-wisp-nod3m", "hq-wisp-dmbqwa"} {
		if !isWispID(id) {
			t.Errorf("%s should be a wisp ID", id)
		}
	}
	for _, id := range []string{"bt-nwzz.6", "hq-cv-abc", "bt-mr-1"} {
		if isWispID(id) {
			t.Errorf("%s should not be a wisp ID", id)
		}
	}
}

func TestOrphanWispCandidates(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	old := now.Add(-2 * time.Hour).Format(time.RFC3339)
	fresh := now.Add(-2 * time.Minute).Format(time.RFC3339)

	dead := func(string) bool { return true }
	alive := func(string) bool { return false }

	t.Run("orphaned wisp is closable", func(t *testing.T) {
		got := orphanWispCandidates(
			[]string{"bt-wisp-a"},
			map[string]beadStatusInfo{"bt-wisp-a": {Status: "open", Assignee: "bt/polecats/nux", CreatedAt: old}},
			"", now, dead)
		if len(got) != 1 || got[0] != "bt-wisp-a" {
			t.Fatalf("got %v, want [bt-wisp-a]", got)
		}
	})

	t.Run("live polecat is left alone", func(t *testing.T) {
		got := orphanWispCandidates(
			[]string{"bt-wisp-a"},
			map[string]beadStatusInfo{"bt-wisp-a": {Status: "open", Assignee: "bt/polecats/nux", CreatedAt: old}},
			"", now, alive)
		if got != nil {
			t.Fatalf("got %v, want nil (polecat session alive)", got)
		}
	})

	t.Run("wisp younger than the grace period is left alone", func(t *testing.T) {
		got := orphanWispCandidates(
			[]string{"bt-wisp-a"},
			map[string]beadStatusInfo{"bt-wisp-a": {Status: "open", Assignee: "bt/polecats/nux", CreatedAt: fresh}},
			"", now, dead)
		if got != nil {
			t.Fatalf("got %v, want nil (within grace period)", got)
		}
	})

	t.Run("a real blocker disqualifies the whole bead", func(t *testing.T) {
		got := orphanWispCandidates(
			[]string{"bt-wisp-a", "bt-real"},
			map[string]beadStatusInfo{
				"bt-wisp-a": {Status: "open", CreatedAt: old},
				"bt-real":   {Status: "open", CreatedAt: old},
			},
			"", now, dead)
		if got != nil {
			t.Fatalf("got %v, want nil (a non-wisp dependency is a real block)", got)
		}
	})

	t.Run("one live owner disqualifies the whole bead", func(t *testing.T) {
		sessionDead := func(assignee string) bool { return assignee != "bt/polecats/live" }
		got := orphanWispCandidates(
			[]string{"bt-wisp-a", "bt-wisp-b"},
			map[string]beadStatusInfo{
				"bt-wisp-a": {Status: "open", Assignee: "bt/polecats/gone", CreatedAt: old},
				"bt-wisp-b": {Status: "open", Assignee: "bt/polecats/live", CreatedAt: old},
			},
			"", now, sessionDead)
		if got != nil {
			t.Fatalf("got %v, want nil (one owner still alive)", got)
		}
	})

	t.Run("unreadable wisp disqualifies the bead", func(t *testing.T) {
		got := orphanWispCandidates([]string{"bt-wisp-a"}, map[string]beadStatusInfo{}, "", now, dead)
		if got != nil {
			t.Fatalf("got %v, want nil (cannot prove it is orphaned)", got)
		}
	})

	t.Run("falls back to the work bead assignee", func(t *testing.T) {
		seen := ""
		sessionDead := func(assignee string) bool { seen = assignee; return true }
		got := orphanWispCandidates(
			[]string{"bt-wisp-a"},
			map[string]beadStatusInfo{"bt-wisp-a": {Status: "open", CreatedAt: old}},
			"bt/polecats/nux", now, sessionDead)
		if len(got) != 1 {
			t.Fatalf("got %v, want one candidate", got)
		}
		if seen != "bt/polecats/nux" {
			t.Fatalf("liveness checked %q, want the work bead assignee", seen)
		}
	})

	t.Run("already-closed wisps are skipped, not closed again", func(t *testing.T) {
		got := orphanWispCandidates(
			[]string{"bt-wisp-closed"},
			map[string]beadStatusInfo{"bt-wisp-closed": {Status: "closed", CreatedAt: old}},
			"", now, dead)
		if got != nil {
			t.Fatalf("got %v, want nil (nothing left to close)", got)
		}
	})

	t.Run("no blockers", func(t *testing.T) {
		if got := orphanWispCandidates(nil, nil, "", now, dead); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})
}

func TestSweepOrphanWispBlockers_SkipsReadyAndUnblocked(t *testing.T) {
	// A ready assessment and one blocked by a real bead must not trigger any
	// bd traffic; a zero return proves the sweep short-circuits before the
	// batch fetch.
	assessments := []scheduledContextAssessment{
		{ready: true, blocked: false},
		{ready: false, blocked: true, blockers: []string{"bt-real"}},
		{ready: false, blocked: false},
	}
	if got := sweepOrphanWispBlockers(t.TempDir(), assessments, true); got != 0 {
		t.Fatalf("sweep closed %d, want 0", got)
	}
}
