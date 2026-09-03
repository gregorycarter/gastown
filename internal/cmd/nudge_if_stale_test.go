package cmd

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseHeartbeatLabels(t *testing.T) {
	now := time.Now().Unix()

	ts, ok := parseHeartbeatLabels([]string{"gt:agent", "idle:3", "heartbeat:" + itoa(now)})
	if !ok {
		t.Fatal("expected heartbeat label to parse")
	}
	if ts.Unix() != now {
		t.Errorf("timestamp = %d, want %d", ts.Unix(), now)
	}

	if _, ok := parseHeartbeatLabels([]string{"gt:agent", "idle:3"}); ok {
		t.Error("no heartbeat label should report ok=false")
	}
	if _, ok := parseHeartbeatLabels(nil); ok {
		t.Error("nil labels should report ok=false")
	}
	if _, ok := parseHeartbeatLabels([]string{"heartbeat:not-a-number"}); ok {
		t.Error("unparsable heartbeat should report ok=false")
	}
	if _, ok := parseHeartbeatLabels([]string{"heartbeat:0"}); ok {
		t.Error("zero heartbeat should report ok=false")
	}
}

func TestParseHeartbeatLabelsPicksNewest(t *testing.T) {
	older := time.Now().Add(-time.Hour).Unix()
	newer := time.Now().Unix()
	ts, ok := parseHeartbeatLabels([]string{"heartbeat:" + itoa(older), "heartbeat:" + itoa(newer)})
	if !ok || ts.Unix() != newer {
		t.Errorf("got %v ok=%v, want newest %d", ts, ok, newer)
	}
}

func TestEvaluateIfStale(t *testing.T) {
	const threshold = 15 * time.Minute

	fresh := evaluateIfStale(threshold, 2*time.Minute, "gt-bt-witness", nil)
	if !fresh.Skip {
		t.Error("fresh heartbeat should skip the nudge")
	}
	if !strings.Contains(fresh.Reason, "nudge skipped") {
		t.Errorf("reason %q should explain the skip", fresh.Reason)
	}

	stale := evaluateIfStale(threshold, 30*time.Minute, "gt-bt-witness", nil)
	if stale.Skip {
		t.Error("stale heartbeat should send the nudge")
	}
	if !strings.Contains(stale.Reason, "sending") {
		t.Errorf("reason %q should explain the send", stale.Reason)
	}

	// Exactly at the threshold counts as stale (send).
	boundary := evaluateIfStale(threshold, threshold, "gt-bt-witness", nil)
	if boundary.Skip {
		t.Error("age == threshold should send, not skip")
	}
}

func TestEvaluateIfStaleFailsOpen(t *testing.T) {
	d := evaluateIfStale(15*time.Minute, 0, "", errors.New("agent bead not found"))
	if d.Skip {
		t.Error("an unreadable heartbeat must never suppress the nudge")
	}
	if !strings.Contains(d.Reason, "sending anyway") {
		t.Errorf("reason %q should say the nudge is sent anyway", d.Reason)
	}
}

func TestNudgeTargetToIdentity(t *testing.T) {
	tests := []struct {
		target string
		want   string
	}{
		{"mayor", "mayor"},
		{"mayor/", "mayor"},
		{"deacon", "deacon"},
		{"bridge_town_core/witness", "bridge_town_core/witness"},
		{"bridge_town_core/refinery", "bridge_town_core/refinery"},
		{"bridge_town_core/furiosa", "bridge_town_core/furiosa"},
		{"bridge_town_core/crew/max", "bridge_town_core/crew/max"},
		{"channel:workers", ""},
		{"gt-witness", ""},
		{"", ""},
	}
	for _, tc := range tests {
		if got := nudgeTargetToIdentity(tc.target); got != tc.want {
			t.Errorf("nudgeTargetToIdentity(%q) = %q, want %q", tc.target, got, tc.want)
		}
	}
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
