package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newScheduleTestManager(t *testing.T) *ConvoyManager {
	t.Helper()
	m := NewConvoyManager(t.TempDir(), func(string, ...interface{}) {}, "gt", 0, nil, nil, nil)
	t.Cleanup(m.cancel)
	return m
}

func TestConvoyManager_Defaults(t *testing.T) {
	m := newScheduleTestManager(t)
	if m.scanInterval != 5*time.Minute {
		t.Errorf("stranded interval = %v, want 5m", m.scanInterval)
	}
	if m.completionSweep != 10*time.Minute {
		t.Errorf("completion sweep = %v, want 10m", m.completionSweep)
	}
	if !m.skipInactiveRigs {
		t.Error("skipInactiveRigs should default on")
	}
}

func TestConvoyManager_Configure(t *testing.T) {
	m := newScheduleTestManager(t)
	off := false
	m.Configure(&ConvoyManagerConfig{StrandedInterval: "90s", CompletionSweepInterval: "0", SkipInactiveRigs: &off})
	if m.scanInterval != 90*time.Second || m.completionSweep != 0 || m.skipInactiveRigs {
		t.Errorf("configure: interval=%v sweep=%v skip=%v", m.scanInterval, m.completionSweep, m.skipInactiveRigs)
	}

	m2 := newScheduleTestManager(t)
	m2.Configure(&ConvoyManagerConfig{StrandedInterval: "bogus", CompletionSweepInterval: "-1m"})
	if m2.scanInterval != defaultStrandedScanInterval || m2.completionSweep != defaultCompletionSweepInterval {
		t.Errorf("invalid values must keep defaults: interval=%v sweep=%v", m2.scanInterval, m2.completionSweep)
	}
	m2.Configure(nil) // no-op
}

func TestNextScanDelay_WaitsFullIntervalUnlessRecovering(t *testing.T) {
	m := newScheduleTestManager(t)
	if got := m.nextScanDelay(); got != 5*time.Minute {
		t.Errorf("normal delay = %v, want 5m", got)
	}
	m.recoveryMode.Store(true)
	if got := m.nextScanDelay(); got != recoveryScanInterval {
		t.Errorf("recovery delay = %v, want %v", got, recoveryScanInterval)
	}
}

func TestCompletionAction_AllClosedChecksImmediately(t *testing.T) {
	m := newScheduleTestManager(t)
	now := time.Now()
	c := strandedConvoyInfo{ID: "hq-cv-a", TrackedCount: 2, OpenIssues: []string{}}
	// Even straight after a check: the scan saw every tracked issue closed.
	m.markCompletionChecked(c.ID, now)
	if check, reason := m.completionAction(c, now, nil); !check || reason != completionReasonAllClosed {
		t.Errorf("all closed: check=%v reason=%q", check, reason)
	}
}

func TestCompletionAction_OpenIssuesWaitForSweep(t *testing.T) {
	m := newScheduleTestManager(t)
	now := time.Now()
	c := strandedConvoyInfo{ID: "hq-cv-b", TrackedCount: 1, OpenIssues: []string{"kasb-1"}}

	// First sighting is staggered into the sweep window, never checked at once.
	if check, reason := m.completionAction(c, now, nil); check {
		t.Fatalf("first sighting with open issues should not check, got reason %q", reason)
	}
	// Within one full sweep interval it becomes due exactly once.
	due := now.Add(m.completionSweep)
	if check, reason := m.completionAction(c, due, nil); !check || reason != completionReasonSweep {
		t.Fatalf("after sweep interval: check=%v reason=%q", check, reason)
	}
	m.markCompletionChecked(c.ID, due)
	if check, _ := m.completionAction(c, due.Add(5*time.Minute), nil); check {
		t.Error("checked again before the next sweep")
	}
	if check, _ := m.completionAction(c, due.Add(10*time.Minute), nil); !check {
		t.Error("not checked at the next sweep")
	}
}

func TestCompletionAction_EventCheckDefersSweep(t *testing.T) {
	m := newScheduleTestManager(t)
	base := time.Now()
	m.now = func() time.Time { return base }
	c := strandedConvoyInfo{ID: "hq-cv-c", TrackedCount: 1, OpenIssues: []string{"kasb-1"}}

	m.recordEventChecks([]string{c.ID})
	if check, _ := m.completionAction(c, base.Add(9*time.Minute), nil); check {
		t.Error("sweep repeated a check the close-event poll just ran")
	}
	if check, _ := m.completionAction(c, base.Add(10*time.Minute), nil); !check {
		t.Error("sweep did not run after the interval")
	}
	if got := m.metrics.checksEvent.Load(); got != 1 {
		t.Errorf("event checks metric = %d, want 1", got)
	}
}

func TestCompletionAction_LegacyOutputChecksOnFirstSight(t *testing.T) {
	m := newScheduleTestManager(t)
	now := time.Now()
	c := strandedConvoyInfo{ID: "hq-cv-d", TrackedCount: 1} // OpenIssues nil: older gt
	if check, reason := m.completionAction(c, now, nil); !check || reason != completionReasonSweep {
		t.Fatalf("legacy first sight: check=%v reason=%q", check, reason)
	}
	m.markCompletionChecked(c.ID, now)
	if check, _ := m.completionAction(c, now.Add(time.Minute), nil); check {
		t.Error("legacy output re-checked before the sweep interval")
	}
}

func TestCompletionAction_ZeroSweepChecksEveryScan(t *testing.T) {
	m := newScheduleTestManager(t)
	m.completionSweep = 0
	now := time.Now()
	c := strandedConvoyInfo{ID: "hq-cv-e", TrackedCount: 1, OpenIssues: []string{"kasb-1"}}
	for i := 0; i < 3; i++ {
		if check, reason := m.completionAction(c, now, nil); !check || reason != completionReasonLegacy {
			t.Fatalf("scan %d: check=%v reason=%q", i, check, reason)
		}
		m.markCompletionChecked(c.ID, now)
	}
}

func writeRoutesForTest(t *testing.T, townRoot string, routes ...string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(townRoot, ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(townRoot, ".beads", "routes.jsonl"), []byte(strings.Join(routes, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRigActivity_InactiveReasons(t *testing.T) {
	town := t.TempDir()
	writeRoutesForTest(t, town,
		`{"prefix":"hq-","path":"."}`,
		`{"prefix":"kasb-","path":"kasb_core"}`,
		`{"prefix":"bt-","path":"bridge_town_core/mayor/rig"}`,
		`{"prefix":"gone-","path":"gone_rig"}`,
	)
	for _, rig := range []string{"kasb_core", "bridge_town_core"} {
		if err := os.MkdirAll(filepath.Join(town, rig), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	parkedCalls := 0
	ra := newRigActivity(town, func(rig string) bool {
		parkedCalls++
		return rig == "bridge_town_core"
	})

	cases := map[string]string{
		"hq-cv-1":                   "",
		"kasb-abc":                  "",
		"external:kasb:kasb-abc":    "",
		"bt-xyz":                    "rig bridge_town_core parked or docked",
		"external:bt:bt-xyz.2":      "rig bridge_town_core parked or docked",
		"hisn-4e4.11":               "no route for prefix hisn- (rig removed)",
		"external:hisn:hisn-0xhy.1": "no route for prefix hisn- (rig removed)",
		"gone-1":                    "rig gone_rig removed",
		"noprefix":                  "",
	}
	for id, want := range cases {
		if got := ra.inactiveReason(id); got != want {
			t.Errorf("inactiveReason(%q) = %q, want %q", id, got, want)
		}
	}
	if parkedCalls != 2 {
		t.Errorf("parked lookups = %d, want one per rig (2)", parkedCalls)
	}

	// Without a routes file nothing is judged inactive.
	if why := newRigActivity(t.TempDir(), nil).inactiveReason("hisn-1"); why != "" {
		t.Errorf("no routes file should judge nothing, got %q", why)
	}
}

func TestCompletionAction_SkipsConvoysOnInactiveRigs(t *testing.T) {
	m := newScheduleTestManager(t)
	var logs []string
	m.logger = func(format string, args ...interface{}) { logs = append(logs, format) }
	town := t.TempDir()
	writeRoutesForTest(t, town, `{"prefix":"kasb-","path":"kasb_core"}`)
	if err := os.MkdirAll(filepath.Join(town, "kasb_core"), 0o755); err != nil {
		t.Fatal(err)
	}
	ra := newRigActivity(town, nil)
	now := time.Now()

	removed := strandedConvoyInfo{ID: "hq-cv-dead", TrackedCount: 2, OpenIssues: []string{"kasb-1", "hisn-9"}}
	for i := 0; i < 3; i++ {
		if check, reason := m.completionAction(removed, now.Add(time.Duration(i)*time.Hour), ra); check || reason != completionReasonInactive {
			t.Fatalf("removed-rig convoy: check=%v reason=%q", check, reason)
		}
	}
	skipLogs := 0
	for _, l := range logs {
		if strings.Contains(l, "skipping completion checks") {
			skipLogs++
		}
	}
	if skipLogs != 1 {
		t.Errorf("skip logged %d times, want once per state change", skipLogs)
	}

	// All closed beats the inactive rule: nothing open is left to wait for.
	closed := strandedConvoyInfo{ID: "hq-cv-dead", TrackedCount: 2, OpenIssues: []string{}}
	if check, _ := m.completionAction(closed, now, ra); !check {
		t.Error("all-closed convoy must be checked even if it once tracked a removed rig")
	}

	// Skip disabled: the convoy enters the normal sweep schedule.
	m.skipInactiveRigs = false
	later := now.Add(24 * time.Hour)
	if check, reason := m.completionAction(removed, later, nil); check || reason != completionReasonNotDue {
		t.Errorf("skip disabled, first sighting: check=%v reason=%q", check, reason)
	}
	if check, reason := m.completionAction(removed, later.Add(m.completionSweep), nil); !check || reason != completionReasonSweep {
		t.Errorf("skip disabled, after sweep: check=%v reason=%q", check, reason)
	}
}

func TestCompletionStagger_StableAndInRange(t *testing.T) {
	interval := 10 * time.Minute
	seen := map[time.Duration]bool{}
	for _, id := range []string{"hq-cv-a", "hq-cv-b", "hq-cv-c", "hq-cv-d", "hq-cv-e", "hq-cv-f"} {
		d := completionStagger(id, interval)
		if d < 0 || d >= interval {
			t.Errorf("stagger(%s) = %v out of range", id, d)
		}
		if d != completionStagger(id, interval) {
			t.Errorf("stagger(%s) not stable", id)
		}
		seen[d] = true
	}
	if len(seen) < 3 {
		t.Errorf("stagger does not spread convoys: %v", seen)
	}
	if completionStagger("x", 0) != 0 {
		t.Error("zero interval must give zero stagger")
	}
}

// TestScan_EventDrivenCompletion runs a scan against a mock gt and checks that
// only the convoy whose tracked issues are all closed gets `gt convoy check`.
func TestScan_EventDrivenCompletion(t *testing.T) {
	paths := mockGtForScanTest(t, scanTestOpts{
		strandedJSON: `[
 {"id":"hq-cv-done","title":"done","tracked_count":2,"ready_count":0,"ready_issues":[],"open_issues":[]},
 {"id":"hq-cv-wait","title":"wait","tracked_count":1,"ready_count":0,"ready_issues":[],"open_issues":["kasb-1"]},
 {"id":"hq-cv-dead","title":"dead","tracked_count":1,"ready_count":0,"ready_issues":[],"open_issues":["hisn-1"]}
]`,
		routes: `{"prefix":"kasb-","path":"kasb_core"}` + "\n",
	})
	if err := os.MkdirAll(filepath.Join(paths.townRoot, "kasb_core"), 0o755); err != nil {
		t.Fatal(err)
	}
	m := NewConvoyManager(paths.townRoot, func(string, ...interface{}) {}, filepath.Join(paths.binDir, "gt"), time.Hour, nil, nil, nil)
	defer m.cancel()

	m.scan()
	m.scan() // a second scan must not repeat anything
	data, err := os.ReadFile(paths.checkLogPath)
	if err != nil {
		t.Fatalf("no convoy check ran: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "hq-cv-done") || !strings.Contains(lines[1], "hq-cv-done") {
		t.Errorf("checks = %q, want hq-cv-done on each scan only", lines)
	}
	if got := m.metrics.skipInactive.Load(); got != 2 {
		t.Errorf("inactive skips = %d, want 2", got)
	}
	line := m.metrics.snapshotLine(time.Hour)
	for _, want := range []string{"convoy checks=2", "all-closed=2", "inactive-rig=2", "stranded scans=2"} {
		if !strings.Contains(line, want) {
			t.Errorf("metrics line %q missing %q", line, want)
		}
	}
}

func TestPruneCompletionState_DropsClosedConvoys(t *testing.T) {
	m := newScheduleTestManager(t)
	now := time.Now()
	m.markCompletionChecked("hq-cv-open", now)
	m.markCompletionChecked("hq-cv-closed", now)
	m.inactiveSkip["hq-cv-closed"] = "x"
	m.pruneCompletionState([]strandedConvoyInfo{{ID: "hq-cv-open"}})
	if _, ok := m.lastCompletionCheck["hq-cv-closed"]; ok {
		t.Error("closed convoy state not pruned")
	}
	if _, ok := m.inactiveSkip["hq-cv-closed"]; ok {
		t.Error("closed convoy skip state not pruned")
	}
	if _, ok := m.lastCompletionCheck["hq-cv-open"]; !ok {
		t.Error("open convoy state pruned")
	}
}
