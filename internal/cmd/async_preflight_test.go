package cmd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/gastown/internal/asyncpreflight"
	"github.com/steveyegge/gastown/internal/beads"
	"github.com/steveyegge/gastown/internal/config"
)

type fakeRev string

func (f fakeRev) Rev(string) (string, error) { return string(f), nil }

func asyncSettings(on bool) *config.RigSettings {
	return &config.RigSettings{Delivery: &config.DeliveryConfig{SubmitWhilePreflight: on}}
}

func stubLaunch(t *testing.T, err error) *[]string {
	t.Helper()
	var calls []string
	old := launchAsyncPreflight
	launchAsyncPreflight = func(p string) error { calls = append(calls, p); return err }
	t.Cleanup(func() { launchAsyncPreflight = old })
	return &calls
}

func TestStartAsyncPreflightForDone(t *testing.T) {
	head := strings.Repeat("a", 40)
	args := []string{"--requirement", "R1", "--acceptance-test", "A1"}
	clean := func() (string, error) { return "", nil }
	pushed := func() (string, error) { return head, nil }
	now := time.Now()

	t.Run("disabled by default", func(t *testing.T) {
		calls := stubLaunch(t, nil)
		_, err := startAsyncPreflightForDone(&config.RigSettings{}, t.TempDir(), "kasb_core", "kasb-x.1", "polecat/nux/kasb-x.1", "/w", "", fakeRev(head), pushed, clean, args, now)
		if err == nil || !strings.Contains(err.Error(), "disabled") || len(*calls) != 0 {
			t.Fatalf("err=%v calls=%v", err, *calls)
		}
	})
	refusals := map[string]func() (string, error){
		"auto-saved": nil,
		"unpushed":   nil,
		"dirty":      nil,
	}
	for name := range refusals {
		t.Run(name, func(t *testing.T) {
			calls := stubLaunch(t, nil)
			autoSaved, remote, dirty := "", pushed, clean
			switch name {
			case "auto-saved":
				autoSaved = head
			case "unpushed":
				remote = func() (string, error) { return strings.Repeat("b", 40), nil }
			case "dirty":
				dirty = func() (string, error) { return " M x.py", nil }
			}
			rig := t.TempDir()
			_, err := startAsyncPreflightForDone(asyncSettings(true), rig, "kasb_core", "kasb-x.1", "b", "/w", autoSaved, fakeRev(head), remote, dirty, args, now)
			if err == nil || len(*calls) != 0 {
				t.Fatalf("err=%v calls=%v", err, *calls)
			}
			if files, _ := filepath.Glob(filepath.Join(asyncpreflight.Dir(rig), "*.json")); len(files) != 0 {
				t.Fatalf("marker left behind: %v", files)
			}
		})
	}
	t.Run("starts once and reuses on retry", func(t *testing.T) {
		calls := stubLaunch(t, nil)
		rig := t.TempDir()
		sub, err := startAsyncPreflightForDone(asyncSettings(true), rig, "kasb_core", "kasb-x.1", "b", "/w", "", fakeRev(head), pushed, clean, args, now)
		if err != nil || sub.Reused || len(*calls) != 1 || (*calls)[0] != sub.MarkerPath {
			t.Fatalf("sub=%+v err=%v calls=%v", sub, err, *calls)
		}
		m, err := asyncpreflight.Load(sub.MarkerPath)
		if err != nil || m.Status != asyncpreflight.StatusStarting || m.Worktree != "/w" || m.Command[0] != "uv" {
			t.Fatalf("marker=%+v err=%v", m, err)
		}
		again, err := startAsyncPreflightForDone(asyncSettings(true), rig, "kasb_core", "kasb-x.1", "b", "/w", "", fakeRev(head), pushed, clean, args, now)
		if err != nil || !again.Reused || len(*calls) != 1 {
			t.Fatalf("retry=%+v err=%v calls=%v", again, err, *calls)
		}
	})
	t.Run("launch failure leaves no marker", func(t *testing.T) {
		stubLaunch(t, errors.New("fork failed"))
		rig := t.TempDir()
		_, err := startAsyncPreflightForDone(asyncSettings(true), rig, "kasb_core", "kasb-x.1", "b", "/w", "", fakeRev(head), pushed, clean, args, now)
		if err == nil || !strings.Contains(err.Error(), "fork failed") {
			t.Fatalf("err=%v", err)
		}
		if files, _ := filepath.Glob(filepath.Join(asyncpreflight.Dir(rig), "*.json")); len(files) != 0 {
			t.Fatalf("marker left behind: %v", files)
		}
	})
	t.Run("custom command prefix", func(t *testing.T) {
		stubLaunch(t, nil)
		s := asyncSettings(true)
		s.Delivery.AsyncPreflightCommand = []string{"python3", "tools/remote_preflight.py", "--"}
		sub, err := startAsyncPreflightForDone(s, t.TempDir(), "kasb_core", "kasb-x.1", "b", "/w", "", fakeRev(head), pushed, clean, args, now)
		if err != nil {
			t.Fatal(err)
		}
		m, _ := asyncpreflight.Load(sub.MarkerPath)
		if strings.Join(m.Command[:3], " ") != "python3 tools/remote_preflight.py --" {
			t.Fatalf("command=%v", m.Command)
		}
	})
}

func TestEnsureAsyncPreflightMRFields(t *testing.T) {
	mr := &beads.Issue{ID: "kasb-wisp-1", Description: "branch: b\ncommit_sha: " + strings.Repeat("a", 40)}
	var updated string
	show := func(string) (*beads.Issue, error) { return mr, nil }
	update := func(id, desc string) error { updated = desc; mr.Description = desc; return nil }
	if err := ensureAsyncPreflightMRFields(show, update, mr.ID, "/m.json"); err != nil {
		t.Fatal(err)
	}
	if asyncpreflight.ParseMRFields(updated).RunPath != "/m.json" {
		t.Fatalf("not bound: %q", updated)
	}
	updated = ""
	if err := ensureAsyncPreflightMRFields(show, update, mr.ID, "/m.json"); err != nil || updated != "" {
		t.Fatalf("rewrote an already-bound MR: %v %q", err, updated)
	}
}

// gateFixture builds a kasb MR bound to an async preflight marker.
// fakeRunnerProcess makes PID 4242 a live runner started at "start-1".
func fakeRunnerProcess(t *testing.T) {
	t.Helper()
	old := asyncpreflight.ProcessStartTime
	asyncpreflight.ProcessStartTime = func(pid int) string {
		if pid == 4242 {
			return "start-1"
		}
		return ""
	}
	t.Cleanup(func() { asyncpreflight.ProcessStartTime = old })
}

func asyncGateFixture(t *testing.T, status string) (rig string, mr *beads.Issue, source *beads.Issue, show func(string) (*beads.Issue, error), marker *asyncpreflight.Marker) {
	t.Helper()
	mr, source, show = kasbRecoveryFixture()
	mr.Status, mr.Labels = "open", []string{"gt:merge-request"}
	rig = t.TempDir()
	m, path, _, err := asyncpreflight.Prepare(asyncpreflight.StartRequest{RigPath: rig, Rig: "kasb_core", Source: source.ID,
		Branch: "polecat/chrome/kasb-test.1+abc", Head: strings.Repeat("a", 40), Worktree: "/w",
		Prefix: []string{"preflight"}, Args: []string{"--requirement", "R"}, Now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	m.Status, m.Reason, m.ReceiptStatus, m.HeartbeatAt = status, "api-tests: test_x failed", "failed", time.Now().UTC().Format(time.RFC3339Nano)
	if status == asyncpreflight.StatusRunning {
		fakeRunnerProcess(t)
		m.PID, m.PIDStart, m.StartedAt = 4242, "start-1", m.HeartbeatAt
	}
	if err := asyncpreflight.Save(path, m); err != nil {
		t.Fatal(err)
	}
	mr.Description = asyncpreflight.WithDescriptionFields(mr.Description, path)
	return rig, mr, source, show, m
}

func TestPreflightGateFailedReturnsSameMRToOriginalWorker(t *testing.T) {
	rig, mr, source, show, _ := asyncGateFixture(t, asyncpreflight.StatusFailed)
	var blockedReason string
	var comments []string
	deps := preflightGateDeps{
		show: show,
		block: func(id, reason string) error {
			// Mirror bd update --status=blocked --add-label=needs-repair --set-metadata=correction_reason=...
			blockedReason = reason
			mr.Status = "blocked"
			mr.Labels = append(mr.Labels, "needs-repair")
			mr.Metadata, _ = json.Marshal(map[string]any{"correction_reason": reason})
			return nil
		},
		comment: func(id, text string) error { comments = append(comments, id); return nil },
		now:     time.Now,
	}
	res, applied, err := runPreflightGate(rig, mr.ID, "", true, deps)
	if err != nil || !applied || res.State != asyncpreflight.GateFailed {
		t.Fatalf("res=%+v applied=%v err=%v", res, applied, err)
	}
	if !strings.Contains(blockedReason, "test_x failed") || len(comments) != 2 || comments[1] != source.ID {
		t.Fatalf("reason=%q comments=%v", blockedReason, comments)
	}
	// The existing Kasb same-MR repair path accepts the disposition and
	// addresses the original worker on the original branch and MR.
	state, err := kasbRecoveryCandidate(mr, strings.Repeat("b", 40), show)
	if err != nil {
		t.Fatalf("repair sweep refused the disposition: %v", err)
	}
	if state.Record.Kind != "same-mr-repair" || state.Record.Worker != "chrome" || state.Record.MR != mr.ID || state.Record.RepairReason != blockedReason {
		t.Fatalf("record=%+v", state.Record)
	}
	// A second pass does not re-apply to a blocked MR.
	if _, applied, _ := runPreflightGate(rig, mr.ID, "", true, deps); applied {
		t.Fatal("re-applied to a blocked MR")
	}
}

func TestPreflightGatePendingAndPassedDoNotApply(t *testing.T) {
	for _, status := range []string{asyncpreflight.StatusRunning, asyncpreflight.StatusPassed} {
		t.Run(status, func(t *testing.T) {
			rig, mr, source, show, m := asyncGateFixture(t, status)
			if status == asyncpreflight.StatusPassed {
				raw, _ := json.Marshal(map[string]string{"head_sha": m.Head, "source_bead_id": source.ID, "status": "passed"})
				if err := os.WriteFile(m.ReceiptCopy, raw, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			deps := preflightGateDeps{show: show, now: time.Now,
				block:   func(string, string) error { t.Fatal("blocked"); return nil },
				comment: func(string, string) error { t.Fatal("commented"); return nil }}
			res, applied, err := runPreflightGate(rig, mr.ID, m.Head, true, deps)
			if err != nil || applied {
				t.Fatalf("res=%+v applied=%v err=%v", res, applied, err)
			}
			if (status == asyncpreflight.StatusPassed) != res.Landable() {
				t.Fatalf("res=%+v", res)
			}
		})
	}
}

func TestPreflightGateLegacyMRLandsAsBefore(t *testing.T) {
	mr, _, show := kasbRecoveryFixture()
	mr.Status = "open"
	res, applied, err := runPreflightGate(t.TempDir(), mr.ID, "", true, preflightGateDeps{show: show, now: time.Now})
	if err != nil || applied || res.State != asyncpreflight.GateNone || !res.Landable() {
		t.Fatalf("res=%+v applied=%v err=%v", res, applied, err)
	}
}

func TestKasbResubmissionDropsAsyncPreflightBinding(t *testing.T) {
	mr, _, _ := kasbRecoveryFixture()
	mr.Description = asyncpreflight.WithDescriptionFields(mr.Description, "/r/.runtime/async-preflight/x.json")
	updated, err := kasbResubmittedDescription(mr, strings.Repeat("c", 40))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(updated, "preflight_run") || strings.Contains(updated, "preflight_receipt") || !strings.Contains(updated, "commit_sha: "+strings.Repeat("c", 40)) {
		t.Fatal(updated)
	}
}

func TestPreflightGateStopsSupersededRunner(t *testing.T) {
	rig, mr, _, show, m := asyncGateFixture(t, asyncpreflight.StatusRunning)
	var stopped *asyncpreflight.Marker
	var blocked bool
	deps := preflightGateDeps{show: show, now: time.Now,
		block:   func(string, string) error { blocked = true; return nil },
		comment: func(string, string) error { return nil },
		stop:    func(x *asyncpreflight.Marker) bool { stopped = x; return true }}
	res, applied, err := runPreflightGate(rig, mr.ID, strings.Repeat("d", 40), true, deps)
	if err != nil || !applied || !blocked || res.State != asyncpreflight.GateMoved {
		t.Fatalf("res=%+v applied=%v err=%v", res, applied, err)
	}
	if stopped == nil || stopped.Head != m.Head || !strings.Contains(res.Reason, "superseded runner stopped") {
		t.Fatalf("runner not stopped: %+v %q", stopped, res.Reason)
	}
	// Without --apply nothing is stopped.
	stopped = nil
	if _, _, err := runPreflightGate(rig, mr.ID, strings.Repeat("d", 40), false, deps); err != nil || stopped != nil {
		t.Fatalf("stopped without --apply: %v", err)
	}
}

func TestPreflightGateFindsMarkerWhenBindingMissing(t *testing.T) {
	rig, mr, _, show, _ := asyncGateFixture(t, asyncpreflight.StatusRunning)
	var kept []string
	for _, line := range strings.Split(mr.Description, "\n") {
		if !strings.HasPrefix(line, "preflight_") {
			kept = append(kept, line)
		}
	}
	mr.Description = strings.Join(kept, "\n")
	res, _, err := runPreflightGate(rig, mr.ID, "", false, preflightGateDeps{show: show, now: time.Now})
	if err != nil || res.State != asyncpreflight.GatePending || res.Landable() {
		t.Fatalf("unbound async MR would land: %+v %v", res, err)
	}
}

func TestHoldUnboundAsyncMR(t *testing.T) {
	var blockedID, comment string
	err := holdUnboundAsyncMR(func(id, reason string) error { blockedID = id; return nil },
		func(id, text string) error { comment = text; return nil }, "kasb-wisp-1", "/m.json", errors.New("dolt timeout"))
	if err == nil || blockedID != "kasb-wisp-1" || !strings.Contains(comment, "dolt timeout") {
		t.Fatalf("err=%v blocked=%q comment=%q", err, blockedID, comment)
	}
	err = holdUnboundAsyncMR(func(string, string) error { return errors.New("bd down") },
		func(string, string) error { return nil }, "kasb-wisp-1", "/m.json", errors.New("dolt timeout"))
	if err == nil || !strings.Contains(err.Error(), "could not be held") {
		t.Fatalf("hold failure not surfaced: %v", err)
	}
}

func TestAsyncPreflightTimeoutFromSettings(t *testing.T) {
	stubLaunch(t, nil)
	s := asyncSettings(true)
	s.Delivery.AsyncPreflightTimeout = "45m"
	head := strings.Repeat("a", 40)
	sub, err := startAsyncPreflightForDone(s, t.TempDir(), "kasb_core", "kasb-x.1", "b", "/w", "", fakeRev(head),
		func() (string, error) { return head, nil }, func() (string, error) { return "", nil },
		[]string{"--requirement", "R"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	m, _ := asyncpreflight.Load(sub.MarkerPath)
	if m.Timeout() != 45*time.Minute {
		t.Fatalf("timeout %s", m.Timeout())
	}
	if (&config.RigSettings{}).AsyncPreflightTimeoutOrDefault() != 2*time.Hour {
		t.Fatal("default timeout is not 2h")
	}
}

func TestMRStateFromBeads(t *testing.T) {
	mr := &beads.Issue{ID: "kasb-wisp-1", Status: "closed", Description: "commit_sha: " + strings.Repeat("a", 40)}
	st := mrStateFromBeads(func(string) (*beads.Issue, error) { return mr, nil })("kasb-wisp-1")
	if !st.Known || !st.Closed || st.Head != strings.Repeat("a", 40) {
		t.Fatalf("%+v", st)
	}
	if st := mrStateFromBeads(func(string) (*beads.Issue, error) { return nil, errors.New("x") })("kasb-wisp-1"); st.Known {
		t.Fatal("lookup error treated as known")
	}
}
