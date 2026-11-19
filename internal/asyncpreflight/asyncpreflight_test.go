package asyncpreflight

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	headA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	headB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	src   = "kasb-abc.1"
)

// withProcesses fakes the process table: pid -> kernel start time.
func withProcesses(t *testing.T, procs map[int]string) {
	t.Helper()
	old := ProcessStartTime
	ProcessStartTime = func(pid int) string { return procs[pid] }
	t.Cleanup(func() { ProcessStartTime = old })
}

func withDeadPIDs(t *testing.T) { withProcesses(t, nil) }

func running(now time.Time) func(*Marker) {
	return func(m *Marker) {
		m.Status, m.PID, m.PIDStart = StatusRunning, 4242, "start-1"
		m.StartedAt, m.HeartbeatAt = now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)
	}
}

func request(rigPath, worktree string, now time.Time) StartRequest {
	return StartRequest{RigPath: rigPath, Rig: "kasb_core", Source: src, Branch: "polecat/nux/" + src, Head: headA,
		Worktree: worktree, Prefix: []string{"uv", "run", "python", "scripts/delivery/preflight.py"},
		Args: []string{"--requirement", "R1", "--acceptance-test", "A1"}, Now: now}
}

func TestValidateArgs(t *testing.T) {
	ok := [][]string{
		{"--requirement", "R", "--acceptance-test", "A"},
		{"--requirement=R", "--acceptance-test=A", "--stage", "broad"},
	}
	for _, a := range ok {
		if err := ValidateArgs(a); err != nil {
			t.Errorf("ValidateArgs(%v) = %v", a, err)
		}
	}
	bad := [][]string{
		nil,
		{"--requirement", "R", "--receipt", "x.json"},
		{"--head=abc"},
		{"--source-bead-id", "kasb-x"},
		{"--stage", "light"},
		{"--stage=light"},
		{"--red-command", "pytest x"},
		{"verify-receipt", "--receipt", "x"},
	}
	for _, a := range bad {
		if err := ValidateArgs(a); err == nil {
			t.Errorf("ValidateArgs(%v) accepted", a)
		}
	}
}

func TestPrepareCreatesReusesAndRefuses(t *testing.T) {
	withDeadPIDs(t)
	rig := t.TempDir()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	m, path, reused, err := Prepare(request(rig, "/w", now))
	if err != nil || reused {
		t.Fatalf("Prepare: %v reused=%v", err, reused)
	}
	if m.Status != StatusStarting || m.Head != headA || m.ReceiptPath != ".delivery/preflight-receipts/async-aaaaaaaaaaaa.json" {
		t.Fatalf("unexpected marker %+v", m)
	}
	want := []string{"uv", "run", "python", "scripts/delivery/preflight.py", "--requirement", "R1", "--acceptance-test", "A1",
		"--head", headA, "--source-bead-id", src, "--receipt", m.ReceiptPath}
	if strings.Join(m.Command, " ") != strings.Join(want, " ") {
		t.Fatalf("command = %v", m.Command)
	}
	// Idempotent retry while the run is fresh.
	if _, p2, reused, err := Prepare(request(rig, "/w", now.Add(time.Minute))); err != nil || !reused || p2 != path {
		t.Fatalf("retry: %v reused=%v path=%s", err, reused, p2)
	}
	// A failed head is refused.
	m.Status, m.Reason = StatusFailed, "api-tests failed"
	if err := Save(path, m); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Prepare(request(rig, "/w", now)); err == nil || !strings.Contains(err.Error(), "already failed") {
		t.Fatalf("failed head not refused: %v", err)
	}
	// An errored run is replaced and the old marker kept aside.
	m.Status = StatusError
	_ = Save(path, m)
	if _, _, reused, err := Prepare(request(rig, "/w", now)); err != nil || reused {
		t.Fatalf("error run not replaced: %v reused=%v", err, reused)
	}
	if prev, _ := filepath.Glob(path + ".*.prev"); len(prev) != 1 {
		t.Fatalf("previous marker not retained: %v", prev)
	}
	// A lost run (stale heartbeat, dead pid) is replaced as well.
	cur, _ := Load(path)
	cur.Status, cur.PID, cur.PIDStart, cur.HeartbeatAt = StatusRunning, 999999, "x", now.Add(-time.Hour).Format(time.RFC3339Nano)
	_ = Save(path, cur)
	if _, _, reused, err := Prepare(request(rig, "/w", now)); err != nil || reused {
		t.Fatalf("lost run not replaced: %v reused=%v", err, reused)
	}
}

func TestDescriptionFields(t *testing.T) {
	desc := "branch: b\ncommit_sha: " + headA + "\nsource_issue: " + src + "\npreflight_run: /old\npreflight_receipt: pending"
	got := WithDescriptionFields(desc, "/new.json")
	f := ParseMRFields(got)
	if f.RunPath != "/new.json" || f.ReceiptTag != "pending" || f.CommitSHA != headA || f.Source != src {
		t.Fatalf("fields = %+v from %q", f, got)
	}
	if strings.Count(got, "preflight_run:") != 1 {
		t.Fatalf("duplicate binding: %q", got)
	}
}

type gateFixture struct {
	rig, path string
	m         *Marker
	f         MRFields
	now       time.Time
}

func newGateFixture(t *testing.T) gateFixture {
	t.Helper()
	withDeadPIDs(t)
	rig := t.TempDir()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	m, path, _, err := Prepare(request(rig, "/w", now))
	if err != nil {
		t.Fatal(err)
	}
	return gateFixture{rig: rig, path: path, m: m, now: now, f: MRFields{Source: src, CommitSHA: headA, RunPath: path}}
}

func (g gateFixture) set(t *testing.T, mutate func(*Marker)) {
	t.Helper()
	mutate(g.m)
	if err := Save(g.path, g.m); err != nil {
		t.Fatal(err)
	}
}

func writeReceipt(t *testing.T, path, head, source, status string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"head_sha": head, "source_bead_id": source, "status": status})
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluate(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		if g := Evaluate(t.TempDir(), MRFields{}, "", time.Now()); g.State != GateNone || !g.Landable() {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("pending: fresh heartbeat and the recorded runner process", func(t *testing.T) {
		fx := newGateFixture(t)
		withProcesses(t, map[int]string{4242: "start-1"})
		fx.set(t, running(fx.now))
		g := Evaluate(fx.rig, fx.f, headA, fx.now.Add(2*time.Minute))
		if g.State != GatePending || g.Landable() || g.NeedsFix() {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("starting within grace is pending, after grace lost", func(t *testing.T) {
		fx := newGateFixture(t)
		if g := Evaluate(fx.rig, fx.f, "", fx.now.Add(time.Minute)); g.State != GatePending {
			t.Fatalf("%+v", g)
		}
		if g := Evaluate(fx.rig, fx.f, "", fx.now.Add(StartGrace+time.Second)); g.State != GateLost {
			t.Fatalf("%+v", g)
		}
	})
	lost := map[string]struct {
		procs map[int]string
		at    time.Duration
		edit  func(*Marker)
	}{
		"pid gone despite fresh heartbeat":    {nil, time.Minute, nil},
		"pid reused after reboot":             {map[int]string{4242: "start-2"}, time.Minute, nil},
		"hung runner: stale heartbeat":        {map[int]string{4242: "start-1"}, HeartbeatFresh + time.Second, nil},
		"no recorded start time":              {map[int]string{4242: ""}, time.Minute, func(m *Marker) { m.PIDStart = "" }},
		"past hard timeout despite heartbeat": {map[int]string{4242: "start-1"}, 0, func(m *Marker) { m.TimeoutSecs = 60 }},
	}
	for name, tc := range lost {
		t.Run("lost: "+name, func(t *testing.T) {
			fx := newGateFixture(t)
			withProcesses(t, tc.procs)
			at := fx.now.Add(tc.at)
			start := fx.now
			if name == "past hard timeout despite heartbeat" {
				at = fx.now.Add(time.Minute + StartGrace + time.Second)
			}
			fx.set(t, running(start))
			if tc.edit != nil {
				fx.set(t, tc.edit)
			}
			if name == "past hard timeout despite heartbeat" {
				fx.set(t, func(m *Marker) { m.HeartbeatAt = at.Add(-time.Second).Format(time.RFC3339Nano) })
			}
			g := Evaluate(fx.rig, fx.f, "", at)
			if g.State != GateLost || !g.NeedsFix() {
				t.Fatalf("%+v", g)
			}
		})
	}
	t.Run("MR without binding is still gated by its marker", func(t *testing.T) {
		fx := newGateFixture(t)
		f := fx.f
		f.RunPath = ""
		if g := Evaluate(fx.rig, f, "", fx.now.Add(time.Minute)); g.State != GatePending || g.Run == nil {
			t.Fatalf("%+v", g)
		}
		f.CommitSHA = headB
		if g := Evaluate(fx.rig, f, "", fx.now); g.State != GateNone {
			t.Fatalf("other head gated: %+v", g)
		}
	})
	t.Run("passed needs a matching receipt copy", func(t *testing.T) {
		fx := newGateFixture(t)
		fx.set(t, func(m *Marker) { m.Status = StatusPassed })
		if g := Evaluate(fx.rig, fx.f, headA, fx.now); g.State != GateInvalid {
			t.Fatalf("no receipt copy: %+v", g)
		}
		writeReceipt(t, fx.m.ReceiptCopy, headA, src, "passed_light")
		if g := Evaluate(fx.rig, fx.f, headA, fx.now); g.State != GateInvalid {
			t.Fatalf("light receipt accepted: %+v", g)
		}
		writeReceipt(t, fx.m.ReceiptCopy, headB, src, "passed")
		if g := Evaluate(fx.rig, fx.f, headA, fx.now); g.State != GateInvalid {
			t.Fatalf("other head accepted: %+v", g)
		}
		writeReceipt(t, fx.m.ReceiptCopy, headA, src, "passed")
		if g := Evaluate(fx.rig, fx.f, headA, fx.now); g.State != GatePassed || !g.Landable() {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("failed", func(t *testing.T) {
		fx := newGateFixture(t)
		fx.set(t, func(m *Marker) { m.Status, m.ReceiptStatus, m.Reason = StatusFailed, "failed", "api-tests: 2 failed" })
		g := Evaluate(fx.rig, fx.f, "", fx.now)
		if g.State != GateFailed || !strings.Contains(g.Reason, "api-tests: 2 failed") {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("moved branch", func(t *testing.T) {
		fx := newGateFixture(t)
		fx.set(t, func(m *Marker) { m.Status = StatusPassed })
		writeReceipt(t, fx.m.ReceiptCopy, headA, src, "passed")
		if g := Evaluate(fx.rig, fx.f, headB, fx.now); g.State != GateMoved {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("marker bound to another head", func(t *testing.T) {
		fx := newGateFixture(t)
		f := fx.f
		f.CommitSHA = headB
		if g := Evaluate(fx.rig, f, "", fx.now); g.State != GateInvalid {
			t.Fatalf("%+v", g)
		}
	})
	t.Run("run path outside the rig run dir", func(t *testing.T) {
		fx := newGateFixture(t)
		outside := filepath.Join(t.TempDir(), "x.json")
		raw, _ := os.ReadFile(fx.path)
		_ = os.WriteFile(outside, raw, 0o644)
		f := fx.f
		f.RunPath = outside
		if g := Evaluate(fx.rig, f, "", fx.now); g.State != GateInvalid {
			t.Fatalf("%+v", g)
		}
		f.RunPath = filepath.Join(Dir(fx.rig), "..", "async-preflight", filepath.Base(fx.path))
		if g := Evaluate(fx.rig, f, "", fx.now); g.State == GateInvalid {
			t.Fatalf("clean-able in-dir path refused: %+v", g)
		}
	})
	t.Run("missing marker", func(t *testing.T) {
		fx := newGateFixture(t)
		_ = os.Remove(fx.path)
		if g := Evaluate(fx.rig, fx.f, "", fx.now); g.State != GateInvalid {
			t.Fatalf("%+v", g)
		}
	})
}

func TestRunningForWorktree(t *testing.T) {
	fx := newGateFixture(t)
	withProcesses(t, map[int]string{4242: "start-1"})
	fx.set(t, running(fx.now))
	if r := RunningForWorktree(fx.rig, "/w", fx.now.Add(time.Minute)); !strings.Contains(r, src) {
		t.Fatalf("running marker not reported: %q", r)
	}
	if r := RunningForWorktree(fx.rig, "/other", fx.now); r != "" {
		t.Fatalf("other worktree reported: %q", r)
	}
	if r := RunningForWorktree(fx.rig, "/w", fx.now.Add(LostAfter+time.Minute)); r != "" {
		t.Fatalf("lost marker still holds the sandbox: %q", r)
	}
}

// runFixture drives Run with a fake git and preflight.
type runFixture struct {
	t        *testing.T
	worktree string
	path     string
	heads    []string // successive rev-parse HEAD answers
	dirty    string
	receipt  map[string]any
	exit     int
	execs    int
}

func (r *runFixture) deps() RunDeps {
	return RunDeps{
		Git: func(dir string, args ...string) (string, error) {
			if dir != r.worktree {
				return "", fmt.Errorf("git in %s", dir)
			}
			switch args[0] {
			case "rev-parse":
				h := r.heads[0]
				if len(r.heads) > 1 {
					r.heads = r.heads[1:]
				}
				return h, nil
			case "status":
				return r.dirty, nil
			}
			return "", fmt.Errorf("unexpected git %v", args)
		},
		Exec: func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
			r.execs++
			fmt.Fprintln(log, "fake preflight", strings.Join(argv, " "))
			if r.receipt != nil {
				m, _ := Load(r.path)
				p := filepath.Join(dir, m.ReceiptPath)
				_ = os.MkdirAll(filepath.Dir(p), 0o755)
				raw, _ := json.Marshal(r.receipt)
				_ = os.WriteFile(p, raw, 0o644)
			}
			return r.exit, nil
		},
		Now:       time.Now,
		Heartbeat: 5 * time.Millisecond,
	}
}

func newRunFixture(t *testing.T) *runFixture {
	t.Helper()
	rig, worktree := t.TempDir(), t.TempDir()
	_, path, _, err := Prepare(request(rig, worktree, time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	return &runFixture{t: t, worktree: worktree, path: path, heads: []string{headA}}
}

func TestRunOutcomes(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(*runFixture)
		status    string
		receipt   string
		wantExecs int
		reason    string
	}{
		{"passed", func(r *runFixture) {
			r.receipt = map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed"}
		}, StatusPassed, "passed", 1, ""},
		{"failed receipt", func(r *runFixture) {
			r.exit = 1
			r.receipt = map[string]any{"head_sha": headA, "source_bead_id": src, "status": "failed", "errors": []string{"api-tests: 2 failed"}}
		}, StatusFailed, "failed", 1, "api-tests: 2 failed"},
		{"light receipt never passes", func(r *runFixture) {
			r.receipt = map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed_light"}
		}, StatusFailed, "passed_light", 1, "passed_light"},
		{"no receipt", func(r *runFixture) { r.exit = 2 }, StatusError, "", 1, "without a receipt"},
		{"receipt for another head", func(r *runFixture) {
			r.receipt = map[string]any{"head_sha": headB, "source_bead_id": src, "status": "passed"}
		}, StatusError, "passed", 1, "receipt binds"},
		{"head moved before start", func(r *runFixture) { r.heads = []string{headB} }, StatusError, "", 0, "not the submitted head"},
		{"dirty before start", func(r *runFixture) { r.dirty = " M apps/api/x.py" }, StatusError, "", 0, "tracked changes"},
		{"head moved during preflight", func(r *runFixture) {
			r.heads = []string{headA, headB}
			r.receipt = map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed"}
		}, StatusError, "", 1, "moved"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRunFixture(t)
			tc.setup(r)
			if err := Run(context.Background(), r.path, r.deps()); err != nil {
				t.Fatal(err)
			}
			m, err := Load(r.path)
			if err != nil {
				t.Fatal(err)
			}
			if m.Status != tc.status || m.ReceiptStatus != tc.receipt || r.execs != tc.wantExecs || !strings.Contains(m.Reason, tc.reason) {
				t.Fatalf("marker %+v execs=%d", m, r.execs)
			}
			if m.FinishedAt == "" || m.PID != os.Getpid() {
				t.Fatalf("finish not recorded: %+v", m)
			}
			if tc.status == StatusPassed {
				if g := Evaluate(filepath.Dir(filepath.Dir(filepath.Dir(r.path))), MRFields{Source: src, CommitSHA: headA, RunPath: r.path}, headA, time.Now()); g.State != GatePassed {
					t.Fatalf("gate after pass: %+v", g)
				}
			}
		})
	}
}

func TestRunRefusesSecondRunner(t *testing.T) {
	r := newRunFixture(t)
	r.receipt = map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed"}
	if err := Run(context.Background(), r.path, r.deps()); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), r.path, r.deps()); err == nil {
		t.Fatal("second runner accepted")
	}
	if r.execs != 1 {
		t.Fatalf("execs = %d", r.execs)
	}
}

func TestRunStoppedRunnerRecordsError(t *testing.T) {
	r := newRunFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	deps := r.deps()
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		cancel()
		return -1, nil
	}
	if err := Run(ctx, r.path, deps); err != nil {
		t.Fatal(err)
	}
	if m, _ := Load(r.path); m.Status != StatusError || !strings.Contains(m.Reason, "stopped") {
		t.Fatalf("%+v", m)
	}
}

func TestExecInGroup(t *testing.T) {
	var out strings.Builder
	code, err := ExecInGroup(context.Background(), t.TempDir(), []string{"sh", "-c", "echo hi; exit 3"}, &out)
	if err != nil || code != 3 || !strings.Contains(out.String(), "hi") {
		t.Fatalf("code=%d err=%v out=%q", code, err, out.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	code, _ = ExecInGroup(ctx, t.TempDir(), []string{"sh", "-c", "sleep 30"}, io.Discard)
	if time.Since(start) > 10*time.Second || code == 0 {
		t.Fatalf("cancel did not stop the group: code=%d after %s", code, time.Since(start))
	}
}

func TestRunTimeoutRecordsFailedReceipt(t *testing.T) {
	r := newRunFixture(t)
	deps := r.deps()
	deps.Timeout = 50 * time.Millisecond
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		<-ctx.Done() // a hung preflight
		return -1, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusFailed || m.ReceiptStatus != ReceiptTimedOut || !strings.Contains(m.Reason, "timeout") {
		t.Fatalf("%+v", m)
	}
	rc, err := ReadReceipt(m.ReceiptCopy)
	if err != nil || rc.Status != ReceiptTimedOut || rc.Head != headA {
		t.Fatalf("receipt %+v %v", rc, err)
	}
	rig := filepath.Dir(filepath.Dir(filepath.Dir(r.path)))
	if g := Evaluate(rig, MRFields{Source: src, CommitSHA: headA, RunPath: r.path}, "", time.Now()); g.State != GateFailed {
		t.Fatalf("gate %+v", g)
	}
}

func TestRunRecordsRunnerIdentity(t *testing.T) {
	r := newRunFixture(t)
	r.receipt = map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed"}
	if err := Run(context.Background(), r.path, r.deps()); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.PIDStart == "" || m.PIDStart != processStartTime(os.Getpid()) || m.Timeout() != DefaultTimeout {
		t.Fatalf("identity/timeout not recorded: %+v", m)
	}
}

func TestProcessStartTimeAndStop(t *testing.T) {
	if processStartTime(os.Getpid()) == "" || processStartTime(os.Getpid()) != processStartTime(os.Getpid()) {
		t.Fatal("own start time unavailable or unstable")
	}
	c := exec.Command("sleep", "30")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	m := &Marker{Status: StatusRunning, PID: c.Process.Pid, PIDStart: "not-this-process"}
	if Stop(m) {
		t.Fatal("stopped a process whose start time does not match (reused PID)")
	}
	m.PIDStart = processStartTime(c.Process.Pid)
	if !Stop(m) {
		t.Fatal("did not stop the recorded runner")
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = c.Process.Kill()
		t.Fatal("SIGTERM not delivered")
	}
	if processStartTime(c.Process.Pid) != "" && processStartTime(c.Process.Pid) == m.PIDStart {
		t.Fatal("exited process still reported")
	}
}

func TestPrune(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	mk := func(t *testing.T, rig, head string, created time.Time, edit func(*Marker), mr string) string {
		t.Helper()
		req := request(rig, "/w", created)
		req.Head = head
		m, p, _, err := Prepare(req)
		if err != nil {
			t.Fatal(err)
		}
		if edit != nil {
			edit(m)
			_ = Save(p, m)
		}
		_ = os.WriteFile(m.Log, []byte("log"), 0o644)
		if mr != "" {
			_ = RecordMR(p, mr)
		}
		return p
	}
	finished := func(m *Marker) { m.Status = StatusPassed }
	t.Run("old finished marker removed with its files", func(t *testing.T) {
		withDeadPIDs(t)
		rig := t.TempDir()
		p := mk(t, rig, headA, now.Add(-8*24*time.Hour), finished, "")
		fresh := mk(t, rig, headB, now.Add(-time.Hour), finished, "")
		res := Prune(rig, now, nil)
		if len(res.Removed) != 1 || res.Removed[0] != p {
			t.Fatalf("%+v", res)
		}
		for _, f := range []string{p, LogPath(p)} {
			if _, err := os.Stat(f); err == nil {
				t.Fatalf("%s kept", f)
			}
		}
		if _, err := os.Stat(fresh); err != nil {
			t.Fatal("fresh marker removed")
		}
	})
	t.Run("closed MR marker removed; unknown MR kept", func(t *testing.T) {
		withDeadPIDs(t)
		rig := t.TempDir()
		closed := mk(t, rig, headA, now.Add(-time.Hour), finished, "kasb-wisp-c")
		unknown := mk(t, rig, headB, now.Add(-time.Hour), finished, "kasb-wisp-u")
		res := Prune(rig, now, func(id string) MRState {
			if id == "kasb-wisp-c" {
				return MRState{Known: true, Closed: true}
			}
			return MRState{}
		})
		if len(res.Removed) != 1 || res.Removed[0] != closed {
			t.Fatalf("%+v", res)
		}
		if _, err := os.Stat(MRSidecarPath(closed)); err == nil {
			t.Fatal("sidecar kept")
		}
		if _, err := os.Stat(unknown); err != nil {
			t.Fatal("unknown-MR marker removed")
		}
	})
	t.Run("live runner superseded by a moved MR head is stopped, not removed", func(t *testing.T) {
		withProcesses(t, map[int]string{4242: "start-1"})
		rig := t.TempDir()
		live := mk(t, rig, headA, now.Add(-time.Minute), running(now.Add(-time.Minute)), "kasb-wisp-m")
		res := Prune(rig, now, func(string) MRState { return MRState{Known: true, Head: headB} })
		// Stop signals the fake PID 4242, which is not ours: Kill fails, so
		// nothing is reported stopped and the live marker is kept.
		if len(res.Removed) != 0 {
			t.Fatalf("live marker removed: %+v", res)
		}
		if _, err := os.Stat(live); err != nil {
			t.Fatal(err)
		}
	})
}
