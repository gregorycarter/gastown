package asyncpreflight

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
)

// recheckExec is a fake preflight whose attempts follow steps: each step
// may write a receipt, cancel the runner's stop context, and exit.
type recheckStep struct {
	receipt map[string]any
	exit    int
	stop    bool // the host stops the runner during this attempt
	dirty   string
}

func recheckDeps(t *testing.T, r *runFixture, steps []recheckStep, cancel context.CancelCauseFunc, argvs *[][]string) RunDeps {
	t.Helper()
	deps := r.deps()
	inner := deps.Exec
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		*argvs = append(*argvs, append([]string(nil), argv...))
		step := steps[len(*argvs)-1]
		r.receipt, r.exit, r.dirty = step.receipt, step.exit, step.dirty
		code, err := inner(ctx, dir, argv, log)
		if step.stop {
			cancel(errors.New("terminated signal received"))
			<-ctx.Done() // ExecInGroup returns only after the group is gone
		}
		return code, err
	}
	deps.Pressure = func() string { return "loadavg 41.2 38.0 30.5, memory free 7%" }
	deps.Signals = func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }
	return deps
}

func recheckPassed() map[string]any {
	return map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed"}
}

func hasRecheck(argv []string) bool { return slices.Contains(argv, "--recheck-environment") }

func TestRunRerunsOnceWithRecheckEnvironmentAfterAKillWithoutReceipt(t *testing.T) {
	r := newRunFixture(t)
	var argvs [][]string
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	deps := recheckDeps(t, r, []recheckStep{{exit: 1}, {receipt: recheckPassed()}}, cancel, &argvs)
	if err := Run(ctx, r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusPassed || len(argvs) != 2 {
		t.Fatalf("marker %+v after %d attempts", m, len(argvs))
	}
	if hasRecheck(argvs[0]) || !hasRecheck(argvs[1]) || !slices.Equal(argvs[1][:len(argvs[0])], argvs[0]) {
		t.Fatalf("rerun must repeat the same command plus --recheck-environment: %q", argvs)
	}
	if hasRecheck(m.Command) {
		t.Fatalf("the recorded command changed: %q", m.Command)
	}
	if !strings.Contains(m.RecheckedEnvironment, "exited 1 without a receipt") || !strings.Contains(m.RecheckedEnvironment, "loadavg 41.2") {
		t.Fatalf("infrastructure death not recorded with host pressure: %q", m.RecheckedEnvironment)
	}
	log, _ := os.ReadFile(m.Log)
	if !strings.Contains(string(log), "rerunning once on") {
		t.Fatalf("log does not record the rerun:\n%s", log)
	}
}

func TestRunSecondInfrastructureDeathStaysAnError(t *testing.T) {
	r := newRunFixture(t)
	var argvs [][]string
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	deps := recheckDeps(t, r, []recheckStep{{exit: 143}, {exit: 143}}, cancel, &argvs)
	if err := Run(ctx, r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusError || len(argvs) != 2 || !strings.Contains(m.Reason, "rerun also died") || !strings.Contains(m.Reason, "memory free 7%") {
		t.Fatalf("second death: marker %+v after %d attempts", m, len(argvs))
	}
}

func TestRunNeverRerunsAProductFailure(t *testing.T) {
	r := newRunFixture(t)
	var argvs [][]string
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	failed := map[string]any{"head_sha": headA, "source_bead_id": src, "status": "failed", "errors": []string{"api-tests: 1 failed"}}
	deps := recheckDeps(t, r, []recheckStep{{receipt: failed, exit: 1}}, cancel, &argvs)
	if err := Run(ctx, r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusFailed || len(argvs) != 1 || m.RecheckedEnvironment != "" {
		t.Fatalf("product failure rerun: marker %+v after %d attempts", m, len(argvs))
	}
}

func TestRunDoesNotRerunWhenTheWorktreeChangedAfterTheKill(t *testing.T) {
	r := newRunFixture(t)
	var argvs [][]string
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	deps := recheckDeps(t, r, []recheckStep{{exit: 1, dirty: " M apps/api/x.py"}}, cancel, &argvs)
	if err := Run(ctx, r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusError || len(argvs) != 1 || !strings.Contains(m.Reason, "without a receipt") {
		t.Fatalf("dirty worktree rerun: marker %+v after %d attempts", m, len(argvs))
	}
}

func TestRunRerunsOnceAfterTheHostStopsTheRunner(t *testing.T) {
	r := newRunFixture(t)
	var argvs [][]string
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	deps := recheckDeps(t, r, []recheckStep{{exit: 143, stop: true}, {receipt: recheckPassed()}}, cancel, &argvs)
	if err := Run(ctx, r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusPassed || len(argvs) != 2 || !hasRecheck(argvs[1]) {
		t.Fatalf("host stop: marker %+v after %d attempts", m, len(argvs))
	}
	if !strings.Contains(m.RecheckedEnvironment, "runner was stopped") || !strings.Contains(m.RecheckedEnvironment, "terminated signal received") {
		t.Fatalf("the killing signal was not recorded: %q", m.RecheckedEnvironment)
	}
}

func TestRunDoesNotRerunWhenGtStoppedTheRunner(t *testing.T) {
	r := newRunFixture(t)
	var argvs [][]string
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	deps := recheckDeps(t, r, []recheckStep{{exit: 143, stop: true}}, cancel, &argvs)
	exec := deps.Exec
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		// prune or Retire marks its stop before signalling the runner.
		if err := os.WriteFile(StopRequestPath(r.path), []byte("now\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return exec(ctx, dir, argv, log)
	}
	if err := Run(ctx, r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusError || len(argvs) != 1 || !strings.Contains(m.Reason, "stop requested by gt") {
		t.Fatalf("deliberate stop rerun: marker %+v after %d attempts", m, len(argvs))
	}
}

func TestExecInGroupLogsTheKillingSignal(t *testing.T) {
	var log bytes.Buffer
	code, err := ExecInGroup(context.Background(), t.TempDir(), []string{"sh", "-c", "kill -KILL $$"}, &log)
	if err != nil || code != -1 || !strings.Contains(log.String(), "terminated by signal killed") {
		t.Fatalf("code=%d err=%v log=%q", code, err, log.String())
	}
	log.Reset()
	code, err = ExecInGroup(context.Background(), t.TempDir(), []string{"sh", "-c", "exit 143"}, &log)
	if err != nil || code != 143 || !strings.Contains(log.String(), "death by signal terminated") {
		t.Fatalf("code=%d err=%v log=%q", code, err, log.String())
	}
}
