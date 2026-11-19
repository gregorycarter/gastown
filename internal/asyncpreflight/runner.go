package asyncpreflight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ReceiptTimedOut is the receipt status recorded when the hard timeout fires.
const ReceiptTimedOut = "failed_timeout"

// RunDeps are the runner's side effects, injectable for tests.
type RunDeps struct {
	// Git runs git in dir and returns trimmed stdout.
	Git func(dir string, args ...string) (string, error)
	// Exec runs argv in dir with output to log and returns its exit code.
	// It must stop the command when ctx is cancelled.
	Exec func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error)
	Now  func() time.Time
	// Heartbeat overrides HeartbeatInterval (tests).
	Heartbeat time.Duration
	// Timeout overrides the marker's hard timeout (tests).
	Timeout time.Duration
}

// DefaultRunDeps uses the real git and process execution.
func DefaultRunDeps() RunDeps {
	return RunDeps{
		Git: func(dir string, args ...string) (string, error) {
			c := exec.Command("git", args...)
			c.Dir = dir
			out, err := c.Output()
			return strings.TrimSpace(string(out)), err
		},
		Exec: ExecInGroup,
		Now:  time.Now,
	}
}

// ExecInGroup runs argv in its own process group and kills the group when
// ctx is cancelled, so a stopped runner never leaves test workers behind.
func ExecInGroup(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
	if len(argv) == 0 {
		return -1, fmt.Errorf("empty command")
	}
	c := exec.Command(argv[0], argv[1:]...)
	c.Dir = dir
	c.Stdout, c.Stderr = log, log
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := c.Start(); err != nil {
		return -1, err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = syscall.Kill(-c.Process.Pid, syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			}
		case <-done:
		}
	}()
	err := c.Wait()
	close(done)
	if err == nil {
		return 0, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), nil
	}
	return -1, err
}

// Run executes the preflight recorded in markerPath and records the outcome.
// It returns an error only when the marker itself cannot be used; preflight
// failures are recorded in the marker.
func Run(ctx context.Context, markerPath string, deps RunDeps) error {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	interval := deps.Heartbeat
	if interval <= 0 {
		interval = HeartbeatInterval
	}
	m, err := Load(markerPath)
	if err != nil {
		return err
	}
	if m.Status != StatusStarting {
		return fmt.Errorf("marker %s is %s, not %s; refusing a second runner", markerPath, m.Status, StatusStarting)
	}
	var mu sync.Mutex
	save := func(mutate func(*Marker)) {
		mu.Lock()
		defer mu.Unlock()
		mutate(m)
		_ = Save(markerPath, m)
	}
	stamp := func() string { return deps.Now().UTC().Format(time.RFC3339Nano) }
	finish := func(status, reason string, exit *int, receiptStatus string) error {
		save(func(m *Marker) {
			m.Status, m.Reason, m.ExitCode, m.ReceiptStatus = status, truncate(reason, 1500), exit, receiptStatus
			m.FinishedAt, m.HeartbeatAt = stamp(), stamp()
		})
		return nil
	}
	save(func(m *Marker) {
		m.Status, m.PID, m.PIDStart, m.StartedAt, m.HeartbeatAt = StatusRunning, os.Getpid(), ProcessStartTime(os.Getpid()), stamp(), stamp()
	})

	if head, err := deps.Git(m.Worktree, "rev-parse", "HEAD"); err != nil || head != m.Head {
		return finish(StatusError, fmt.Sprintf("worktree %s is at %q, not the submitted head %s (%v)", m.Worktree, head, m.Head, err), nil, "")
	}
	if dirty, err := deps.Git(m.Worktree, "status", "--porcelain", "--untracked-files=no"); err != nil || dirty != "" {
		return finish(StatusError, fmt.Sprintf("worktree %s has tracked changes before the preflight: %s %v", m.Worktree, truncate(dirty, 300), err), nil, "")
	}

	receiptAbs := filepath.Join(m.Worktree, m.ReceiptPath)
	_ = os.Remove(receiptAbs) // never read a receipt from an earlier attempt

	logFile, err := os.OpenFile(m.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return finish(StatusError, fmt.Sprintf("cannot open log %s: %v", m.Log, err), nil, "")
	}
	defer logFile.Close()
	fmt.Fprintf(logFile, "%s async preflight %s@%s in %s: %s\n", stamp(), m.Source, m.Head, m.Worktree, strings.Join(m.Command, " "))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				save(func(m *Marker) { m.HeartbeatAt = stamp() })
			}
		}
	}()
	timeout := deps.Timeout
	if timeout <= 0 {
		timeout = m.Timeout()
	}
	runCtx, cancelRun := context.WithTimeout(ctx, timeout)
	code, execErr := deps.Exec(runCtx, m.Worktree, m.Command, logFile)
	timedOut := ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded)
	cancelRun()
	close(stop)
	wg.Wait()
	fmt.Fprintf(logFile, "%s preflight exited %d (%v)\n", stamp(), code, execErr)
	exit := code

	if ctx.Err() != nil {
		return finish(StatusError, "async preflight runner was stopped before the preflight finished", &exit, "")
	}
	if timedOut {
		reason := fmt.Sprintf("preflight exceeded its %s timeout and was killed", timeout)
		raw, _ := json.Marshal(map[string]string{"head_sha": m.Head, "source_bead_id": m.Source, "status": ReceiptTimedOut, "reason": reason})
		_ = writeFileAtomic(m.ReceiptCopy, raw)
		return finish(StatusFailed, reason, &exit, ReceiptTimedOut)
	}
	if execErr != nil {
		return finish(StatusError, fmt.Sprintf("preflight could not run: %v", execErr), &exit, "")
	}
	if head, err := deps.Git(m.Worktree, "rev-parse", "HEAD"); err != nil || head != m.Head {
		return finish(StatusError, fmt.Sprintf("worktree head moved to %q during the preflight; the receipt does not prove %s", head, m.Head), &exit, "")
	}
	raw, err := os.ReadFile(receiptAbs)
	if err != nil {
		return finish(StatusError, fmt.Sprintf("preflight exited %d without a receipt at %s", code, m.ReceiptPath), &exit, "")
	}
	var r struct {
		Receipt
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return finish(StatusError, fmt.Sprintf("receipt %s is not JSON: %v", m.ReceiptPath, err), &exit, "")
	}
	if r.Head != m.Head || r.Source != m.Source {
		return finish(StatusError, fmt.Sprintf("receipt binds %s@%s, not %s@%s", r.Source, short(r.Head), m.Source, short(m.Head)), &exit, r.Status)
	}
	if err := writeFileAtomic(m.ReceiptCopy, raw); err != nil {
		return finish(StatusError, fmt.Sprintf("cannot keep receipt copy %s: %v", m.ReceiptCopy, err), &exit, r.Status)
	}
	if code == 0 && Passing(r.Status) {
		return finish(StatusPassed, "", &exit, r.Status)
	}
	var reasons []string
	for i, e := range r.Errors {
		if i == 3 {
			break
		}
		reasons = append(reasons, strings.TrimSpace(string(e)))
	}
	reason := fmt.Sprintf("receipt status %s, exit %d", r.Status, code)
	if len(reasons) > 0 {
		reason += ": " + strings.Join(reasons, "; ")
	}
	return finish(StatusFailed, reason, &exit, r.Status)
}

func writeFileAtomic(path string, raw []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
