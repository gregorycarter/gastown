package asyncpreflight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ReceiptTimedOut is the receipt status recorded when the hard timeout fires.
const ReceiptTimedOut = "failed_timeout"

// ReceiptLeaseWaitExhausted is the receipt status recorded when a preflight
// waited out the queue cap for a heavy-work lease and never acquired one. It is
// lease starvation, not a code failure, so it is never failed_timeout
// (kasb-9oupz.70).
const ReceiptLeaseWaitExhausted = "lease_wait_exhausted"

// errRunTimeout and errQueueCap are the causes the heartbeat uses to stop the
// preflight's process group, so the outcome is classified without racing the
// context's own error (kasb-9oupz.70).
var (
	errRunTimeout = errors.New("run timeout exceeded")
	errQueueCap   = errors.New("lease-wait cap exceeded")
)

// DefaultBackstopMargin is the slack above the queue cap plus run timeout for the
// outer context deadline, so a stalled or panicked heartbeat can never leave a
// preflight running forever; in normal operation the heartbeat always fires
// first and this is pure backstop (kasb-9oupz.70).
const DefaultBackstopMargin = 5 * time.Minute

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
	// QueueCap overrides the marker's heavy-work lease-wait cap (tests).
	QueueCap time.Duration
	// BackstopMargin overrides the outer-deadline slack (tests).
	BackstopMargin time.Duration
	// Pressure describes the host's load when a preflight dies (tests override it).
	Pressure func() string
	// Signals returns a fresh stop context for the one rerun after the host
	// stopped the runner (default: SIGTERM, SIGINT and SIGHUP).
	Signals func() (context.Context, context.CancelFunc)
}

// RecheckEnvironmentReason is passed to the one rerun of a preflight whose
// process group died without writing a receipt (hq-y1q7ew).
const RecheckEnvironmentReason = "async preflight process group died without a receipt; rerun once on the same head (hq-y1q7ew)"

// DefaultRunDeps uses the real git and process execution.
func DefaultRunDeps() RunDeps {
	return RunDeps{
		Git: func(dir string, args ...string) (string, error) {
			c := exec.Command("git", args...)
			c.Dir = dir
			out, err := c.Output()
			return strings.TrimSpace(string(out)), err
		},
		Exec:     ExecInGroup,
		Now:      time.Now,
		Pressure: HostPressure,
		Signals: func() (context.Context, context.CancelFunc) {
			return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
		},
	}
}

// HostPressure reports the load average and, where the host exposes it, the
// memory level, so an infrastructure death records what the host was doing.
func HostPressure() string {
	var parts []string
	if raw, err := os.ReadFile("/proc/loadavg"); err == nil {
		parts = append(parts, "loadavg "+strings.Join(strings.Fields(string(raw))[:3], " "))
	} else if out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output(); err == nil {
		parts = append(parts, "loadavg "+strings.Trim(strings.TrimSpace(string(out)), "{} "))
	}
	if out, err := exec.Command("sysctl", "-n", "kern.memorystatus_level").Output(); err == nil {
		parts = append(parts, "memory free "+strings.TrimSpace(string(out))+"%")
	}
	if len(parts) == 0 {
		return "host pressure unavailable"
	}
	return strings.Join(parts, ", ")
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
		// hq-y1q7ew: name the signal that ended the group, to find what kills it.
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			fmt.Fprintf(log, "preflight process group leader was terminated by signal %s\n", status.Signal())
		} else if code := exitErr.ExitCode(); code > 128 && code < 128+65 {
			fmt.Fprintf(log, "preflight exited %d: a child reported death by signal %s\n", code, syscall.Signal(code-128))
		}
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
	_ = os.Remove(StopRequestPath(markerPath)) // a request belongs to an earlier run
	save(func(m *Marker) {
		m.Status, m.PID, m.PIDStart, m.StartedAt, m.HeartbeatAt = StatusRunning, os.Getpid(), ProcessStartTime(os.Getpid()), stamp(), stamp()
	})

	if head, err := deps.Git(m.Worktree, "rev-parse", "HEAD"); err != nil || head != m.Head {
		return finish(StatusError, fmt.Sprintf("worktree %s is at %q, not the submitted head %s (%v)", m.Worktree, head, m.Head, err), nil, "")
	}
	if dirty, err := deps.Git(m.Worktree, "status", "--porcelain", "--untracked-files=no"); err != nil || dirty != "" {
		return finish(StatusError, fmt.Sprintf("worktree %s has tracked changes before the preflight: %s %v", m.Worktree, truncate(dirty, 300), err), nil, "")
	}

	// Immutable run identity, read by the heartbeat without the marker lock.
	ident := runIdentity{Source: m.Source, Head: m.Head, Worktree: m.Worktree, CreatedAt: m.CreatedAt}
	receiptAbs := filepath.Join(m.Worktree, m.ReceiptPath)
	stopRerunSignals := func() {}
	defer func() { stopRerunSignals() }()
	logFile, err := os.OpenFile(m.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return finish(StatusError, fmt.Sprintf("cannot open log %s: %v", m.Log, err), nil, "")
	}
	defer logFile.Close()
	timeout := deps.Timeout
	if timeout <= 0 {
		timeout = m.Timeout()
	}
	queueCap := deps.QueueCap
	if queueCap <= 0 {
		queueCap = m.QueueCap()
	}
	backstopMargin := deps.BackstopMargin
	if backstopMargin <= 0 {
		backstopMargin = DefaultBackstopMargin
	}
	// kasb-9oupz.70: tell the preflight where to report its heavy-work lease
	// state. This runner process serves exactly one marker, so setting it in the
	// environment is safe. A preflight that does not write the sentinel (an older
	// one, or a command that takes no lease) leaves the timeout measured from
	// submission, exactly as before.
	_ = os.Remove(LeaseSentinelPath(markerPath)) // a sentinel belongs to an earlier run
	_ = os.Setenv(LeaseSentinelEnv, LeaseSentinelPath(markerPath))

	pressure := func() string {
		if deps.Pressure == nil {
			return "host pressure unavailable"
		}
		return deps.Pressure()
	}
	runParent := ctx
	command := m.Command
	attempt := 1
	// rerunnable prepares the one rerun after an infrastructure death: the
	// preflight recorded no product failure (no receipt), so rerun it on the
	// same, unchanged head with --recheck-environment (hq-y1q7ew). It reports
	// false when the worktree no longer holds that head cleanly.
	rerunnable := func(reason string) bool {
		if head, err := deps.Git(m.Worktree, "rev-parse", "HEAD"); err != nil || head != m.Head {
			return false
		}
		if dirty, err := deps.Git(m.Worktree, "status", "--porcelain", "--untracked-files=no"); err != nil || dirty != "" {
			return false
		}
		if runParent.Err() != nil {
			if deps.Signals == nil {
				return false
			}
			runParent, stopRerunSignals = deps.Signals()
		}
		fmt.Fprintf(logFile, "%s %s; rerunning once on %s with --recheck-environment\n", stamp(), reason, short(m.Head))
		_ = os.Remove(LeaseSentinelPath(markerPath)) // the rerun re-acquires its own lease (kasb-9oupz.70)
		save(func(m *Marker) {
			m.RecheckedEnvironment = truncate(reason, 1500)
			m.StartedAt, m.HeartbeatAt = stamp(), stamp() // the rerun gets its own timeout
			m.LeaseAcquiredAt, m.LeaseWaiting = "", false
		})
		command = withRecheckEnvironment(m.Command)
		attempt++
		return true
	}
	var raw []byte
	var code int
	var exit int
	for {
		_ = os.Remove(receiptAbs) // never read a receipt from an earlier attempt
		// Filesystem times can be coarser than the clock; allow a second of slack.
		runStart := deps.Now().Add(-time.Second)
		fmt.Fprintf(logFile, "%s async preflight %s@%s in %s: %s\n", stamp(), m.Source, m.Head, m.Worktree, strings.Join(command, " "))

		// kasb-84hoc: the heartbeat watches for supersession and stops the
		// preflight's process group at once, instead of finishing a run whose
		// receipt cannot prove the head (~420 wall-min a day were lost this way).
		// kasb-9oupz.70: it also enforces the hard timeout, measured from lease
		// acquisition (the sentinel the preflight writes), with a separate
		// generous cap on the lease wait alone that reports lease starvation, not
		// failed_timeout. An outer deadline (queue cap + run timeout + margin) is
		// the backstop: if the heartbeat stalls or panics the preflight is still
		// killed, never left running forever.
		outerCtx, cancelOuter := context.WithTimeout(runParent, queueCap+timeout+backstopMargin)
		execCtx, cancelExec := context.WithCancelCause(outerCtx)
		var superseded string   // guarded by mu
		var timedOutKind string // "", "run" or "queue"; guarded by mu
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(interval)
			defer t.Stop()
			var leaseAt time.Time // acquisition time; zero until the lease is acquired
			waiting := false      // the preflight reported it is queued for a lease
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					save(func(m *Marker) { m.HeartbeatAt = stamp() })
					if leaseAt.IsZero() {
						if s, err := ReadLeaseSentinel(markerPath); err == nil && s != nil {
							switch s.Phase {
							case LeasePhaseAcquired:
								if at, perr := time.Parse(time.RFC3339Nano, s.At); perr == nil {
									leaseAt = at
								} else {
									leaseAt = deps.Now()
								}
								acquired := leaseAt.UTC().Format(time.RFC3339Nano)
								save(func(m *Marker) { m.LeaseAcquiredAt, m.LeaseWaiting = acquired, false })
								fmt.Fprintf(logFile, "%s heavy-work lease acquired after %.0fs; the %s run timeout starts now (kasb-9oupz.70)\n", stamp(), s.QueueSeconds, timeout)
							case LeasePhaseWaiting:
								if !waiting {
									waiting = true
									save(func(m *Marker) { m.LeaseWaiting = true })
								}
							}
						}
					}
					now := deps.Now()
					switch {
					case !leaseAt.IsZero():
						if now.Sub(leaseAt) > timeout {
							mu.Lock()
							timedOutKind = "run"
							mu.Unlock()
							fmt.Fprintf(logFile, "%s preflight exceeded its %s run timeout after the lease; stopping the process group\n", stamp(), timeout)
							cancelExec(errRunTimeout)
							return
						}
					case waiting:
						if now.Sub(runStart) > queueCap {
							mu.Lock()
							timedOutKind = "queue"
							mu.Unlock()
							fmt.Fprintf(logFile, "%s preflight waited the full %s for a heavy-work lease; stopping the process group (lease starvation, not a code timeout)\n", stamp(), queueCap)
							cancelExec(errQueueCap)
							return
						}
					default:
						// No lease report (older preflight, or no lease taken):
						// measure from submission, exactly as before.
						if now.Sub(runStart) > timeout {
							mu.Lock()
							timedOutKind = "run"
							mu.Unlock()
							fmt.Fprintf(logFile, "%s preflight exceeded its %s timeout; stopping the process group\n", stamp(), timeout)
							cancelExec(errRunTimeout)
							return
						}
					}
					if why := supersession(deps, markerPath, ident); why != "" {
						mu.Lock()
						superseded = why
						mu.Unlock()
						fmt.Fprintf(logFile, "%s %s; stopping the preflight process group\n", stamp(), why)
						cancelExec(errors.New(why))
						return
					}
				}
			}
		}()
		var execErr error
		code, execErr = deps.Exec(execCtx, m.Worktree, command, logFile)
		// The outer deadline fired without the heartbeat classifying the kill:
		// the heartbeat stalled. Classify it as a backstop timeout below.
		backstop := runParent.Err() == nil && errors.Is(outerCtx.Err(), context.DeadlineExceeded)
		cancelExec(nil)
		cancelOuter()
		close(stop)
		wg.Wait()
		fmt.Fprintf(logFile, "%s preflight exited %d (%v)\n", stamp(), code, execErr)
		exit = code

		mu.Lock()
		supersededReason := superseded
		tko := timedOutKind
		mu.Unlock()
		if supersededReason != "" {
			// Never rerun: no receipt for this head can help any more.
			return finish(StatusError, supersededReason, &exit, "")
		}
		if runParent.Err() != nil {
			reason := fmt.Sprintf("async preflight runner was stopped before the preflight finished (%v; %s)", context.Cause(runParent), pressure())
			if _, statErr := os.Stat(StopRequestPath(markerPath)); statErr == nil {
				// gt asked this runner to stop (a superseded or retired run): never rerun.
				return finish(StatusError, reason+"; stop requested by gt", &exit, "")
			}
			if attempt > 1 {
				return finish(StatusError, reason+"; the one --recheck-environment rerun was also stopped", &exit, "")
			}
			if !rerunnable(reason) {
				return finish(StatusError, reason, &exit, "")
			}
			continue
		}
		if tko == "run" {
			reason := fmt.Sprintf("preflight exceeded its %s timeout and was killed", timeout)
			raw, _ := json.Marshal(map[string]string{"head_sha": m.Head, "source_bead_id": m.Source, "status": ReceiptTimedOut, "reason": reason})
			_ = writeFileAtomic(m.ReceiptCopy, raw)
			return finish(StatusFailed, reason, &exit, ReceiptTimedOut)
		}
		if tko == "queue" {
			// Lease starvation: never failed_timeout. Reported as an error with a
			// distinct status so the lander does not bounce the MR to its worker as
			// a code failure; the repeated-pending cap is kasb-9oupz.63.2.
			reason := fmt.Sprintf("preflight waited the full %s for a heavy-work lease and never acquired one (lease starvation; %s); not a code failure", queueCap, pressure())
			raw, _ := json.Marshal(map[string]string{"head_sha": m.Head, "source_bead_id": m.Source, "status": ReceiptLeaseWaitExhausted, "reason": reason})
			_ = writeFileAtomic(m.ReceiptCopy, raw)
			return finish(StatusError, reason, &exit, ReceiptLeaseWaitExhausted)
		}
		if backstop {
			// The heartbeat did not classify the kill (stalled or panicked), but
			// the outer deadline stopped the preflight. Record a failed_timeout so
			// the run never lands silently; name the backstop for diagnosis.
			reason := fmt.Sprintf("preflight exceeded the absolute cap (%s) and was killed by the backstop; the heartbeat did not classify it (%s)", queueCap+timeout+backstopMargin, pressure())
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
		raw, err = os.ReadFile(receiptAbs)
		if err != nil && code == 0 {
			// hq-4gmjjb: an infrastructure-only head takes preflight's
			// infrastructure route, which records its proof under
			// .delivery/infrastructure-receipts/ and ignores --receipt.
			if adopted, ok := adoptInfrastructureReceipt(m, runStart); ok {
				raw, err = adopted, nil
				_ = os.MkdirAll(filepath.Dir(receiptAbs), 0o755)
				_ = writeFileAtomic(receiptAbs, raw) // the requested path now holds the proof too
				fmt.Fprintf(logFile, "%s adopted the infrastructure receipt for %s@%s as the async receipt\n", stamp(), m.Source, short(m.Head))
			}
		}
		if err == nil {
			// kasb-9oupz.71: a receipt whose failures are entirely environmental
			// (failures_all_environmental, set by preflight) is not a product
			// failure. Rerun once on the same head, as for an infra death, keeping
			// the MR and its staging approval; the gate sees a failure only if the
			// rerun also fails. A missing field (an older preflight) means no rerun.
			if attempt == 1 && receiptAllEnvironmental(raw) {
				_ = writeFileAtomic(sibling(markerPath, ".receipt-environmental.json"), raw) // keep both receipts
				reason := "all preflight failures were classified environmental; rerunning once on the same head (kasb-9oupz.71)"
				if rerunnable(reason) {
					continue
				}
				// Not rerunnable (head moved or the runner was stopped): fall
				// through and record the environmental receipt as it stands.
			}
			break
		}
		// hq-y1q7ew: no receipt means the preflight recorded no product failure;
		// the host killed its process group. Record what the host was doing and
		// rerun once on the same, unchanged head. A second death stays an infra
		// ERROR, never a product failure.
		reason := fmt.Sprintf("preflight exited %d without a receipt at %s (%s)", code, m.ReceiptPath, pressure())
		if attempt > 1 {
			return finish(StatusError, reason+"; the one --recheck-environment rerun also died", &exit, "")
		}
		if !rerunnable(reason) {
			return finish(StatusError, reason, &exit, "")
		}
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

// runIdentity is what a run proves: a source bead at a head in a worktree.
type runIdentity struct {
	Source, Head, Worktree, CreatedAt string
}

// supersession returns why a running preflight can no longer produce a useful
// receipt (kasb-84hoc), or "" to keep running:
//   - the worktree HEAD moved away from the head under test: the receipt
//     would bind a head the worktree no longer holds, and Run would record an
//     error at the end anyway;
//   - a newer marker exists for the same source bead at another head: gt done
//     rebound the MR to that run, so this one proves nothing anyone will read.
//
// A git error is not evidence of a move (an index.lock race, say): keep running.
func supersession(deps RunDeps, markerPath string, id runIdentity) string {
	if head, err := deps.Git(id.Worktree, "rev-parse", "HEAD"); err == nil && head != "" && head != id.Head {
		return fmt.Sprintf("worktree head moved to %s during the preflight; the receipt cannot prove %s, so the run was stopped early", short(head), short(id.Head))
	}
	if newer := NewerMarkerForSource(markerPath, id.Source, id.Head, id.CreatedAt); newer != "" {
		return fmt.Sprintf("superseded by a newer async preflight %s for %s; the run on %s was stopped early", filepath.Base(newer), id.Source, short(id.Head))
	}
	return ""
}

// NewerMarkerForSource returns the path of a marker in markerPath's directory
// for the same source at a different head, created after createdAt.
func NewerMarkerForSource(markerPath, source, head, createdAt string) string {
	if !sourceRe.MatchString(source) {
		return ""
	}
	created, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return ""
	}
	files, _ := filepath.Glob(filepath.Join(filepath.Dir(markerPath), source+"--*.json"))
	for _, p := range files {
		if p == markerPath || strings.HasSuffix(p, ".receipt.json") {
			continue
		}
		other, err := Load(p)
		if err != nil || other.Source != source || other.Head == head {
			continue
		}
		if otherCreated, err := time.Parse(time.RFC3339Nano, other.CreatedAt); err == nil && otherCreated.After(created) {
			return p
		}
	}
	return ""
}

// StopRequestPath records that gt itself asked a runner to stop, so the
// runner does not mistake a deliberate stop for a host kill.
func StopRequestPath(markerPath string) string { return sibling(markerPath, ".stop-requested") }

// withRecheckEnvironment returns command with preflight's --recheck-environment
// reason, unless the command already carries one.
func withRecheckEnvironment(command []string) []string {
	for _, arg := range command {
		if arg == "--recheck-environment" || strings.HasPrefix(arg, "--recheck-environment=") {
			return command
		}
	}
	return append(append([]string(nil), command...), "--recheck-environment", RecheckEnvironmentReason)
}

// receiptAllEnvironmental reports whether a receipt marks every failure
// environmental (failures_all_environmental, kasb-9oupz.71). A missing field
// (an older preflight, or a command that takes no lease) is false, so the
// runner keeps its prior behaviour.
func receiptAllEnvironmental(raw []byte) bool {
	var r struct {
		FailuresAllEnvironmental bool `json:"failures_all_environmental"`
	}
	return json.Unmarshal(raw, &r) == nil && r.FailuresAllEnvironmental
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
