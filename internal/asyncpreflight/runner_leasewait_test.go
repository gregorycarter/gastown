package asyncpreflight

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeSentinel writes the lease sentinel the preflight would write (kasb-9oupz.70).
func writeSentinel(t *testing.T, markerPath string, s LeaseSentinel) {
	t.Helper()
	raw, _ := json.Marshal(s)
	if err := os.WriteFile(LeaseSentinelPath(markerPath), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func writePassReceipt(t *testing.T, dir, markerPath string) {
	t.Helper()
	m, _ := Load(markerPath)
	p := filepath.Join(dir, m.ReceiptPath)
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	raw, _ := json.Marshal(map[string]any{"head_sha": headA, "source_bead_id": src, "status": "passed"})
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestLeaseWaitDoesNotCountAgainstRunTimeout is the kasb-9oupz.70 regression:
// a preflight that waits past its run timeout for a heavy-work lease, then
// acquires one and finishes, must pass — the queue wait no longer eats the run
// budget. Before the fix the run was killed as failed_timeout while queued.
func TestLeaseWaitDoesNotCountAgainstRunTimeout(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	deps.Timeout = 60 * time.Millisecond
	deps.QueueCap = 10 * time.Second
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		writeSentinel(t, r.path, LeaseSentinel{Phase: LeasePhaseWaiting})
		select {
		case <-ctx.Done(): // killed while still queued: the bug is back
			return -1, nil
		case <-time.After(120 * time.Millisecond): // past StartedAt+timeout
		}
		writeSentinel(t, r.path, LeaseSentinel{Phase: LeasePhaseAcquired,
			At: time.Now().UTC().Format(time.RFC3339Nano), QueueSeconds: 0.12})
		time.Sleep(30 * time.Millisecond) // let the heartbeat record the acquisition
		writePassReceipt(t, dir, r.path)
		return 0, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusPassed {
		t.Fatalf("waiting for a lease must not count against the run timeout: got %s / %q", m.Status, m.Reason)
	}
	if m.LeaseAcquiredAt == "" {
		t.Fatalf("lease_acquired_at should be recorded")
	}
	if m.LeaseWaiting {
		t.Fatalf("lease_waiting should be cleared once the lease is acquired")
	}
}

// TestRunTimeoutFiresFromAcquisition confirms the hard timeout still fires, now
// measured from lease acquisition, and is classified as failed_timeout.
func TestRunTimeoutFiresFromAcquisition(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	deps.Timeout = 40 * time.Millisecond
	deps.QueueCap = 10 * time.Second
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		writeSentinel(t, r.path, LeaseSentinel{Phase: LeasePhaseAcquired,
			At: time.Now().UTC().Format(time.RFC3339Nano)})
		<-ctx.Done() // a hung run after the lease
		return -1, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusFailed || m.ReceiptStatus != ReceiptTimedOut || !strings.Contains(m.Reason, "timeout") {
		t.Fatalf("expected failed_timeout from acquisition, got %s / %s / %q", m.Status, m.ReceiptStatus, m.Reason)
	}
	if m.LeaseAcquiredAt == "" {
		t.Fatalf("lease_acquired_at should be recorded")
	}
}

// TestQueueCapRecordsLeaseStarvation: a preflight that never acquires a lease is
// stopped at the queue cap and recorded as lease starvation, never
// failed_timeout, so the lander does not treat it as a code failure's timeout.
func TestQueueCapRecordsLeaseStarvation(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	deps.Timeout = 10 * time.Second
	deps.QueueCap = 40 * time.Millisecond
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		writeSentinel(t, r.path, LeaseSentinel{Phase: LeasePhaseWaiting})
		<-ctx.Done() // never acquires a lease
		return -1, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusError || m.ReceiptStatus != ReceiptLeaseWaitExhausted {
		t.Fatalf("expected lease_wait_exhausted error, got %s / %s / %q", m.Status, m.ReceiptStatus, m.Reason)
	}
	if m.ReceiptStatus == ReceiptTimedOut {
		t.Fatalf("lease starvation must never be failed_timeout")
	}
	if !strings.Contains(m.Reason, "starvation") {
		t.Fatalf("reason should name lease starvation: %q", m.Reason)
	}
}

// TestLegacyNoSentinelKeepsSubmissionTimeout: with no lease sentinel (an older
// preflight), the timeout is measured from submission and classified exactly as
// before — failed_timeout at the run timeout.
func TestLegacyNoSentinelKeepsSubmissionTimeout(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	deps.Timeout = 40 * time.Millisecond
	deps.QueueCap = 10 * time.Second
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		<-ctx.Done() // never writes a sentinel
		return -1, nil
	}
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	m, _ := Load(r.path)
	if m.Status != StatusFailed || m.ReceiptStatus != ReceiptTimedOut {
		t.Fatalf("no-sentinel preflight should keep today's failed_timeout, got %s / %s", m.Status, m.ReceiptStatus)
	}
	if m.LeaseAcquiredAt != "" || m.LeaseWaiting {
		t.Fatalf("no lease fields should be set without a sentinel")
	}
}

// TestBackstopKillsWhenHeartbeatStalls: with the heartbeat effectively stalled
// (its interval longer than the whole run), the outer deadline still stops the
// preflight and records a failed_timeout, so a stuck heartbeat can never leave a
// run going forever (kasb-9oupz.70).
func TestBackstopKillsWhenHeartbeatStalls(t *testing.T) {
	r := newRunFixture(t)
	r.heads = []string{headA}
	deps := r.deps()
	deps.Heartbeat = 10 * time.Second // never ticks within the test: a stalled heartbeat
	deps.Timeout = 30 * time.Millisecond
	deps.QueueCap = 30 * time.Millisecond
	deps.BackstopMargin = 40 * time.Millisecond // outer deadline = 100ms
	deps.Exec = func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		<-ctx.Done() // a hung preflight the heartbeat never catches
		return -1, nil
	}
	start := time.Now()
	if err := Run(context.Background(), r.path, deps); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("backstop did not fire; ran %s", time.Since(start))
	}
	m, _ := Load(r.path)
	if m.Status != StatusFailed || m.ReceiptStatus != ReceiptTimedOut || !strings.Contains(m.Reason, "backstop") {
		t.Fatalf("expected backstop failed_timeout, got %s / %s / %q", m.Status, m.ReceiptStatus, m.Reason)
	}
}

// TestLiveUsesLeaseClock covers the gate side: a running marker is live for the
// run timeout measured from lease acquisition, or for the queue cap while still
// waiting — not just StartedAt+Timeout (kasb-9oupz.70).
func TestLiveUsesLeaseClock(t *testing.T) {
	now := time.Now()
	ts := func(d time.Duration) string { return now.Add(d).UTC().Format(time.RFC3339Nano) }
	base := func() *Marker {
		return &Marker{
			Status:       StatusRunning,
			PID:          os.Getpid(),
			PIDStart:     ProcessStartTime(os.Getpid()),
			HeartbeatAt:  ts(0),
			StartedAt:    ts(-5 * time.Hour),
			TimeoutSecs:  int((2 * time.Hour) / time.Second),
			QueueCapSecs: int((6 * time.Hour) / time.Second),
		}
	}
	cases := []struct {
		name string
		mut  func(*Marker)
		want bool
	}{
		{"legacy past timeout", func(m *Marker) {}, false},
		{"acquired recently", func(m *Marker) { m.LeaseAcquiredAt = ts(-1 * time.Hour) }, true},
		{"acquired past timeout", func(m *Marker) { m.LeaseAcquiredAt = ts(-3 * time.Hour) }, false},
		{"waiting within cap", func(m *Marker) { m.LeaseWaiting = true }, true},
		{"waiting past cap", func(m *Marker) { m.LeaseWaiting = true; m.StartedAt = ts(-7 * time.Hour) }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := base()
			c.mut(m)
			if got := Live(m, now); got != c.want {
				t.Fatalf("Live=%v, want %v", got, c.want)
			}
		})
	}
}
