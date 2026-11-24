package asyncpreflight

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestActiveForWorktreeFailsClosed(t *testing.T) {
	fx := newGateFixture(t)
	withProcesses(t, map[int]string{4242: "start-1"})
	fx.set(t, running(fx.now))

	// The marker's worktree (/w) is below the polecat directory being reclaimed.
	if r, err := ActiveForWorktree(fx.rig, "/w", fx.now.Add(time.Minute)); err != nil || !strings.Contains(r, "async_preflight=running") {
		t.Fatalf("live run not reported: %q %v", r, err)
	}
	if r, err := ActiveForWorktree(fx.rig, "/", fx.now.Add(time.Minute)); err != nil || r == "" {
		t.Fatalf("run below the sandbox root not reported: %q %v", r, err)
	}
	if r, err := ActiveForWorktree(fx.rig, "/other", fx.now); err != nil || r != "" {
		t.Fatalf("other worktree reported: %q %v", r, err)
	}
	// Stale heartbeat, but the runner process still exists: it still owns
	// the worktree (RunningForWorktree would release it).
	late := fx.now.Add(LostAfter + time.Minute)
	if RunningForWorktree(fx.rig, "/w", late) != "" {
		t.Fatal("precondition: RunningForWorktree releases a stale heartbeat")
	}
	if r, err := ActiveForWorktree(fx.rig, "/w", late); err != nil || !strings.Contains(r, "runner_alive") {
		t.Fatalf("live runner with stale heartbeat not reported: %q %v", r, err)
	}
	withDeadPIDs(t)
	if r, err := ActiveForWorktree(fx.rig, "/w", late); err != nil || r != "" {
		t.Fatalf("dead run still holds the worktree: %q %v", r, err)
	}
}

func TestActiveForWorktreeUnreadableMarker(t *testing.T) {
	rig := t.TempDir()
	bad := filepath.Join(Dir(rig), "kasb-x--"+headA+".json")
	if err := os.MkdirAll(Dir(rig), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ActiveForWorktree(rig, "/w", time.Now()); err == nil {
		t.Fatal("a young unreadable marker must fail closed")
	}
	old := time.Now().Add(-(DefaultTimeout + StartGrace + time.Hour))
	if err := os.Chtimes(bad, old, old); err != nil {
		t.Fatal(err)
	}
	if r, err := ActiveForWorktree(rig, "/w", time.Now()); err != nil || r != "" {
		t.Fatalf("an old corrupt marker cannot be a live run: %q %v", r, err)
	}
}

// blockingExec is a preflight that runs until its context is cancelled.
func blockingExec(started chan<- struct{}) func(context.Context, string, []string, io.Writer) (int, error) {
	return func(ctx context.Context, dir string, argv []string, log io.Writer) (int, error) {
		close(started)
		<-ctx.Done()
		return -1, nil
	}
}

func TestRunStopsPromptlyWhenWorktreeHeadMoves(t *testing.T) {
	r := newRunFixture(t)
	var mu sync.Mutex
	moved := false
	deps := r.deps()
	deps.Git = func(dir string, args ...string) (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if args[0] == "status" {
			return "", nil
		}
		if moved {
			return headB, nil
		}
		return headA, nil
	}
	started := make(chan struct{})
	deps.Exec = blockingExec(started)
	deps.Timeout = time.Hour
	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), r.path, deps) }()
	<-started
	mu.Lock()
	moved = true // the polecat committed again mid-run
	mu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("preflight kept running after the worktree head moved")
	}
	m, _ := Load(r.path)
	if m.Status != StatusError || !strings.Contains(m.Reason, "head moved") || !strings.Contains(m.Reason, "stopped early") {
		t.Fatalf("marker %+v", m)
	}
	if m.RecheckedEnvironment != "" {
		t.Fatalf("a superseded run must not rerun: %q", m.RecheckedEnvironment)
	}
}

func TestRunStopsPromptlyWhenANewerMarkerExists(t *testing.T) {
	r := newRunFixture(t)
	deps := r.deps()
	started := make(chan struct{})
	deps.Exec = blockingExec(started)
	deps.Timeout = time.Hour
	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), r.path, deps) }()
	<-started
	// gt done ran again for the same bead at a new head (other worktree).
	rig := filepath.Dir(filepath.Dir(filepath.Dir(r.path)))
	req := request(rig, t.TempDir(), time.Now().Add(time.Second))
	req.Head = headB
	if _, _, _, err := Prepare(req); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("preflight kept running after a newer marker superseded it")
	}
	m, _ := Load(r.path)
	if m.Status != StatusError || !strings.Contains(m.Reason, "superseded by a newer async preflight") {
		t.Fatalf("marker %+v", m)
	}
}

func TestNewerMarkerForSourceIgnoresOlderAndSameHead(t *testing.T) {
	rig := t.TempDir()
	now := time.Now()
	_, pathA, _, err := Prepare(request(rig, "/w", now))
	if err != nil {
		t.Fatal(err)
	}
	mA, _ := Load(pathA)
	if got := NewerMarkerForSource(pathA, src, headA, mA.CreatedAt); got != "" {
		t.Fatalf("no other marker, got %q", got)
	}
	older := request(rig, "/w", now.Add(-time.Hour))
	older.Head = headB
	if _, _, _, err := Prepare(older); err != nil {
		t.Fatal(err)
	}
	if got := NewerMarkerForSource(pathA, src, headA, mA.CreatedAt); got != "" {
		t.Fatalf("an older marker is not newer: %q", got)
	}
}
