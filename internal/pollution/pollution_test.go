package pollution

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// fakeOpts returns a base Options where every external check is stubbed
// with deterministic values. Individual tests override fields they need.
func fakeOpts(t *testing.T) Options {
	t.Helper()
	tmp := t.TempDir()
	dogs := t.TempDir()
	return Options{
		TmpDir:           tmp,
		DogsDir:          dogs,
		ProcessAlive:     func(pid int) bool { return false },
		DirInUse:         func(dir string) bool { return false },
		TmuxSessionAlive: func(name string) bool { return false },
		KillImposters:    func(townRoot string) error { return nil },
		PruneWorktree:    func(path string) error { return os.RemoveAll(path) },
	}
}

func TestResultSummary(t *testing.T) {
	cases := []struct {
		name string
		res  Result
		want string
	}{
		{"empty", Result{}, "Test pollution cleanup: clean"},
		{"populated", Result{RogueDolt: 1, StaleDirs: 2, StalePIDs: 3, DeadWorktrees: 4},
			"Test pollution cleanup: rogue_dolt=1 stale_dirs=2 stale_pids=3 dead_worktrees=4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.res.Summary(); got != tc.want {
				t.Errorf("Summary() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResultTotal(t *testing.T) {
	r := Result{RogueDolt: 1, StaleDirs: 2, StalePIDs: 3, DeadWorktrees: 4}
	if got, want := r.Total(), 10; got != want {
		t.Errorf("Total() = %d, want %d", got, want)
	}
}

func TestCleanStaleDirs_RemovesIdleDirs(t *testing.T) {
	opts := fakeOpts(t)

	staleA := filepath.Join(opts.TmpDir, "beads-test-dolt-pid12345")
	staleB := filepath.Join(opts.TmpDir, "beads-bd-tests-abc")
	unrelated := filepath.Join(opts.TmpDir, "some-other-dir")

	for _, d := range []string{staleA, staleB, unrelated} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean returned error: %v", err)
	}

	if got, want := res.StaleDirs, 2; got != want {
		t.Errorf("StaleDirs = %d, want %d", got, want)
	}
	for _, d := range []string{staleA, staleB} {
		if _, err := os.Stat(d); !os.IsNotExist(err) {
			t.Errorf("expected %s removed, stat err = %v", d, err)
		}
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("unrelated dir should be untouched: %v", err)
	}
}

func TestCleanStaleDirs_SkipsActiveDirs(t *testing.T) {
	opts := fakeOpts(t)
	opts.DirInUse = func(dir string) bool { return true }

	stale := filepath.Join(opts.TmpDir, "beads-test-dolt-pid7")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.StaleDirs != 0 {
		t.Errorf("StaleDirs = %d, want 0 (in-use)", res.StaleDirs)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Errorf("active dir should not be removed: %v", err)
	}
	if len(res.Skipped) != 1 || res.Skipped[0].Path != stale {
		t.Errorf("expected 1 skipped item for %s, got %+v", stale, res.Skipped)
	}
}

func TestCleanStaleDirs_DryRun(t *testing.T) {
	opts := fakeOpts(t)
	opts.DryRun = true

	stale := filepath.Join(opts.TmpDir, "beads-bd-tests-xyz")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.StaleDirs != 0 {
		t.Errorf("StaleDirs = %d, want 0 in dry run", res.StaleDirs)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Errorf("dry-run must not delete %s: %v", stale, err)
	}
	if len(res.Skipped) == 0 {
		t.Errorf("dry-run should record skipped item, got none")
	}
}

func TestCleanStalePIDFiles_RemovesDeadPIDs(t *testing.T) {
	opts := fakeOpts(t)
	// All PIDs are dead by default.

	pidDead := filepath.Join(opts.TmpDir, "dolt-test-server-dead.pid")
	if err := os.WriteFile(pidDead, []byte("99999\n"), 0o644); err != nil {
		t.Fatalf("write pidDead: %v", err)
	}
	pidOther := filepath.Join(opts.TmpDir, "beads-test-dolt-also-dead.pid")
	if err := os.WriteFile(pidOther, []byte("88888"), 0o644); err != nil {
		t.Fatalf("write pidOther: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if got, want := res.StalePIDs, 2; got != want {
		t.Errorf("StalePIDs = %d, want %d", got, want)
	}
	for _, p := range []string{pidDead, pidOther} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected %s removed, stat err = %v", p, err)
		}
	}
}

func TestCleanStalePIDFiles_PreservesLivePIDs(t *testing.T) {
	opts := fakeOpts(t)
	opts.ProcessAlive = func(pid int) bool { return pid == 4242 }

	livePID := filepath.Join(opts.TmpDir, "dolt-test-server-live.pid")
	if err := os.WriteFile(livePID, []byte("4242"), 0o644); err != nil {
		t.Fatalf("write live pid: %v", err)
	}
	deadPID := filepath.Join(opts.TmpDir, "dolt-test-server-dead.pid")
	if err := os.WriteFile(deadPID, []byte("11111"), 0o644); err != nil {
		t.Fatalf("write dead pid: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.StalePIDs != 1 {
		t.Errorf("StalePIDs = %d, want 1", res.StalePIDs)
	}
	if _, err := os.Stat(livePID); err != nil {
		t.Errorf("live PID file should remain: %v", err)
	}
	if _, err := os.Stat(deadPID); !os.IsNotExist(err) {
		t.Errorf("dead PID file should be removed: %v", err)
	}

	// Verify the live entry is recorded as skipped with the alive reason.
	var found bool
	for _, item := range res.Skipped {
		if item.Path == livePID && item.Category == CategoryStalePID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected skipped entry for live PID, got %+v", res.Skipped)
	}
}

func TestCleanStalePIDFiles_IgnoresMalformedFiles(t *testing.T) {
	opts := fakeOpts(t)

	// File with non-numeric content — must be left alone, not treated as dead.
	bad := filepath.Join(opts.TmpDir, "dolt-test-server-bad.pid")
	if err := os.WriteFile(bad, []byte("not-a-pid"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	empty := filepath.Join(opts.TmpDir, "dolt-test-server-empty.pid")
	if err := os.WriteFile(empty, []byte("  \n"), 0o644); err != nil {
		t.Fatalf("write empty: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.StalePIDs != 0 {
		t.Errorf("StalePIDs = %d, want 0 for malformed files", res.StalePIDs)
	}
	for _, p := range []string{bad, empty} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("malformed PID file %s should remain: %v", p, err)
		}
	}
}

func TestCleanDeadWorktrees_PrunesWhenSessionDead(t *testing.T) {
	opts := fakeOpts(t)
	// All sessions reported dead; default PruneWorktree removes the dir.

	dog := filepath.Join(opts.DogsDir, "echo")
	repo := filepath.Join(dog, "rig-repo")
	gitDir := filepath.Join(repo, ".git")
	if err := os.MkdirAll(gitDir, 0o755); err != nil {
		t.Fatalf("mkdir git: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.DeadWorktrees != 1 {
		t.Errorf("DeadWorktrees = %d, want 1", res.DeadWorktrees)
	}
	if _, err := os.Stat(repo); !os.IsNotExist(err) {
		t.Errorf("worktree should be pruned, stat err = %v", err)
	}
}

func TestCleanDeadWorktrees_SkipsLiveSession(t *testing.T) {
	opts := fakeOpts(t)
	opts.TmuxSessionAlive = func(name string) bool {
		return name == "dog-bravo"
	}
	pruned := []string{}
	opts.PruneWorktree = func(path string) error {
		pruned = append(pruned, path)
		return os.RemoveAll(path)
	}

	for _, name := range []string{"alpha", "bravo"} {
		repo := filepath.Join(opts.DogsDir, name, "rig")
		if err := os.MkdirAll(filepath.Join(repo, ".git"), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.DeadWorktrees != 1 {
		t.Errorf("DeadWorktrees = %d, want 1", res.DeadWorktrees)
	}

	// Live session's worktree is preserved; dead session's worktree pruned.
	live := filepath.Join(opts.DogsDir, "bravo", "rig")
	dead := filepath.Join(opts.DogsDir, "alpha", "rig")
	if _, err := os.Stat(live); err != nil {
		t.Errorf("live dog worktree should remain: %v", err)
	}
	if _, err := os.Stat(dead); !os.IsNotExist(err) {
		t.Errorf("dead dog worktree should be removed, stat err = %v", err)
	}
	if len(pruned) != 1 || pruned[0] != dead {
		t.Errorf("expected only dead worktree pruned, got %v", pruned)
	}
}

func TestCleanDeadWorktrees_IgnoresNonWorktreeDirs(t *testing.T) {
	opts := fakeOpts(t)

	dog := filepath.Join(opts.DogsDir, "charlie")
	notRepo := filepath.Join(dog, "scratch")
	if err := os.MkdirAll(notRepo, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.DeadWorktrees != 0 {
		t.Errorf("DeadWorktrees = %d, want 0 (not a worktree)", res.DeadWorktrees)
	}
	if _, err := os.Stat(notRepo); err != nil {
		t.Errorf("non-worktree dir should remain: %v", err)
	}
}

func TestCleanDeadWorktrees_GitFilePointer(t *testing.T) {
	opts := fakeOpts(t)
	pruned := []string{}
	opts.PruneWorktree = func(path string) error {
		pruned = append(pruned, path)
		return os.RemoveAll(path)
	}

	// Worktrees often have a .git FILE pointing at the main repo's worktrees dir.
	dog := filepath.Join(opts.DogsDir, "delta")
	repo := filepath.Join(dog, "rig")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	gitFile := filepath.Join(repo, ".git")
	if err := os.WriteFile(gitFile, []byte("gitdir: /elsewhere/.git/worktrees/delta\n"), 0o644); err != nil {
		t.Fatalf("write gitfile: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.DeadWorktrees != 1 {
		t.Errorf("DeadWorktrees = %d, want 1", res.DeadWorktrees)
	}
	if len(pruned) != 1 || pruned[0] != repo {
		t.Errorf("expected pruned=[%s], got %v", repo, pruned)
	}
}

func TestCleanRogueDolt_Skipped(t *testing.T) {
	opts := fakeOpts(t)
	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.RogueDolt != 0 {
		t.Errorf("RogueDolt = %d, want 0 when TownRoot empty", res.RogueDolt)
	}
}

func TestCleanRogueDolt_Invokes(t *testing.T) {
	opts := fakeOpts(t)
	called := ""
	opts.TownRoot = "/fake/town"
	opts.KillImposters = func(townRoot string) error {
		called = townRoot
		return nil
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if called != "/fake/town" {
		t.Errorf("KillImposters called with %q, want %q", called, "/fake/town")
	}
	if res.RogueDolt != 1 {
		t.Errorf("RogueDolt = %d, want 1", res.RogueDolt)
	}
}

func TestCleanRogueDolt_RecordsErrors(t *testing.T) {
	opts := fakeOpts(t)
	opts.TownRoot = "/fake/town"
	opts.KillImposters = func(townRoot string) error { return fmt.Errorf("port busy") }

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.RogueDolt != 0 {
		t.Errorf("RogueDolt = %d, want 0 on error", res.RogueDolt)
	}
	if len(res.Errors) == 0 {
		t.Errorf("expected error recorded, got none")
	}
}

func TestCleanRogueDolt_DryRun(t *testing.T) {
	opts := fakeOpts(t)
	opts.TownRoot = "/fake/town"
	opts.DryRun = true
	called := false
	opts.KillImposters = func(townRoot string) error {
		called = true
		return nil
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if called {
		t.Errorf("KillImposters must not run in dry-run mode")
	}
	if res.RogueDolt != 0 {
		t.Errorf("RogueDolt = %d, want 0 in dry run", res.RogueDolt)
	}
	if len(res.Skipped) == 0 {
		t.Errorf("dry-run should record skipped item")
	}
}

func TestClean_FullScenarioCounts(t *testing.T) {
	opts := fakeOpts(t)
	opts.TownRoot = "/town"

	// Stale dir
	staleDir := filepath.Join(opts.TmpDir, "beads-test-dolt-1")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Stale PID
	pidFile := filepath.Join(opts.TmpDir, "dolt-test-server-1.pid")
	if err := os.WriteFile(pidFile, []byte("12345"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Dead worktree
	dogRepo := filepath.Join(opts.DogsDir, "echo", "rig-repo")
	if err := os.MkdirAll(filepath.Join(dogRepo, ".git"), 0o755); err != nil {
		t.Fatalf("mkdir git: %v", err)
	}

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}

	// Validate per-category counts.
	want := Result{RogueDolt: 1, StaleDirs: 1, StalePIDs: 1, DeadWorktrees: 1}
	if res.RogueDolt != want.RogueDolt ||
		res.StaleDirs != want.StaleDirs ||
		res.StalePIDs != want.StalePIDs ||
		res.DeadWorktrees != want.DeadWorktrees {
		t.Errorf("counts = %+v, want %+v", *res, want)
	}
	if got, want := res.Total(), 4; got != want {
		t.Errorf("Total() = %d, want %d", got, want)
	}
	if got := res.Summary(); got != "Test pollution cleanup: rogue_dolt=1 stale_dirs=1 stale_pids=1 dead_worktrees=1" {
		t.Errorf("Summary() = %q", got)
	}

	// Confirm cleaned items cover all four categories.
	categories := map[string]bool{}
	for _, item := range res.Cleaned {
		categories[item.Category] = true
	}
	for _, want := range []string{CategoryRogueDolt, CategoryStaleDir, CategoryStalePID, CategoryDeadWorktree} {
		if !categories[want] {
			t.Errorf("Cleaned missing category %q (got categories: %v)", want, sortedKeys(categories))
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestReadPIDFile(t *testing.T) {
	tmp := t.TempDir()
	good := filepath.Join(tmp, "good.pid")
	if err := os.WriteFile(good, []byte(" 4242 \n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if pid, ok := readPIDFile(good); !ok || pid != 4242 {
		t.Errorf("readPIDFile(good) = (%d,%v), want (4242,true)", pid, ok)
	}
	if _, ok := readPIDFile(filepath.Join(tmp, "missing.pid")); ok {
		t.Errorf("readPIDFile on missing file should return false")
	}
	bad := filepath.Join(tmp, "bad.pid")
	if err := os.WriteFile(bad, []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readPIDFile(bad); ok {
		t.Errorf("readPIDFile on non-numeric should return false")
	}
	zero := filepath.Join(tmp, "zero.pid")
	if err := os.WriteFile(zero, []byte("0"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readPIDFile(zero); ok {
		t.Errorf("readPIDFile must reject pid 0")
	}
}

func TestIsWorktree(t *testing.T) {
	tmp := t.TempDir()

	// Directory with .git dir.
	repoDir := filepath.Join(tmp, "real")
	if err := os.MkdirAll(filepath.Join(repoDir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if !isWorktree(repoDir) {
		t.Errorf("isWorktree(real) = false, want true")
	}

	// Worktree with .git pointer file.
	wtDir := filepath.Join(tmp, "worktree")
	if err := os.MkdirAll(wtDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wtDir, ".git"), []byte("gitdir: /elsewhere/.git\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !isWorktree(wtDir) {
		t.Errorf("isWorktree(worktree) = false, want true")
	}

	// Plain directory — not a worktree.
	plain := filepath.Join(tmp, "plain")
	if err := os.MkdirAll(plain, 0o755); err != nil {
		t.Fatal(err)
	}
	if isWorktree(plain) {
		t.Errorf("isWorktree(plain) = true, want false")
	}

	// Regular file at .git but not a gitdir pointer.
	bogus := filepath.Join(tmp, "bogus")
	if err := os.MkdirAll(bogus, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bogus, ".git"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if isWorktree(bogus) {
		t.Errorf("isWorktree(bogus) = true, want false")
	}
}

func TestCleanStaleDirs_GlobError(t *testing.T) {
	// A glob pattern that filepath.Glob never errors on, but verifying that
	// missing TmpDir is treated as benign (nothing to clean, no errors).
	opts := fakeOpts(t)
	opts.TmpDir = filepath.Join(t.TempDir(), "does-not-exist")

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.StaleDirs != 0 {
		t.Errorf("StaleDirs = %d, want 0", res.StaleDirs)
	}
	if len(res.Errors) != 0 {
		t.Errorf("expected no errors, got %v", res.Errors)
	}
}

func TestCleanDeadWorktrees_MissingDogsDir(t *testing.T) {
	opts := fakeOpts(t)
	opts.DogsDir = filepath.Join(t.TempDir(), "absent")

	res, err := Clean(opts)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if res.DeadWorktrees != 0 {
		t.Errorf("DeadWorktrees = %d, want 0", res.DeadWorktrees)
	}
	if len(res.Errors) != 0 {
		t.Errorf("expected no errors for missing dogs dir, got %v", res.Errors)
	}
}
