// Package pollution detects and cleans runtime test pollution left by tests
// and dead processes.
//
// This implements the "test-pollution-cleanup" step of the Deacon's patrol
// (mol-deacon-patrol). It cleans four categories of pollution, but only
// where the owning process is confirmed dead — never killing or removing
// resources owned by live processes.
//
// Categories:
//
//  1. Rogue dolt servers — sql-server processes holding this workspace's
//     port but using a different data directory. Delegated to doltserver.KillImposters.
//
//  2. Stale test temp dirs — beads-test-dolt-* and beads-bd-tests-* in TMPDIR
//     where no process holds open file handles.
//
//  3. Stale PID/lock files — /tmp/dolt-test-server-*.pid and
//     /tmp/beads-test-dolt-*.pid where the recorded PID is dead.
//
//  4. Dead dog worktrees — git worktrees under ~/gt/deacon/dogs/<name>/
//     when the dog's tmux session no longer exists.
//
// All categories are best-effort and tolerate per-item errors so a single
// failure does not abort the patrol step. Per-category counts and per-item
// details are returned for the patrol digest.
package pollution

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/steveyegge/gastown/internal/doltserver"
)

// Category enumerates the kinds of pollution this package cleans.
const (
	CategoryRogueDolt     = "rogue_dolt"
	CategoryStaleDir      = "stale_dir"
	CategoryStalePID      = "stale_pid"
	CategoryDeadWorktree  = "dead_worktree"
)

// Item describes a single piece of pollution that was cleaned (or skipped).
type Item struct {
	Category string `json:"category"`
	Path     string `json:"path"`
	Reason   string `json:"reason"`
}

// Result summarises a Clean run.
type Result struct {
	RogueDolt     int      `json:"rogue_dolt"`
	StaleDirs     int      `json:"stale_dirs"`
	StalePIDs     int      `json:"stale_pids"`
	DeadWorktrees int      `json:"dead_worktrees"`
	Cleaned       []Item   `json:"cleaned,omitempty"`
	Skipped       []Item   `json:"skipped,omitempty"`
	Errors        []string `json:"errors,omitempty"`
}

// Total returns the sum of all cleaned counters.
func (r *Result) Total() int {
	return r.RogueDolt + r.StaleDirs + r.StalePIDs + r.DeadWorktrees
}

// Summary returns the patrol-digest line for this run.
func (r *Result) Summary() string {
	if r.Total() == 0 {
		return "Test pollution cleanup: clean"
	}
	return fmt.Sprintf("Test pollution cleanup: rogue_dolt=%d stale_dirs=%d stale_pids=%d dead_worktrees=%d",
		r.RogueDolt, r.StaleDirs, r.StalePIDs, r.DeadWorktrees)
}

// Options configures Clean. Zero-value fields fall back to defaults that
// inspect the live system.
type Options struct {
	// TownRoot is the Gas Town workspace root used to identify rogue dolt
	// imposters. When empty, the rogue-dolt category is skipped.
	TownRoot string

	// TmpDir is the directory scanned for stale test dirs and PID files.
	// When empty, os.TempDir() is used.
	TmpDir string

	// DogsDir is the kennel directory whose dead-dog worktrees are pruned.
	// When empty, ~/gt/deacon/dogs is used.
	DogsDir string

	// DryRun skips destructive actions (kill, rm, worktree remove) but still
	// reports what would have been cleaned in Result.Skipped.
	DryRun bool

	// Test injection points. Production callers leave these nil to use
	// the real implementations.

	// ProcessAlive returns true if pid is still running.
	ProcessAlive func(pid int) bool

	// DirInUse returns true if any process holds an open file under dir.
	DirInUse func(dir string) bool

	// TmuxSessionAlive returns true if a tmux session named name exists.
	TmuxSessionAlive func(name string) bool

	// KillImposters terminates dolt sql-server imposters bound to townRoot's
	// port that serve from a different data directory. When nil, the real
	// doltserver.KillImposters is used.
	KillImposters func(townRoot string) error

	// PruneWorktree removes a git worktree at path. When nil,
	// `git worktree remove --force <path>` is invoked from the worktree's
	// main repository.
	PruneWorktree func(path string) error
}

// Clean detects and cleans runtime test pollution across all four categories.
// It always returns a non-nil Result; per-category errors are recorded in
// Result.Errors rather than aborting the run.
func Clean(opts Options) (*Result, error) {
	res := &Result{}

	cleanRogueDolt(opts, res)
	cleanStaleDirs(opts, res)
	cleanStalePIDFiles(opts, res)
	cleanDeadWorktrees(opts, res)

	return res, nil
}

// cleanRogueDolt invokes the imposter killer when a town root is configured.
func cleanRogueDolt(opts Options, res *Result) {
	if opts.TownRoot == "" {
		return
	}
	if opts.DryRun {
		// Caller can still observe an item entry, but do not invoke the killer.
		res.Skipped = append(res.Skipped, Item{
			Category: CategoryRogueDolt,
			Path:     opts.TownRoot,
			Reason:   "dry-run",
		})
		return
	}

	killer := opts.KillImposters
	if killer == nil {
		killer = doltserver.KillImposters
	}
	if err := killer(opts.TownRoot); err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("rogue_dolt: %v", err))
		return
	}

	// KillImposters has no return value indicating how many were killed; it
	// returns nil whether or not it found and terminated an imposter. Treat a
	// nil return as "best-effort completed" and credit one cleanup unit only
	// when the caller expects detection (TownRoot set).
	res.RogueDolt++
	res.Cleaned = append(res.Cleaned, Item{
		Category: CategoryRogueDolt,
		Path:     opts.TownRoot,
		Reason:   "imposter sweep",
	})
}

// staleDirGlobs are the test-pollution directory name patterns scanned in TMPDIR.
var staleDirGlobs = []string{"beads-test-dolt-*", "beads-bd-tests-*"}

// cleanStaleDirs removes test temp dirs that no live process holds open.
func cleanStaleDirs(opts Options, res *Result) {
	tmp := opts.TmpDir
	if tmp == "" {
		tmp = os.TempDir()
	}

	inUse := opts.DirInUse
	if inUse == nil {
		inUse = directoryInUse
	}

	for _, pattern := range staleDirGlobs {
		matches, err := filepath.Glob(filepath.Join(tmp, pattern))
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("stale_dir glob %q: %v", pattern, err))
			continue
		}
		for _, dir := range matches {
			info, err := os.Stat(dir)
			if err != nil || !info.IsDir() {
				continue
			}
			if inUse(dir) {
				res.Skipped = append(res.Skipped, Item{
					Category: CategoryStaleDir,
					Path:     dir,
					Reason:   "open file handles",
				})
				continue
			}
			if opts.DryRun {
				res.Skipped = append(res.Skipped, Item{
					Category: CategoryStaleDir,
					Path:     dir,
					Reason:   "dry-run",
				})
				continue
			}
			// Restore write perms before removing — tests sometimes mark dirs
			// read-only on cleanup. Best-effort: ignore chmod errors.
			_ = chmodWritableTree(dir)
			if err := os.RemoveAll(dir); err != nil {
				res.Errors = append(res.Errors,
					fmt.Sprintf("stale_dir remove %q: %v", dir, err))
				continue
			}
			res.StaleDirs++
			res.Cleaned = append(res.Cleaned, Item{
				Category: CategoryStaleDir,
				Path:     dir,
				Reason:   "no live file handles",
			})
		}
	}
}

// stalePIDGlobs are the PID/lock files scanned. Each path is glob-expanded
// against /tmp directly because the test-server PID files are written there
// regardless of TMPDIR overrides.
var stalePIDGlobs = []string{
	"/tmp/dolt-test-server-*.pid",
	"/tmp/beads-test-dolt-*.pid",
}

// cleanStalePIDFiles removes PID files whose recorded PID is dead.
func cleanStalePIDFiles(opts Options, res *Result) {
	alive := opts.ProcessAlive
	if alive == nil {
		alive = isProcessAlive
	}

	// When TmpDir is set (tests), search there in addition to /tmp so callers
	// can exercise the logic without writing into the real /tmp.
	patterns := append([]string(nil), stalePIDGlobs...)
	if opts.TmpDir != "" {
		patterns = append(patterns,
			filepath.Join(opts.TmpDir, "dolt-test-server-*.pid"),
			filepath.Join(opts.TmpDir, "beads-test-dolt-*.pid"),
		)
	}

	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("stale_pid glob %q: %v", pattern, err))
			continue
		}
		for _, pidFile := range matches {
			pid, ok := readPIDFile(pidFile)
			if !ok {
				// Empty or unreadable — leave it; we cannot prove the owner is dead.
				continue
			}
			if alive(pid) {
				res.Skipped = append(res.Skipped, Item{
					Category: CategoryStalePID,
					Path:     pidFile,
					Reason:   fmt.Sprintf("PID %d alive", pid),
				})
				continue
			}
			if opts.DryRun {
				res.Skipped = append(res.Skipped, Item{
					Category: CategoryStalePID,
					Path:     pidFile,
					Reason:   "dry-run",
				})
				continue
			}
			if err := os.Remove(pidFile); err != nil {
				res.Errors = append(res.Errors,
					fmt.Sprintf("stale_pid remove %q: %v", pidFile, err))
				continue
			}
			res.StalePIDs++
			res.Cleaned = append(res.Cleaned, Item{
				Category: CategoryStalePID,
				Path:     pidFile,
				Reason:   fmt.Sprintf("PID %d dead", pid),
			})
		}
	}
}

// cleanDeadWorktrees prunes git worktrees under ~/gt/deacon/dogs/<name>/
// where the dog's tmux session is no longer running.
func cleanDeadWorktrees(opts Options, res *Result) {
	dogsDir := opts.DogsDir
	if dogsDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("dead_worktree home: %v", err))
			return
		}
		dogsDir = filepath.Join(home, "gt", "deacon", "dogs")
	}

	entries, err := os.ReadDir(dogsDir)
	if err != nil {
		// Missing kennel directory is fine — no dogs, nothing to prune.
		if os.IsNotExist(err) {
			return
		}
		res.Errors = append(res.Errors, fmt.Sprintf("dead_worktree readdir: %v", err))
		return
	}

	tmuxAlive := opts.TmuxSessionAlive
	if tmuxAlive == nil {
		tmuxAlive = isTmuxSessionAlive
	}

	prune := opts.PruneWorktree
	if prune == nil {
		prune = pruneWorktreeViaGit
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dogName := entry.Name()
		dogDir := filepath.Join(dogsDir, dogName)
		sessionName := "dog-" + dogName
		if tmuxAlive(sessionName) {
			continue
		}

		// Session is dead. Walk the dog's children and prune each git worktree.
		children, err := os.ReadDir(dogDir)
		if err != nil {
			res.Errors = append(res.Errors,
				fmt.Sprintf("dead_worktree readdir %q: %v", dogDir, err))
			continue
		}
		for _, child := range children {
			if !child.IsDir() {
				continue
			}
			rigRepo := filepath.Join(dogDir, child.Name())
			if !isWorktree(rigRepo) {
				continue
			}
			if opts.DryRun {
				res.Skipped = append(res.Skipped, Item{
					Category: CategoryDeadWorktree,
					Path:     rigRepo,
					Reason:   "dry-run",
				})
				continue
			}
			if err := prune(rigRepo); err != nil {
				res.Errors = append(res.Errors,
					fmt.Sprintf("dead_worktree prune %q: %v", rigRepo, err))
				continue
			}
			res.DeadWorktrees++
			res.Cleaned = append(res.Cleaned, Item{
				Category: CategoryDeadWorktree,
				Path:     rigRepo,
				Reason:   fmt.Sprintf("tmux session %q dead", sessionName),
			})
		}
	}
}

// readPIDFile returns the PID stored in pidFile, or false if the file is
// missing, empty, or malformed.
func readPIDFile(pidFile string) (int, bool) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, false
	}
	text := strings.TrimSpace(string(data))
	if text == "" {
		return 0, false
	}
	pid, err := strconv.Atoi(text)
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// isWorktree reports whether path is a git worktree (has a .git entry that
// is either a directory or a "gitdir:" pointer file).
func isWorktree(path string) bool {
	gitPath := filepath.Join(path, ".git")
	info, err := os.Lstat(gitPath)
	if err != nil {
		return false
	}
	if info.IsDir() {
		return true
	}
	// Worktrees use a .git file that points to the main repo's worktrees dir.
	if info.Mode().IsRegular() {
		data, err := os.ReadFile(gitPath)
		if err != nil {
			return false
		}
		return strings.HasPrefix(strings.TrimSpace(string(data)), "gitdir:")
	}
	return false
}

// pruneWorktreeViaGit invokes `git worktree remove --force <path>` from the
// main repository associated with the worktree.
func pruneWorktreeViaGit(path string) error {
	// Locate the main repo via git rev-parse --git-common-dir, which works
	// inside both the main repo and any of its worktrees.
	commonDir, err := runGit(path, "rev-parse", "--git-common-dir")
	if err != nil {
		return fmt.Errorf("locate main repo: %w", err)
	}
	commonDir = strings.TrimSpace(commonDir)
	if commonDir == "" {
		return fmt.Errorf("git rev-parse returned empty common dir for %q", path)
	}
	// commonDir may be a relative path; normalise against the worktree.
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(path, commonDir)
	}
	mainRepo := commonDir
	if filepath.Base(mainRepo) == ".git" {
		mainRepo = filepath.Dir(mainRepo)
	}

	if _, err := runGit(mainRepo, "worktree", "remove", "--force", path); err != nil {
		// As a last resort, ask git to prune metadata even if remove failed.
		_, _ = runGit(mainRepo, "worktree", "prune")
		return fmt.Errorf("git worktree remove: %w", err)
	}
	return nil
}

// runGit executes git in cwd with the given args and returns its stdout.
func runGit(cwd string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = cwd
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}
