//go:build !windows

package pollution

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

// isProcessAlive returns true if a process with pid is still running.
// This is the Unix equivalent of `kill -0 <pid>`: signal 0 performs the
// permission/existence check without delivering a signal.
func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return process.Signal(syscall.Signal(0)) == nil
}

// directoryInUse reports whether any process holds an open file under dir.
// Uses `lsof +D` which descends into the tree. A non-zero exit from lsof
// (no matching file handles) is treated as "not in use", matching the bash
// snippet in the patrol formula.
func directoryInUse(dir string) bool {
	// lsof +D scans recursively; -t prints only PIDs (so we can detect "no
	// matches" by an empty stdout instead of parsing). We deliberately do
	// not return true on lsof errors — a missing/broken lsof must not block
	// cleanup in environments where the binary is unavailable.
	cmd := exec.Command("lsof", "+D", dir)
	out, err := cmd.Output()
	if err != nil {
		// lsof exits 1 when no matches; treat any error as "not in use" so
		// a missing tool does not pin pollution forever.
		return false
	}
	return len(out) > 0
}

// isTmuxSessionAlive reports whether tmux has a session named name. A
// non-zero exit from `tmux has-session -t <name>` means the session is
// absent; any other error is treated as "session absent" so that a tmux
// outage does not stop us from pruning unambiguously dead worktrees.
func isTmuxSessionAlive(name string) bool {
	cmd := exec.Command("tmux", "has-session", "-t", name)
	return cmd.Run() == nil
}

// chmodWritableTree restores u+w on every file in dir so RemoveAll can
// proceed. Tests sometimes mark scratch dirs read-only on cleanup; this
// mirrors the `chmod -R u+w` step in the patrol formula's bash snippet.
func chmodWritableTree(dir string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			// Permission errors are expected on read-only entries — keep walking.
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		_ = os.Chmod(path, info.Mode()|0o200)
		return nil
	})
}
