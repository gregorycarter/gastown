//go:build windows

package pollution

import (
	"os"
	"syscall"
)

// isProcessAlive checks whether a process with the given PID is still running.
// Mirrors internal/doltserver's Windows implementation.
func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	const PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
	h, err := syscall.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	syscall.CloseHandle(h)
	return true
}

// directoryInUse on Windows is a conservative no-op: we cannot easily detect
// open handles without the Restart Manager API, so callers who need this
// check on Windows must inject their own DirInUse implementation. Returning
// false here mirrors the behaviour the patrol bash snippet falls back to
// when lsof is unavailable.
func directoryInUse(dir string) bool {
	return false
}

// isTmuxSessionAlive on Windows always returns false because tmux is not
// supported as a primary runtime. Production deployments that need this
// behaviour run on Unix.
func isTmuxSessionAlive(name string) bool {
	return false
}

// chmodWritableTree is a best-effort chmod for Windows.
func chmodWritableTree(dir string) error {
	return os.Chmod(dir, 0o700)
}
