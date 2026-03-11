//go:build windows

package nudge

import (
	"os"
	"syscall"
)

// detachedProcAttr returns SysProcAttr for Windows.
// On Windows, CREATE_NEW_PROCESS_GROUP detaches the child.
func detachedProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		CreationFlags: 0x00000200, // CREATE_NEW_PROCESS_GROUP
	}
}

// isProcessAlive checks if a process is running.
// On Windows, FindProcess always succeeds, so we attempt signal 0.
func isProcessAlive(proc *os.Process) bool {
	return proc.Signal(syscall.Signal(0)) == nil
}

// terminateProcess kills the process on Windows.
func terminateProcess(proc *os.Process) error {
	return proc.Kill()
}
