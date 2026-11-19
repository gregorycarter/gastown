//go:build darwin

package asyncpreflight

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// processStartTime returns an identity for pid's start (kernel start time), or
// "" when the process does not exist. A reused PID has a different value.
func processStartTime(pid int) string {
	if pid <= 0 {
		return ""
	}
	k, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || k == nil || int(k.Proc.P_pid) != pid {
		return ""
	}
	t := k.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", t.Sec, t.Usec)
}
