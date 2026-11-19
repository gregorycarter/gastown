//go:build !darwin

package asyncpreflight

import (
	"os"
	"strconv"
	"strings"
)

// processStartTime returns an identity for pid's start (field 22 of
// /proc/<pid>/stat on Linux), or "" when unavailable.
func processStartTime(pid int) string {
	if pid <= 0 {
		return ""
	}
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(raw)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return ""
	}
	fields := strings.Fields(s[i+1:])
	if len(fields) < 20 {
		return ""
	}
	return fields[19]
}
