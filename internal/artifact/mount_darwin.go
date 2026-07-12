//go:build darwin

package artifact

import (
	"path/filepath"
	"sync"
	"syscall"
)

var darwinMounts struct {
	sync.Once
	paths map[string]bool
	err   error
}

const mntNowait = 2 // getfsstat(2) MNT_NOWAIT; not exported by syscall on Darwin.

func isMountPoint(candidate string) (bool, error) {
	darwinMounts.Do(func() {
		count, err := syscall.Getfsstat(nil, mntNowait)
		if err != nil {
			darwinMounts.err = err
			return
		}
		stats := make([]syscall.Statfs_t, count)
		count, err = syscall.Getfsstat(stats, mntNowait)
		if err != nil {
			darwinMounts.err = err
			return
		}
		darwinMounts.paths = make(map[string]bool, count)
		for _, stat := range stats[:count] {
			darwinMounts.paths[filepath.Clean(int8CString(stat.Mntonname[:]))] = true
		}
	})
	return darwinMounts.paths[filepath.Clean(candidate)], darwinMounts.err
}

func int8CString(value []int8) string {
	bytes := make([]byte, 0, len(value))
	for _, char := range value {
		if char == 0 {
			break
		}
		bytes = append(bytes, byte(char))
	}
	return string(bytes)
}
